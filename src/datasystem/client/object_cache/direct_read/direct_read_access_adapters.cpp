/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Client adapters for ObjectReadAccessFlow.
 */
#include "datasystem/client/object_cache/direct_read/direct_read_access_adapters.h"

#include "datasystem/client/object_cache/client_worker_api/iclient_worker_api.h"
#include "datasystem/client/object_cache/direct_read/direct_read_flow.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace object_cache {
DirectReadRouteProviderAdapter::DirectReadRouteProviderAdapter(DirectReadRouteProvider *provider) : provider_(provider)
{
}

Status DirectReadRouteProviderAdapter::GetMetaAddress(const std::string &objectKey, HostPort &metaAddress)
{
    RETURN_RUNTIME_ERROR_IF_NULL(provider_);
    return provider_->GetMetaAddress(objectKey, metaAddress);
}

Status DirectReadRouteProviderAdapter::RefreshRouteIfNeeded()
{
    RETURN_RUNTIME_ERROR_IF_NULL(provider_);
    return provider_->RefreshRouteIfNeeded();
}

DirectReadMetaClientAdapter::DirectReadMetaClientAdapter(DirectReadRpcAdapter *rpcAdapter,
                                                         HostPort clientWorkerAddress,
                                                         DirectReadRouteProvider *routeProvider)
    : rpcAdapter_(rpcAdapter),
      clientWorkerAddress_(std::move(clientWorkerAddress)),
      routeProvider_(routeProvider)
{
}

Status DirectReadMetaClientAdapter::QueryMeta(const HostPort &metaAddress, const std::vector<std::string> &objectKeys,
                                              int64_t subTimeoutMs, master::QueryMetaRspPb &rsp,
                                              std::vector<RpcMessage> &payloads)
{
    RETURN_RUNTIME_ERROR_IF_NULL(rpcAdapter_);
    GetParam getParam { objectKeys, subTimeoutMs, {}, false };
    DirectReadRpcAdapter::RouteRefreshFn refreshRoute;
    if (routeProvider_ != nullptr) {
        refreshRoute = [this]() { return routeProvider_->RefreshRouteIfNeeded(); };
    }
    return rpcAdapter_->QueryMeta(metaAddress, clientWorkerAddress_, getParam, rsp, payloads, refreshRoute);
}

DirectReadDataClientAdapter::DirectReadDataClientAdapter(DirectReadRpcAdapter *rpcAdapter) : rpcAdapter_(rpcAdapter)
{
}

void DirectReadDataClientAdapter::SetGetParam(const GetParam *getParam)
{
    getParam_ = getParam;
}

Status DirectReadDataClientAdapter::ReadData(const master::QueryMetaInfoPb &queryMeta, int64_t subTimeoutMs,
                                             size_t objectIndex, GetObjectRemoteRspPb &rsp,
                                             std::vector<RpcMessage> &payloads)
{
    (void)subTimeoutMs;
    RETURN_RUNTIME_ERROR_IF_NULL(rpcAdapter_);
    RETURN_RUNTIME_ERROR_IF_NULL(getParam_);
    if (queryMeta.address().empty()) {
        return Status(K_RUNTIME_ERROR, DirectReadFlow::kDataWorkerUnavailableFallbackReason);
    }
    HostPort dataAddress;
    RETURN_IF_NOT_OK(dataAddress.ParseString(queryMeta.address()));
    Status remoteRc = rpcAdapter_->GetObjectRemoteTcp(dataAddress, queryMeta, *getParam_, objectIndex, rsp, payloads);
    if (remoteRc.IsError()) {
        return Status(remoteRc.GetCode(), DirectReadFlow::kDataWorkerUnavailableFallbackReason);
    }
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
