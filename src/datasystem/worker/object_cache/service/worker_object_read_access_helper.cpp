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
 * Description: Worker helper for ObjectReadAccessFlow meta phase.
 */
#include "datasystem/worker/object_cache/service/worker_object_read_access_helper.h"

#include <memory>
#include <utility>
#include <vector>

#include "datasystem/common/object_cache/read_access/object_read_access_flow.h"
#include "datasystem/common/object_cache/read_access/query_meta_orchestrating_meta_client.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_get_impl.h"
#include "datasystem/worker/object_cache/service/worker_query_meta_transport.h"

namespace datasystem {
namespace object_cache {
namespace {
class WorkerFixedMetaRouteProvider : public IObjectReadRouteProvider {
public:
    explicit WorkerFixedMetaRouteProvider(HostPort metaAddress) : metaAddress_(std::move(metaAddress))
    {
    }

    Status GetMetaAddress(const std::string &objectKey, HostPort &metaAddress) override
    {
        (void)objectKey;
        metaAddress = metaAddress_;
        return Status::OK();
    }

    Status RefreshRouteIfNeeded() override
    {
        return Status::OK();
    }

private:
    HostPort metaAddress_;
};

class WorkerObjectReadMetaClient : public IObjectReadMetaClient {
public:
    WorkerObjectReadMetaClient(WorkerOcServiceGetImpl *getImpl, bool isFromOtherAz)
    {
        auto transport = std::make_shared<WorkerQueryMetaTransport>(getImpl, isFromOtherAz);
        metaClient_ = std::make_shared<QueryMetaOrchestratingMetaClient>(transport,
                                                                         getImpl->BuildQueryMetaOrchestratingOptions());
    }

    Status QueryMeta(const HostPort &metaAddress, const std::vector<std::string> &objectKeys, int64_t subTimeoutMs,
                     master::QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads) override
    {
        RETURN_RUNTIME_ERROR_IF_NULL(metaClient_);
        return metaClient_->QueryMeta(metaAddress, objectKeys, subTimeoutMs, rsp, payloads);
    }

private:
    std::shared_ptr<QueryMetaOrchestratingMetaClient> metaClient_;
};

class WorkerNullDataClient : public IObjectReadDataClient {
public:
    Status ReadData(const master::QueryMetaInfoPb &queryMeta, int64_t subTimeoutMs, size_t objectIndex,
                    GetObjectRemoteRspPb &rsp, std::vector<RpcMessage> &payloads) override
    {
        (void)queryMeta;
        (void)subTimeoutMs;
        (void)objectIndex;
        (void)rsp;
        (void)payloads;
        return Status(K_NOT_SUPPORTED, "worker_meta_only");
    }
};
}  // namespace

Status QueryMetaGroupUsingSharedFlow(WorkerOcServiceGetImpl &getImpl, const HostPort &masterAddress,
                                     const std::vector<std::string> &objectKeys, bool isFromOtherAz,
                                     uint64_t subTimeout, master::QueryMetaRspPb &rsp,
                                     std::vector<RpcMessage> &payloads)
{
    auto routeProvider = std::make_shared<WorkerFixedMetaRouteProvider>(masterAddress);
    auto metaClient = std::make_shared<WorkerObjectReadMetaClient>(&getImpl, isFromOtherAz);
    auto dataClient = std::make_shared<WorkerNullDataClient>();
    ObjectReadAccessFlow flow(routeProvider, metaClient, dataClient);

    ObjectReadAccessRequest request;
    request.objectKeys = objectKeys;
    request.subTimeoutMs = static_cast<int64_t>(subTimeout);
    request.clientWorkerAddress = masterAddress;

    ObjectReadAccessMetaResult result;
    RETURN_IF_NOT_OK(flow.ExecuteMetaPhase(request, result));
    rsp.CopyFrom(result.metaRsp);
    payloads = std::move(result.metaPayloads);
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
