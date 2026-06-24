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
#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_DIRECT_READ_ACCESS_ADAPTERS_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_DIRECT_READ_ACCESS_ADAPTERS_H

#include <vector>

#include "datasystem/client/object_cache/direct_read/direct_read_route_provider.h"
#include "datasystem/client/object_cache/direct_read/direct_read_rpc_adapter.h"
#include "datasystem/common/object_cache/read_access/object_read_access_flow.h"
#include "datasystem/common/util/net_util.h"

namespace datasystem {
namespace object_cache {
class DirectReadRouteProviderAdapter : public IObjectReadRouteProvider {
public:
    explicit DirectReadRouteProviderAdapter(DirectReadRouteProvider *provider);

    Status GetMetaAddress(const std::string &objectKey, HostPort &metaAddress) override;
    Status RefreshRouteIfNeeded() override;

private:
    DirectReadRouteProvider *provider_;
};

class DirectReadDataClientAdapter : public IObjectReadDataClient {
public:
    explicit DirectReadDataClientAdapter(DirectReadRpcAdapter *rpcAdapter);

    void SetGetParam(const GetParam *getParam);

    Status ReadData(const master::QueryMetaInfoPb &queryMeta, int64_t subTimeoutMs, size_t objectIndex,
                    GetObjectRemoteRspPb &rsp, std::vector<RpcMessage> &payloads) override;

private:
    DirectReadRpcAdapter *rpcAdapter_;
    const GetParam *getParam_ = nullptr;
};
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_DIRECT_READ_ACCESS_ADAPTERS_H
