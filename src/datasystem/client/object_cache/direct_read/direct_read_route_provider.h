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
 * Description: Route provider for client direct read backed by ClientHashRingSource.
 */
#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_DIRECT_READ_ROUTE_PROVIDER_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_DIRECT_READ_ROUTE_PROVIDER_H

#include <memory>

#include "datasystem/client/object_cache/client_worker_api/iclient_worker_api.h"
#include "datasystem/client/object_cache/direct_read/client_hash_ring_source.h"
#include "datasystem/client/object_cache/direct_read/direct_read_rpc_adapter.h"
#include "datasystem/common/object_cache/read_access/object_read_meta_access_flow.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
class DirectReadRouteProvider : public IObjectReadRouteProvider {
public:
    DirectReadRouteProvider(std::shared_ptr<IClientWorkerApi> workerApi,
                            std::shared_ptr<DirectReadRpcAdapter> rpcAdapter);
    explicit DirectReadRouteProvider(std::shared_ptr<ClientHashRingSource> sharedRingSource);

    Status GetMetaAddress(const GetParam &getParam, HostPort &metaAddress);
    Status GetMetaAddress(const std::string &objectKey, HostPort &metaAddress) override;
    Status RefreshRouteIfNeeded() override;
    Status RefreshRouteOnClusterEvent();

    ClientHashRingSource &HashRingSourceForTest();

private:
    ClientHashRingSource &RingSource();

    std::shared_ptr<ClientHashRingSource> ringSource_;
};
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_DIRECT_READ_ROUTE_PROVIDER_H
