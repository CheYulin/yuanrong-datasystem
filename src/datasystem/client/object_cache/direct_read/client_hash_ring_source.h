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
 * Description: Client-side hash ring refresh source (bootstrap etcd, then worker with etcd fallback).
 */
#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_CLIENT_HASH_RING_SOURCE_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_CLIENT_HASH_RING_SOURCE_H

#include <memory>
#include <string>

#include "datasystem/client/object_cache/client_worker_api/iclient_worker_api.h"
#include "datasystem/client/object_cache/direct_read/direct_read_rpc_adapter.h"
#include "datasystem/common/object_cache/read_only_hash_ring_view.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {
class EtcdStore;

namespace object_cache {
class ClientHashRingSource {
public:
    ClientHashRingSource(std::shared_ptr<IClientWorkerApi> workerApi, DirectReadRpcAdapter *rpcAdapter);

    Status GetMetaAddress(const std::string &objectKey, HostPort &metaAddress);
    Status RefreshForRouteLookup();

    ReadOnlyHashRingView &ViewForTest();
    HashRingRefreshSource LastRefreshSource() const;

private:
    Status EnsureInitialized();
    Status LoadFromEtcd();
    Status LoadFromWorker();
    Status BootstrapRing();
    Status RefreshRing();

    std::shared_ptr<IClientWorkerApi> workerApi_;
    DirectReadRpcAdapter *rpcAdapter_;
    ReadOnlyHashRingView view_;
    std::shared_ptr<EtcdStore> etcdStore_;
    HashRingRefreshSource lastRefreshSource_ = HashRingRefreshSource::NONE;
};
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_CLIENT_HASH_RING_SOURCE_H
