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
 * Description: Minimal RPC adapter for client direct read.
 */
#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_DIRECT_READ_RPC_ADAPTER_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_DIRECT_READ_RPC_ADAPTER_H

#include <memory>
#include <vector>

#include "datasystem/client/object_cache/client_worker_api/iclient_worker_api.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/rpc/rpc_credential.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/hash_ring.pb.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
class DirectReadRpcAdapter {
public:
    DirectReadRpcAdapter(RpcCredential cred, Signature *signature, int32_t requestTimeoutMs);

    Status QueryMeta(const HostPort &metaAddress, const HostPort &clientWorkerAddress, const GetParam &getParam,
                     master::QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads) const;

    Status GetClusterState(const HostPort &workerAddress, HashRingPb &ring, int64_t &version) const;

private:
    RpcCredential cred_;
    Signature *signature_;
    int32_t requestTimeoutMs_;
};
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_DIRECT_READ_RPC_ADAPTER_H
