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
 * Description: Client single-shot QueryMeta RPC transport.
 */
#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_CLIENT_QUERY_META_TRANSPORT_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_CLIENT_QUERY_META_TRANSPORT_H

#include <cstdint>
#include <vector>

#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/object_cache/read_access/query_meta_transport.h"
#include "datasystem/common/rpc/rpc_credential.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
class ClientQueryMetaTransport : public IQueryMetaTransport {
public:
    ClientQueryMetaTransport(RpcCredential cred, Signature *signature, int32_t requestTimeoutMs,
                             HostPort clientWorkerAddress);

    Status QueryMetaOnce(const HostPort &metaAddress, const std::vector<std::string> &objectKeys, int64_t subTimeoutMs,
                         bool enableRedirect, master::QueryMetaRspPb &rsp,
                         std::vector<RpcMessage> &payloads) override;

private:
    RpcCredential cred_;
    Signature *signature_;
    int32_t requestTimeoutMs_;
    HostPort clientWorkerAddress_;
};
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_CLIENT_QUERY_META_TRANSPORT_H
