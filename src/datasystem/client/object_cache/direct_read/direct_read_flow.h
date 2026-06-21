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
 * Description: Client direct read flow skeleton.
 */
#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_DIRECT_READ_FLOW_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_DIRECT_READ_FLOW_H

#include <functional>
#include <memory>
#include <vector>

#include "datasystem/client/object_cache/client_worker_api/iclient_worker_api.h"
#include "datasystem/client/object_cache/direct_read/direct_read_route_provider.h"
#include "datasystem/client/object_cache/direct_read/direct_read_rpc_adapter.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/rpc/rpc_credential.h"
#include "datasystem/object/buffer.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
using DirectReadFinishGetFn = std::function<Status(const GetParam &, GetRspPb &, std::vector<RpcMessage> &,
                                                   std::vector<std::shared_ptr<Buffer>> &)>;

class DirectReadFlow {
public:
    static constexpr const char *kNotImplementedFallbackReason = "direct_flow_not_implemented";
    static constexpr const char *kDataWorkerUnavailableFallbackReason = "data_worker_unavailable";

    DirectReadFlow(std::shared_ptr<IClientWorkerApi> workerApi, RpcCredential cred, Signature *signature,
                   int32_t requestTimeoutMs);

    Status Get(const GetParam &getParam, std::vector<std::shared_ptr<Buffer>> &buffers,
               const DirectReadFinishGetFn &finishGet);

private:
    std::shared_ptr<IClientWorkerApi> workerApi_;
    DirectReadRpcAdapter rpcAdapter_;
    DirectReadRouteProvider routeProvider_;
};
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_DIRECT_READ_FLOW_H
