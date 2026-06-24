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
 * Description: Default IObjectReadMetaClient with redirect/moving orchestration.
 */
#include "datasystem/common/object_cache/read_access/query_meta_orchestrating_meta_client.h"

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace object_cache {
QueryMetaOrchestratingMetaClient::QueryMetaOrchestratingMetaClient(std::shared_ptr<IQueryMetaTransport> transport,
                                                                   Options options)
    : transport_(std::move(transport)), options_(std::move(options))
{
}

Status QueryMetaOrchestratingMetaClient::QueryMeta(const HostPort &metaAddress,
                                                 const std::vector<std::string> &objectKeys, int64_t subTimeoutMs,
                                                 master::QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads)
{
    RETURN_RUNTIME_ERROR_IF_NULL(transport_);
    QueryMetaAtMasterFn queryMeta = [this, subTimeoutMs](const HostPort &queryMetaAddress,
                                                         const std::vector<std::string> &keys, bool enableRedirect,
                                                         master::QueryMetaRspPb &queryRsp,
                                                         std::vector<RpcMessage> &queryPayloads) -> Status {
        return transport_->QueryMetaOnce(queryMetaAddress, keys, subTimeoutMs, enableRedirect, queryRsp,
                                         queryPayloads);
    };
    auto movingOpts = options_.moving;
    movingOpts.subTimeoutMs = subTimeoutMs;
    return QueryMetaWithRedirectAndMoving(metaAddress, objectKeys, queryMeta, movingOpts, options_.redirect, rsp,
                                          payloads);
}
}  // namespace object_cache
}  // namespace datasystem
