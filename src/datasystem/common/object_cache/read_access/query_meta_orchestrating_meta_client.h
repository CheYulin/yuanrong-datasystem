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
#ifndef DATASYSTEM_COMMON_OBJECT_CACHE_READ_ACCESS_QUERY_META_ORCHESTRATING_META_CLIENT_H
#define DATASYSTEM_COMMON_OBJECT_CACHE_READ_ACCESS_QUERY_META_ORCHESTRATING_META_CLIENT_H

#include <memory>
#include <vector>

#include "datasystem/common/object_cache/read_access/object_read_access_flow.h"
#include "datasystem/common/object_cache/read_access/query_meta_redirect_helper.h"
#include "datasystem/common/object_cache/read_access/query_meta_transport.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
class QueryMetaOrchestratingMetaClient : public IObjectReadMetaClient {
public:
    struct Options {
        QueryMetaMovingRetryOptions moving;
        QueryMetaRedirectFollowOptions redirect;
    };

    QueryMetaOrchestratingMetaClient(std::shared_ptr<IQueryMetaTransport> transport, Options options);

    Status QueryMeta(const HostPort &metaAddress, const std::vector<std::string> &objectKeys, int64_t subTimeoutMs,
                     master::QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads) override;

private:
    std::shared_ptr<IQueryMetaTransport> transport_;
    Options options_;
};
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_OBJECT_CACHE_READ_ACCESS_QUERY_META_ORCHESTRATING_META_CLIENT_H
