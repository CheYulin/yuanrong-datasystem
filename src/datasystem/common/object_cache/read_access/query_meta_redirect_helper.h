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
 * Description: Shared QueryMeta redirect/moving control-plane logic for client and worker.
 */
#ifndef DATASYSTEM_COMMON_OBJECT_CACHE_READ_ACCESS_QUERY_META_REDIRECT_HELPER_H
#define DATASYSTEM_COMMON_OBJECT_CACHE_READ_ACCESS_QUERY_META_REDIRECT_HELPER_H

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
using QueryMetaAtMasterFn = std::function<Status(const HostPort &metaAddress, const std::vector<std::string> &objectKeys,
                                                 bool enableRedirect, master::QueryMetaRspPb &rsp,
                                                 std::vector<RpcMessage> &payloads)>;

using ResolveRedirectMetaAddressFn = std::function<Status(const std::string &redirectAddress, HostPort &metaAddress)>;

struct QueryMetaMovingRetryOptions {
    static constexpr int64_t kInitSleepMs = 1;
    static constexpr int64_t kMaxSleepMs = 128;

    int32_t maxMovingRetries = -1;
    std::function<int64_t()> remainingDeadlineMs;
    std::function<Status()> beforeMovingRetry;
    std::function<void()> onMovingRetry;
    Status movingRetryExceededStatus = Status(K_TRY_AGAIN, "meta_is_moving");
    int64_t maxSleepCapMs = kMaxSleepMs;
    int64_t subTimeoutMs = 0;
};

struct QueryMetaRedirectFollowOptions {
    ResolveRedirectMetaAddressFn resolveRedirectAddress;
    std::function<void()> onRedirectRetry;
    Status emptyRedirectAddressError = Status(K_RUNTIME_ERROR, "redirect_loop");
    Status nestedRedirectError = Status(K_RUNTIME_ERROR, "redirect_loop");
    Status movingOnRedirectError = Status(K_TRY_AGAIN, "meta_is_moving");
    bool rejectMovingOnRedirect = true;
    bool rejectNestedRedirectInfo = true;
};

bool ShouldRetryQueryMetaMoving(const master::QueryMetaRspPb &rsp);

Status RetryWhileMetaIsMoving(master::QueryMetaRspPb &rsp, std::function<Status()> requery,
                              std::function<void()> clearResponse, const QueryMetaMovingRetryOptions &options);

Status FollowQueryMetaRedirects(QueryMetaAtMasterFn queryMeta, const QueryMetaRedirectFollowOptions &options,
                                master::QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads);

Status QueryMetaWithRedirectAndMoving(const HostPort &primaryMetaAddress, const std::vector<std::string> &objectKeys,
                                      QueryMetaAtMasterFn queryMeta, const QueryMetaMovingRetryOptions &movingOptions,
                                      const QueryMetaRedirectFollowOptions &redirectOptions,
                                      master::QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads);

Status ResolveRedirectMetaAddressByParse(const std::string &redirectAddress, HostPort &metaAddress);
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_OBJECT_CACHE_READ_ACCESS_QUERY_META_REDIRECT_HELPER_H
