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
#include "datasystem/common/object_cache/read_access/query_meta_redirect_helper.h"

#include <algorithm>
#include <chrono>
#include <thread>

#include "datasystem/common/object_cache/read_access/query_meta_merge_helper.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace object_cache {
namespace {
Status CheckRetryBudget(int32_t movingAttempts, const QueryMetaMovingRetryOptions &options)
{
    if (options.maxMovingRetries >= 0 && movingAttempts > options.maxMovingRetries) {
        return options.movingRetryExceededStatus;
    }
    if (options.remainingDeadlineMs && options.remainingDeadlineMs() <= 0) {
        RETURN_STATUS(K_RPC_DEADLINE_EXCEEDED, "Rpc timeout");
    }
    return Status::OK();
}

int64_t NextBackoffSleepMs(int64_t sleepTimeMs, const QueryMetaMovingRetryOptions &options)
{
    int64_t capped = std::min(sleepTimeMs, options.maxSleepCapMs);
    if (options.subTimeoutMs > 0) {
        capped = std::min(capped, options.subTimeoutMs);
    }
    if (options.remainingDeadlineMs) {
        capped = std::min(capped, options.remainingDeadlineMs());
    }
    return capped;
}
}  // namespace

bool ShouldRetryQueryMetaMoving(const master::QueryMetaRspPb &rsp)
{
    return rsp.info_size() > 0 && rsp.meta_is_moving();
}

Status ResolveRedirectMetaAddressByParse(const std::string &redirectAddress, HostPort &metaAddress)
{
    return metaAddress.ParseString(redirectAddress);
}

Status RetryWhileMetaIsMoving(master::QueryMetaRspPb &rsp, std::function<Status()> requery,
                              std::function<void()> clearResponse, const QueryMetaMovingRetryOptions &options)
{
    int32_t movingAttempts = 0;
    int64_t sleepTimeMs = QueryMetaMovingRetryOptions::kInitSleepMs;

    while (ShouldRetryQueryMetaMoving(rsp)) {
        if (options.onMovingRetry) {
            options.onMovingRetry();
        }
        RETURN_IF_NOT_OK(CheckRetryBudget(++movingAttempts, options));
        if (options.beforeMovingRetry) {
            RETURN_IF_NOT_OK(options.beforeMovingRetry());
        }
        if (clearResponse) {
            clearResponse();
        }
        const int64_t sleepMs = NextBackoffSleepMs(sleepTimeMs, options);
        if (sleepMs > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
        }
        sleepTimeMs = std::min(sleepTimeMs * 2, options.maxSleepCapMs);
        RETURN_IF_NOT_OK(requery());
    }
    return Status::OK();
}

Status FollowQueryMetaRedirects(QueryMetaAtMasterFn queryMeta, const QueryMetaRedirectFollowOptions &options,
                                master::QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads)
{
    CHECK_FAIL_RETURN_STATUS(queryMeta != nullptr, K_INVALID, "QueryMeta transport is null");
    const google::protobuf::RepeatedPtrField<RedirectMetaInfo> redirectInfos = rsp.info();
    rsp.clear_info();

    auto resolveRedirectAddress = options.resolveRedirectAddress;
    if (!resolveRedirectAddress) {
        resolveRedirectAddress = ResolveRedirectMetaAddressByParse;
    }

    for (const auto &redirectInfo : redirectInfos) {
        if (redirectInfo.redirect_meta_address().empty()) {
            return options.emptyRedirectAddressError;
        }
        std::vector<std::string> redirectIds = { redirectInfo.change_meta_ids().begin(),
                                                   redirectInfo.change_meta_ids().end() };
        if (redirectIds.empty()) {
            continue;
        }

        HostPort redirectMetaAddress;
        RETURN_IF_NOT_OK(resolveRedirectAddress(redirectInfo.redirect_meta_address(), redirectMetaAddress));
        if (options.onRedirectRetry) {
            options.onRedirectRetry();
        }

        master::QueryMetaRspPb redirectRsp;
        std::vector<RpcMessage> redirectPayloads;
        RETURN_IF_NOT_OK(
            queryMeta(redirectMetaAddress, redirectIds, false, redirectRsp, redirectPayloads));

        if (options.rejectMovingOnRedirect && redirectRsp.meta_is_moving()) {
            return options.movingOnRedirectError;
        }
        if (options.rejectNestedRedirectInfo && redirectRsp.info_size() > 0) {
            return options.nestedRedirectError;
        }

        MergeQueryMetaResponses(rsp, redirectRsp);
        RETURN_IF_NOT_OK(AppendQueryMetaPayloads(payloads, redirectRsp, redirectPayloads));
    }
    return Status::OK();
}

Status QueryMetaWithRedirectAndMoving(const HostPort &primaryMetaAddress, const std::vector<std::string> &objectKeys,
                                      QueryMetaAtMasterFn queryMeta, const QueryMetaMovingRetryOptions &movingOptions,
                                      const QueryMetaRedirectFollowOptions &redirectOptions,
                                      master::QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads)
{
    CHECK_FAIL_RETURN_STATUS(queryMeta != nullptr, K_INVALID, "QueryMeta transport is null");
    std::vector<RpcMessage> primaryPayloads;
    RETURN_IF_NOT_OK(queryMeta(primaryMetaAddress, objectKeys, true, rsp, primaryPayloads));

    QueryMetaMovingRetryOptions movingOpts = movingOptions;
    RETURN_IF_NOT_OK(RetryWhileMetaIsMoving(
        rsp,
        [&]() { return queryMeta(primaryMetaAddress, objectKeys, true, rsp, primaryPayloads); },
        [&]() {
            rsp.Clear();
            primaryPayloads.clear();
        },
        movingOpts));

    if (rsp.meta_is_moving()) {
        return movingOpts.movingRetryExceededStatus;
    }

    payloads.clear();
    RETURN_IF_NOT_OK(AppendQueryMetaPayloads(payloads, rsp, primaryPayloads));
    if (rsp.info_size() > 0) {
        RETURN_IF_NOT_OK(FollowQueryMetaRedirects(queryMeta, redirectOptions, rsp, payloads));
    }
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
