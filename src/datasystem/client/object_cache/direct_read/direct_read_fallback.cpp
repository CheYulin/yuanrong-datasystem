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
 * Description: Path fallback reason helpers for client direct read.
 */
#include "datasystem/client/object_cache/direct_read/direct_read_fallback.h"

#include "datasystem/client/object_cache/direct_read/direct_read_flow.h"

namespace datasystem {
namespace object_cache {
namespace {
constexpr const char *kTraceIdSuffix = ", traceId:";

bool ContainsReasonToken(const std::string &text, const char *token)
{
    return text.find(token) != std::string::npos;
}
}  // namespace

std::string DirectReadFallback::NormalizeReason(const std::string &reason)
{
    const auto pos = reason.find(kTraceIdSuffix);
    if (pos == std::string::npos) {
        return reason;
    }
    return reason.substr(0, pos);
}

bool DirectReadFallback::IsControlPlaneFailure(const Status &status)
{
    if (status.IsOk()) {
        return false;
    }
    const auto &msg = status.GetMsg();
    if (status.GetCode() == K_TRY_AGAIN && ContainsReasonToken(msg, "meta_is_moving")) {
        return true;
    }
    if (status.GetCode() == K_NOT_READY) {
        return true;
    }
    if (status.GetCode() == K_RPC_DEADLINE_EXCEEDED || status.GetCode() == K_RPC_UNAVAILABLE
        || status.GetCode() == K_WORKER_TIMEOUT) {
        return true;
    }
    return ContainsReasonToken(msg, DirectReadFlow::kStaleRouteFallbackReason)
           || ContainsReasonToken(msg, DirectReadFlow::kRedirectLoopFallbackReason)
           || ContainsReasonToken(msg, DirectReadFlow::kMetaTimeoutFallbackReason)
           || ContainsReasonToken(msg, DirectReadFlow::kMetaMovingFallbackReason)
           || ContainsReasonToken(msg, DirectReadFlow::kRouteUnavailableFallbackReason);
}

bool DirectReadFallback::IsRetriableControlPlaneFailure(const Status &status)
{
    if (!IsControlPlaneFailure(status)) {
        return false;
    }
    if (status.GetCode() == K_RPC_DEADLINE_EXCEEDED || status.GetCode() == K_WORKER_TIMEOUT) {
        return false;
    }
    return ContainsReasonToken(status.GetMsg(), DirectReadFlow::kRedirectLoopFallbackReason)
           || ContainsReasonToken(status.GetMsg(), DirectReadFlow::kMetaMovingFallbackReason)
           || ContainsReasonToken(status.GetMsg(), DirectReadFlow::kStaleRouteFallbackReason)
           || ContainsReasonToken(status.GetMsg(), DirectReadFlow::kRouteUnavailableFallbackReason)
           || (status.GetCode() == K_TRY_AGAIN && ContainsReasonToken(status.GetMsg(), "meta_is_moving"))
           || status.GetCode() == K_NOT_READY;
}

Status DirectReadFallback::ToPathFallbackStatus(const Status &status)
{
    if (status.IsOk()) {
        return status;
    }
    const auto &msg = status.GetMsg();
    if (ContainsReasonToken(msg, DirectReadFlow::kStaleRouteFallbackReason)) {
        return Status(K_TRY_AGAIN, DirectReadFlow::kStaleRouteFallbackReason);
    }
    if (ContainsReasonToken(msg, DirectReadFlow::kRedirectLoopFallbackReason)) {
        return Status(K_RUNTIME_ERROR, DirectReadFlow::kRedirectLoopFallbackReason);
    }
    if (ContainsReasonToken(msg, DirectReadFlow::kMetaTimeoutFallbackReason)) {
        return Status(K_RPC_DEADLINE_EXCEEDED, DirectReadFlow::kMetaTimeoutFallbackReason);
    }
    if (ContainsReasonToken(msg, DirectReadFlow::kMetaMovingFallbackReason)
        || (status.GetCode() == K_TRY_AGAIN && ContainsReasonToken(msg, "meta_is_moving"))) {
        return Status(K_TRY_AGAIN, DirectReadFlow::kMetaMovingFallbackReason);
    }
    if (status.GetCode() == K_NOT_READY) {
        return Status(K_NOT_READY, DirectReadFlow::kRouteUnavailableFallbackReason);
    }
    if (status.GetCode() == K_RPC_DEADLINE_EXCEEDED || status.GetCode() == K_WORKER_TIMEOUT) {
        return Status(status.GetCode(), DirectReadFlow::kMetaTimeoutFallbackReason);
    }
    if (status.GetCode() == K_RPC_UNAVAILABLE) {
        return Status(K_RPC_UNAVAILABLE, DirectReadFlow::kMetaTimeoutFallbackReason);
    }
    return status;
}
}  // namespace object_cache
}  // namespace datasystem
