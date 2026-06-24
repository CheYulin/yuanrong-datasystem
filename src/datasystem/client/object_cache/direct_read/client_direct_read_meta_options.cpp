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
 * Description: Client QueryMeta orchestration options for direct read.
 */
#include "datasystem/client/object_cache/direct_read/client_direct_read_meta_options.h"

#include <algorithm>

#include "datasystem/client/object_cache/direct_read/direct_read_flow.h"
#include "datasystem/client/object_cache/direct_read/direct_read_route_provider.h"
#include "datasystem/client/object_cache/direct_read/direct_read_test_hook.h"
#include "datasystem/common/flags/flags.h"

DS_DECLARE_int32(client_direct_read_retry_count);

namespace datasystem {
namespace object_cache {
namespace {
int32_t MaxControlPlaneRetries()
{
    return std::max(0, FLAGS_client_direct_read_retry_count);
}
}  // namespace

QueryMetaOrchestratingMetaClient::Options BuildClientDirectReadMetaOptions(
    DirectReadRouteProvider *routeProvider, std::function<Status()> refreshRoute)
{
    (void)routeProvider;
    QueryMetaOrchestratingMetaClient::Options options;
    options.moving.maxMovingRetries = MaxControlPlaneRetries();
    options.moving.movingRetryExceededStatus = Status(K_TRY_AGAIN, DirectReadFlow::kMetaMovingFallbackReason);
    options.moving.beforeMovingRetry = std::move(refreshRoute);
    options.moving.onMovingRetry = []() { DirectReadTestHook::RecordMovingRetry(); };

    options.redirect.onRedirectRetry = []() { DirectReadTestHook::RecordRedirectRetry(); };
    options.redirect.emptyRedirectAddressError = Status(K_RUNTIME_ERROR, DirectReadFlow::kRedirectLoopFallbackReason);
    options.redirect.nestedRedirectError = Status(K_RUNTIME_ERROR, DirectReadFlow::kRedirectLoopFallbackReason);
    options.redirect.movingOnRedirectError = Status(K_TRY_AGAIN, DirectReadFlow::kMetaMovingFallbackReason);
    return options;
}
}  // namespace object_cache
}  // namespace datasystem
