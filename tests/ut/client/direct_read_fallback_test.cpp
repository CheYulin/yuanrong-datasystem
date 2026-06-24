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

#include <gtest/gtest.h>

#include "ut/common.h"
#include "datasystem/client/object_cache/direct_read/direct_read_fallback.h"
#include "datasystem/client/object_cache/direct_read/direct_read_flow.h"

namespace datasystem {
namespace ut {
namespace {
TEST(DirectReadFallbackTest, OuterMetaPhaseRetryIgnoresMovingStatuses)
{
    using object_cache::DirectReadFallback;
    using object_cache::DirectReadFlow;

    EXPECT_FALSE(DirectReadFallback::IsOuterMetaPhaseRetriable(Status(K_TRY_AGAIN, "meta_is_moving")));
    EXPECT_FALSE(DirectReadFallback::IsOuterMetaPhaseRetriable(
        Status(K_TRY_AGAIN, DirectReadFlow::kMetaMovingFallbackReason)));
    EXPECT_FALSE(DirectReadFallback::IsOuterMetaPhaseRetriable(
        Status(K_RUNTIME_ERROR, DirectReadFlow::kRedirectLoopFallbackReason)));
}

TEST(DirectReadFallbackTest, OuterMetaPhaseRetryAllowsStaleRouteAndRouteUnavailable)
{
    using object_cache::DirectReadFallback;
    using object_cache::DirectReadFlow;

    EXPECT_TRUE(DirectReadFallback::IsOuterMetaPhaseRetriable(
        Status(K_NOT_READY, DirectReadFlow::kRouteUnavailableFallbackReason)));
    EXPECT_TRUE(DirectReadFallback::IsOuterMetaPhaseRetriable(
        Status(K_TRY_AGAIN, DirectReadFlow::kStaleRouteFallbackReason)));
    EXPECT_TRUE(DirectReadFallback::IsOuterMetaPhaseRetriable(Status(K_NOT_READY, "route_unavailable")));
}
}  // namespace
}  // namespace ut
}  // namespace datasystem
