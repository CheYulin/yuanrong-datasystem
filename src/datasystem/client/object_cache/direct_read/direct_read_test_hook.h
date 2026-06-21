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
 * Description: Test-visible counters for the client direct read feature gate.
 */
#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_DIRECT_READ_TEST_HOOK_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_DIRECT_READ_TEST_HOOK_H

#include <cstdint>
#include <string>

namespace datasystem {
namespace object_cache {
struct DirectReadStats {
    uint64_t directAttemptCount = 0;
    uint64_t routeQueryCount = 0;
    uint64_t metaQueryCount = 0;
    uint64_t dataQueryCount = 0;
    uint64_t hashRingEtcdRefreshCount = 0;
    uint64_t hashRingWorkerRefreshCount = 0;
    uint64_t pathFallbackCount = 0;
    std::string lastFallbackReason;
};

class DirectReadTestHook {
public:
    static void Reset();
    static DirectReadStats Snapshot();
    static void SetForceDirectRead(bool enabled);
    static bool ForceDirectRead();
    static void RecordDirectAttempt();
    static void RecordRouteQuery();
    static void RecordMetaQuery();
    static void RecordDataQuery();
    static void SetPreferRemoteDataGet(bool enabled);
    static bool PreferRemoteDataGet();
    static void RecordHashRingEtcdRefresh();
    static void RecordHashRingWorkerRefresh();
    static void RecordPathFallback(const std::string &reason);
    static void SetForceHashRingRefresh(bool enabled);
    static bool ForceHashRingRefresh();
};
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_DIRECT_READ_TEST_HOOK_H
