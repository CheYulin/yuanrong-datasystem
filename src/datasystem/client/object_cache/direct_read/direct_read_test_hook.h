/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
    static void RecordPathFallback(const std::string &reason);
    static void SetPreferRemoteDataGet(bool enabled);
    static bool PreferRemoteDataGet();
};
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_DIRECT_READ_TEST_HOOK_H
