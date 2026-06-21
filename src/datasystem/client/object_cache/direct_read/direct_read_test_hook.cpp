/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */
#include "datasystem/client/object_cache/direct_read/direct_read_test_hook.h"

#include <mutex>

#include "datasystem/common/flags/flags.h"

DS_DEFINE_bool(enable_client_direct_read, false, "Enable client direct read for cross-node object cache reads.");
DS_DEFINE_bool(enable_client_direct_read_fallback, true,
               "Fallback to the existing client-worker read path when client direct read fails.");
DS_DEFINE_int32(client_direct_read_retry_count, 1, "Retry count for client direct read route refresh and moving states.");

namespace datasystem {
namespace object_cache {
namespace {
std::mutex g_directReadStatsMutex;
DirectReadStats g_directReadStats;
bool g_forceDirectRead = false;
bool g_preferRemoteDataGet = false;
}  // namespace

void DirectReadTestHook::Reset()
{
    std::lock_guard<std::mutex> lock(g_directReadStatsMutex);
    g_directReadStats = DirectReadStats{};
    g_forceDirectRead = false;
    g_preferRemoteDataGet = false;
}

DirectReadStats DirectReadTestHook::Snapshot()
{
    std::lock_guard<std::mutex> lock(g_directReadStatsMutex);
    return g_directReadStats;
}

void DirectReadTestHook::SetForceDirectRead(bool enabled)
{
    std::lock_guard<std::mutex> lock(g_directReadStatsMutex);
    g_forceDirectRead = enabled;
}

bool DirectReadTestHook::ForceDirectRead()
{
    std::lock_guard<std::mutex> lock(g_directReadStatsMutex);
    return g_forceDirectRead;
}

void DirectReadTestHook::RecordDirectAttempt()
{
    std::lock_guard<std::mutex> lock(g_directReadStatsMutex);
    ++g_directReadStats.directAttemptCount;
}

void DirectReadTestHook::RecordRouteQuery()
{
    std::lock_guard<std::mutex> lock(g_directReadStatsMutex);
    ++g_directReadStats.routeQueryCount;
}

void DirectReadTestHook::RecordMetaQuery()
{
    std::lock_guard<std::mutex> lock(g_directReadStatsMutex);
    ++g_directReadStats.metaQueryCount;
}

void DirectReadTestHook::RecordDataQuery()
{
    std::lock_guard<std::mutex> lock(g_directReadStatsMutex);
    ++g_directReadStats.dataQueryCount;
}

void DirectReadTestHook::SetPreferRemoteDataGet(bool enabled)
{
    std::lock_guard<std::mutex> lock(g_directReadStatsMutex);
    g_preferRemoteDataGet = enabled;
}

bool DirectReadTestHook::PreferRemoteDataGet()
{
    std::lock_guard<std::mutex> lock(g_directReadStatsMutex);
    return g_preferRemoteDataGet;
}

void DirectReadTestHook::RecordPathFallback(const std::string &reason)
{
    std::lock_guard<std::mutex> lock(g_directReadStatsMutex);
    ++g_directReadStats.pathFallbackCount;
    g_directReadStats.lastFallbackReason = reason;
}
}  // namespace object_cache
}  // namespace datasystem
