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
}  // namespace

void DirectReadTestHook::Reset()
{
    std::lock_guard<std::mutex> lock(g_directReadStatsMutex);
    g_directReadStats = DirectReadStats{};
    g_forceDirectRead = false;
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

void DirectReadTestHook::RecordPathFallback(const std::string &reason)
{
    std::lock_guard<std::mutex> lock(g_directReadStatsMutex);
    ++g_directReadStats.pathFallbackCount;
    g_directReadStats.lastFallbackReason = reason;
}
}  // namespace object_cache
}  // namespace datasystem
