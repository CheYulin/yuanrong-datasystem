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
 * Description: In-process mock IPC HBM backend.
 */
#include "datasystem/common/device/hbm_ipc/mock_ipc_hbm_backend.h"

#include <unistd.h>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace hbm_ipc {

Status MockIpcHbmBackend::Export(void *localVa, size_t size, int32_t deviceIdx, IpcExportHandle &out)
{
    CHECK_FAIL_RETURN_STATUS(localVa != nullptr, StatusCode::K_INVALID, "Export localVa is null");
    CHECK_FAIL_RETURN_STATUS(size > 0, StatusCode::K_INVALID, "Export size is zero");

    std::lock_guard<std::mutex> lock(mu_);
    auto it = vaToKey_.find(localVa);
    if (it != vaToKey_.end()) {
        out.key = it->second;
        return Status::OK();
    }

    const std::string key = "mock-ipc-" + std::to_string(nextId_++);
    ExportEntry entry;
    entry.localVa = localVa;
    entry.size = size;
    entry.deviceIdx = deviceIdx;
    exports_.emplace(key, entry);
    vaToKey_.emplace(localVa, key);
    out.key = key;
    return Status::OK();
}

Status MockIpcHbmBackend::AllowImportPid(const IpcExportHandle &handle, pid_t peerPid)
{
    CHECK_FAIL_RETURN_STATUS(!handle.key.empty(), StatusCode::K_INVALID, "empty export handle");

    std::lock_guard<std::mutex> lock(mu_);
    auto it = exports_.find(handle.key);
    CHECK_FAIL_RETURN_STATUS(it != exports_.end(), StatusCode::K_NOT_FOUND, "export handle not found");
    it->second.allowedPids.insert(peerPid);
    return Status::OK();
}

Status MockIpcHbmBackend::Import(const IpcExportHandle &handle, int32_t deviceIdx, void **localVa, size_t *size)
{
    CHECK_FAIL_RETURN_STATUS(!handle.key.empty(), StatusCode::K_INVALID, "empty export handle");
    CHECK_FAIL_RETURN_STATUS(localVa != nullptr, StatusCode::K_INVALID, "Import localVa out is null");
    CHECK_FAIL_RETURN_STATUS(size != nullptr, StatusCode::K_INVALID, "Import size out is null");

    std::lock_guard<std::mutex> lock(mu_);
    auto it = exports_.find(handle.key);
    CHECK_FAIL_RETURN_STATUS(it != exports_.end(), StatusCode::K_NOT_FOUND, "export handle not found");

    const ExportEntry &entry = it->second;
    if (!entry.allowedPids.empty()) {
        const pid_t self = getpid();
        CHECK_FAIL_RETURN_STATUS(entry.allowedPids.count(self) > 0, StatusCode::K_INVALID, "pid not allowed to import");
    }
    if (deviceIdx >= 0 && entry.deviceIdx >= 0 && deviceIdx != entry.deviceIdx) {
        RETURN_STATUS(StatusCode::K_INVALID, "deviceIdx mismatch on import");
    }

    *localVa = entry.localVa;
    *size = entry.size;
    return Status::OK();
}

Status MockIpcHbmBackend::Close(void *localVa)
{
    CHECK_FAIL_RETURN_STATUS(localVa != nullptr, StatusCode::K_INVALID, "Close localVa is null");

    std::lock_guard<std::mutex> lock(mu_);
    auto vaIt = vaToKey_.find(localVa);
    CHECK_FAIL_RETURN_STATUS(vaIt != vaToKey_.end(), StatusCode::K_NOT_FOUND, "va not exported");

    exports_.erase(vaIt->second);
    vaToKey_.erase(vaIt);
    return Status::OK();
}

void MockIpcHbmBackend::Reset()
{
    std::lock_guard<std::mutex> lock(mu_);
    exports_.clear();
    vaToKey_.clear();
    nextId_ = 0;
}

}  // namespace hbm_ipc
}  // namespace datasystem
