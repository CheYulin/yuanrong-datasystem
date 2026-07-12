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
 * Description: In-process mock IPC HBM backend for binmock / UT (same pointer round-trip).
 */
#ifndef DATASYSTEM_COMMON_DEVICE_HBM_IPC_MOCK_IPC_HBM_BACKEND_H
#define DATASYSTEM_COMMON_DEVICE_HBM_IPC_MOCK_IPC_HBM_BACKEND_H

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "datasystem/common/device/hbm_ipc/ipc_hbm_backend.h"

namespace datasystem {
namespace hbm_ipc {

class MockIpcHbmBackend : public IpcHbmBackend {
public:
    Status Export(void *localVa, size_t size, int32_t deviceIdx, IpcExportHandle &out) override;
    Status AllowImportPid(const IpcExportHandle &handle, pid_t peerPid) override;
    Status Import(const IpcExportHandle &handle, int32_t deviceIdx, void **localVa, size_t *size) override;
    Status Close(void *localVa) override;

    // Test helper: reset all registrations.
    void Reset();

private:
    struct ExportEntry {
        void *localVa = nullptr;
        size_t size = 0;
        int32_t deviceIdx = 0;
        std::unordered_set<pid_t> allowedPids;
    };

    std::mutex mu_;
    std::unordered_map<std::string, ExportEntry> exports_;
    std::unordered_map<void *, std::string> vaToKey_;
    uint64_t nextId_ = 0;
};

}  // namespace hbm_ipc
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_DEVICE_HBM_IPC_MOCK_IPC_HBM_BACKEND_H
