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
 * Description: CANN IPC HBM backend abstraction (Export / Import / Close).
 */
#ifndef DATASYSTEM_COMMON_DEVICE_HBM_IPC_IPC_HBM_BACKEND_H
#define DATASYSTEM_COMMON_DEVICE_HBM_IPC_IPC_HBM_BACKEND_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <sys/types.h>

#include "datasystem/utils/status.h"

namespace datasystem {
namespace hbm_ipc {

struct IpcExportHandle {
    std::string key;
};

class IpcHbmBackend {
public:
    virtual ~IpcHbmBackend() = default;

    virtual Status Export(void *localVa, size_t size, int32_t deviceIdx, IpcExportHandle &out) = 0;
    virtual Status AllowImportPid(const IpcExportHandle &handle, pid_t peerPid) = 0;
    virtual Status Import(const IpcExportHandle &handle, int32_t deviceIdx, void **localVa, size_t *size) = 0;
    virtual Status Close(void *localVa) = 0;
};

}  // namespace hbm_ipc
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_DEVICE_HBM_IPC_IPC_HBM_BACKEND_H
