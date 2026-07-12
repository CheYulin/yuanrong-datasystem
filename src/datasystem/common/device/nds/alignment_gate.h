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
 * Description: NDS SSD→HBM direct-path alignment gate (Phase-1 default 4KiB).
 */
#ifndef DATASYSTEM_COMMON_DEVICE_NDS_ALIGNMENT_GATE_H
#define DATASYSTEM_COMMON_DEVICE_NDS_ALIGNMENT_GATE_H

#include <cstdint>

namespace datasystem {
namespace nds {

constexpr uint32_t kDefaultNdsAlignBytes = 4096;

// Returns true when file offset, length, and HBM VA are all aligned to alignBytes.
bool AlignmentGatePass(uint64_t fileOff, uint64_t len, uintptr_t hbmAddr, uint32_t alignBytes = kDefaultNdsAlignBytes);

}  // namespace nds
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_DEVICE_NDS_ALIGNMENT_GATE_H
