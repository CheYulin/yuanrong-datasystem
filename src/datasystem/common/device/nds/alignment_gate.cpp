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
 * Description: NDS SSD→HBM direct-path alignment gate.
 */
#include "datasystem/common/device/nds/alignment_gate.h"

namespace datasystem {
namespace nds {

bool AlignmentGatePass(uint64_t fileOff, uint64_t len, uintptr_t hbmAddr, uint32_t alignBytes)
{
    if (alignBytes == 0 || len == 0) {
        return false;
    }
    return (fileOff % alignBytes) == 0 && (len % alignBytes) == 0 && (hbmAddr % alignBytes) == 0;
}

}  // namespace nds
}  // namespace datasystem
