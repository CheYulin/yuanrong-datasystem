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
 * Description: Fake NDS spill reader — pread spill file then host memcpy to imported VA.
 */
#ifndef DATASYSTEM_COMMON_DEVICE_NDS_FAKE_NDS_SPILL_READER_H
#define DATASYSTEM_COMMON_DEVICE_NDS_FAKE_NDS_SPILL_READER_H

#include "datasystem/common/device/nds/nds_spill_reader.h"

namespace datasystem {
namespace nds {

class FakeNdsSpillReader : public NdsSpillReader {
public:
    Status ReadToHbm(const SpillFileLoc &loc, uint64_t readOff, uint64_t readSize, void *importedVa, uint64_t destOff,
                     int32_t deviceIdx) override;
};

}  // namespace nds
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_DEVICE_NDS_FAKE_NDS_SPILL_READER_H
