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
 * Description: NDS AlignmentGate unit tests.
 */
#include "datasystem/common/device/nds/alignment_gate.h"

#include <gtest/gtest.h>

namespace datasystem {
namespace nds {
namespace {

TEST(AlignmentGateTest, Default4kRejects512AlignedOnly)
{
    EXPECT_TRUE(AlignmentGatePass(4096, 4096, 0x1000, 4096));
    EXPECT_FALSE(AlignmentGatePass(512, 512, 0x200, 4096));
    EXPECT_TRUE(AlignmentGatePass(512, 512, 0x200, 512));
}

TEST(AlignmentGateTest, OffByOneFails)
{
    EXPECT_FALSE(AlignmentGatePass(4097, 4096, 0x1000, 4096));
    EXPECT_FALSE(AlignmentGatePass(4096, 4097, 0x1000, 4096));
    EXPECT_FALSE(AlignmentGatePass(4096, 4096, 0x1001, 4096));
}

TEST(AlignmentGateTest, ZeroLengthOrAlignRejected)
{
    EXPECT_FALSE(AlignmentGatePass(0, 4096, 0x1000, 4096));
    EXPECT_FALSE(AlignmentGatePass(4096, 0, 0x1000, 4096));
    EXPECT_FALSE(AlignmentGatePass(4096, 4096, 0x1000, 0));
}

}  // namespace
}  // namespace nds
}  // namespace datasystem
