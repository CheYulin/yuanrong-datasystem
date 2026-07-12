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
 * Description: FakeNdsSpillReader unit tests.
 */
#include "datasystem/common/device/nds/fake_nds_spill_reader.h"

#include <fcntl.h>
#include <unistd.h>

#include <cstring>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace nds {
namespace {

class FakeNdsSpillReaderTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        char tmpl[] = "/tmp/nds_fake_spill_XXXXXX";
        fd_ = ::mkstemp(tmpl);
        ASSERT_GE(fd_, 0);
        path_ = tmpl;
    }

    void TearDown() override
    {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
        if (!path_.empty()) {
            ::unlink(path_.c_str());
        }
    }

    void WritePattern(size_t off, const std::string &data)
    {
        ASSERT_EQ(::pwrite(fd_, data.data(), data.size(), static_cast<off_t>(off)), static_cast<ssize_t>(data.size()));
    }

    int fd_ = -1;
    std::string path_;
};

TEST_F(FakeNdsSpillReaderTest, ReadToHbmCopiesFileBytes)
{
    const std::string payload(4096, 'z');
    WritePattern(0, payload);

    std::vector<char> dest(8192, 0);
    FakeNdsSpillReader reader;
    SpillFileLoc loc;
    loc.path = path_;
    loc.offset = 0;
    loc.size = payload.size();

    ASSERT_TRUE(reader.ReadToHbm(loc, 0, payload.size(), dest.data(), 0, 0).IsOk());
    EXPECT_EQ(std::memcmp(dest.data(), payload.data(), payload.size()), 0);
}

TEST_F(FakeNdsSpillReaderTest, ReadWithFileOffsetAndDestOff)
{
    const std::string head(512, 'a');
    const std::string tail(512, 'b');
    WritePattern(0, head);
    WritePattern(512, tail);

    std::vector<char> dest(2048, 0);
    FakeNdsSpillReader reader;
    SpillFileLoc loc;
    loc.path = path_;
    loc.offset = 512;
    loc.size = tail.size();

    ASSERT_TRUE(reader.ReadToHbm(loc, 0, tail.size(), dest.data(), 256, 0).IsOk());
    EXPECT_EQ(std::memcmp(dest.data() + 256, tail.data(), tail.size()), 0);
}

}  // namespace
}  // namespace nds
}  // namespace datasystem
