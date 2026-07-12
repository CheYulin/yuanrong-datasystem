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
 * Description: Fake NDS spill reader.
 */
#include "datasystem/common/device/nds/fake_nds_spill_reader.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <string>
#include <vector>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace nds {

Status FakeNdsSpillReader::ReadToHbm(const SpillFileLoc &loc, uint64_t readOff, uint64_t readSize, void *importedVa,
                                     uint64_t destOff, int32_t /*deviceIdx*/)
{
    (void)loc.size;
    CHECK_FAIL_RETURN_STATUS(!loc.path.empty(), StatusCode::K_INVALID, "empty spill path");
    CHECK_FAIL_RETURN_STATUS(importedVa != nullptr, StatusCode::K_INVALID, "importedVa is null");
    CHECK_FAIL_RETURN_STATUS(readSize > 0, StatusCode::K_INVALID, "readSize is zero");

    const int fd = ::open(loc.path.c_str(), O_RDONLY | O_CLOEXEC);
    CHECK_FAIL_RETURN_STATUS(fd >= 0, StatusCode::K_IO_ERROR, "open spill file failed");

    const off_t fileOff = static_cast<off_t>(loc.offset + readOff);
    std::vector<char> staging(readSize);
    const ssize_t n = ::pread(fd, staging.data(), readSize, fileOff);
    const int savedErrno = errno;
    ::close(fd);
    CHECK_FAIL_RETURN_STATUS(n == static_cast<ssize_t>(readSize), StatusCode::K_IO_ERROR,
                             "pread spill file failed errno=" + std::to_string(savedErrno));

    auto *dest = static_cast<char *>(importedVa) + destOff;
    std::memcpy(dest, staging.data(), readSize);
    return Status::OK();
}

}  // namespace nds
}  // namespace datasystem
