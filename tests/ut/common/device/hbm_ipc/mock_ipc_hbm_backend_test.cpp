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
 * Description: MockIpcHbmBackend unit tests.
 */
#include "datasystem/common/device/hbm_ipc/mock_ipc_hbm_backend.h"

#include <cstring>
#include <vector>

#include <gtest/gtest.h>
#include <unistd.h>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace hbm_ipc {
namespace {

TEST(MockIpcHbmBackendTest, ExportImportSamePointer)
{
    MockIpcHbmBackend backend;
    std::vector<char> buf(4096, 'x');
    void *src = buf.data();

    IpcExportHandle handle;
    ASSERT_TRUE(backend.Export(src, buf.size(), 0, handle).IsOk());
    ASSERT_TRUE(backend.AllowImportPid(handle, getpid()).IsOk());

    void *imported = nullptr;
    size_t importedSize = 0;
    ASSERT_TRUE(backend.Import(handle, 0, &imported, &importedSize).IsOk());
    EXPECT_EQ(imported, src);
    EXPECT_EQ(importedSize, buf.size());

    std::memset(imported, 'y', 16);
    EXPECT_EQ(buf[0], 'y');
}

TEST(MockIpcHbmBackendTest, CloseRemovesExport)
{
    MockIpcHbmBackend backend;
    std::vector<char> buf(512, 0);
    IpcExportHandle handle;
    ASSERT_TRUE(backend.Export(buf.data(), buf.size(), 0, handle).IsOk());
    ASSERT_TRUE(backend.Close(buf.data()).IsOk());

    void *imported = nullptr;
    size_t importedSize = 0;
    EXPECT_FALSE(backend.Import(handle, 0, &imported, &importedSize).IsOk());
}

TEST(MockIpcHbmBackendTest, ReExportSameVaReturnsSameHandle)
{
    MockIpcHbmBackend backend;
    std::vector<char> buf(1024, 0);
    IpcExportHandle h1;
    IpcExportHandle h2;
    ASSERT_TRUE(backend.Export(buf.data(), buf.size(), 0, h1).IsOk());
    ASSERT_TRUE(backend.Export(buf.data(), buf.size(), 0, h2).IsOk());
    EXPECT_EQ(h1.key, h2.key);
}

}  // namespace
}  // namespace hbm_ipc
}  // namespace datasystem
