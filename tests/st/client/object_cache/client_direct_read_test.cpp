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
 * Description: Tests that client direct read starts behind a safe feature gate.
 */
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/client/object_cache/direct_read/direct_read_test_hook.h"
#include "datasystem/common/object_cache/read_access/object_read_access_flow.h"
#include "datasystem/common/flags/flags.h"
#include "oc_client_common.h"

DS_DECLARE_bool(enable_client_direct_read);
DS_DECLARE_bool(enable_client_direct_read_fallback);

namespace datasystem {
namespace st {
namespace {
constexpr int WORKER_NUM = 1;
constexpr int64_t TEST_DATA_SIZE = 256;

std::string BuildPayload()
{
    return RandomData().GetRandomString(TEST_DATA_SIZE);
}
}  // namespace

class ClientDirectReadTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = WORKER_NUM;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        object_cache::DirectReadTestHook::Reset();
        object_cache::ObjectReadAccessFlow::ResetTestCounters();
    }

    void TearDown() override
    {
        object_cache::DirectReadTestHook::Reset();
        object_cache::ObjectReadAccessFlow::ResetTestCounters();
        FLAGS_enable_client_direct_read = false;
        FLAGS_enable_client_direct_read_fallback = true;
        ExternalClusterTest::TearDown();
    }

protected:
    void PutAndGetOnClient(const std::shared_ptr<ObjectClient> &client)
    {
        auto objectKey = ObjectKey();
        auto payload = BuildPayload();
        DS_ASSERT_OK(client->Put(objectKey, reinterpret_cast<uint8_t *>(payload.data()), payload.size(), CreateParam{}));

        std::vector<Optional<Buffer>> buffers;
        DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));
        ASSERT_EQ(buffers.size(), 1ul);
        ASSERT_TRUE(buffers[0]);
        ASSERT_EQ(buffers[0]->GetSize(), payload.size());
        buffers[0]->RLatch();
        AssertBufferEqual(*buffers[0], payload);
        buffers[0]->UnRLatch();
    }
};

TEST_F(ClientDirectReadTest, DefaultDisabledUsesWorkerPath)
{
    FLAGS_enable_client_direct_read = false;
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    PutAndGetOnClient(client);

    auto stats = object_cache::DirectReadTestHook::Snapshot();
    EXPECT_EQ(stats.directAttemptCount, 0ul);
    EXPECT_EQ(stats.routeQueryCount, 0ul);
    EXPECT_EQ(stats.metaQueryCount, 0ul);
    EXPECT_EQ(stats.dataQueryCount, 0ul);
    EXPECT_EQ(stats.pathFallbackCount, 0ul);
    EXPECT_TRUE(stats.lastFallbackReason.empty());
}

TEST_F(ClientDirectReadTest, SameNodeUsesWorkerPathWhenEnabled)
{
    FLAGS_enable_client_direct_read = true;
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    PutAndGetOnClient(client);

    auto stats = object_cache::DirectReadTestHook::Snapshot();
    EXPECT_EQ(stats.directAttemptCount, 0ul);
    EXPECT_EQ(stats.routeQueryCount, 0ul);
    EXPECT_EQ(stats.metaQueryCount, 0ul);
    EXPECT_EQ(stats.dataQueryCount, 0ul);
    EXPECT_EQ(stats.pathFallbackCount, 0ul);
    EXPECT_TRUE(stats.lastFallbackReason.empty());
}

TEST_F(ClientDirectReadTest, DirectUnsupportedFallsBackOnce)
{
    FLAGS_enable_client_direct_read = true;
    FLAGS_enable_client_direct_read_fallback = true;
    object_cache::DirectReadTestHook::SetForceDirectRead(true);
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    PutAndGetOnClient(client);

    auto stats = object_cache::DirectReadTestHook::Snapshot();
    EXPECT_EQ(stats.directAttemptCount, 1ul);
    EXPECT_EQ(stats.routeQueryCount, 1ul);
    EXPECT_EQ(stats.metaQueryCount, 1ul);
    EXPECT_EQ(stats.dataQueryCount, 0ul);
    EXPECT_EQ(stats.pathFallbackCount, 1ul);
    EXPECT_EQ(stats.lastFallbackReason, "direct_flow_not_implemented");
    EXPECT_GE(object_cache::ObjectReadAccessFlow::MetaPhaseCountForTest(), 1ul);
}

TEST_F(ClientDirectReadTest, DirectQueriesMetaBeforeFallback)
{
    FLAGS_enable_client_direct_read = true;
    FLAGS_enable_client_direct_read_fallback = true;
    object_cache::DirectReadTestHook::SetForceDirectRead(true);
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    PutAndGetOnClient(client);

    auto stats = object_cache::DirectReadTestHook::Snapshot();
    EXPECT_EQ(stats.directAttemptCount, 1ul);
    EXPECT_EQ(stats.routeQueryCount, 1ul);
    EXPECT_EQ(stats.metaQueryCount, 1ul);
    EXPECT_EQ(stats.dataQueryCount, 0ul);
    EXPECT_EQ(stats.pathFallbackCount, 1ul);
    EXPECT_EQ(stats.lastFallbackReason, "direct_flow_not_implemented");
    EXPECT_GE(object_cache::ObjectReadAccessFlow::MetaPhaseCountForTest(), 1ul);
}

class ClientDirectReadHashRingTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = WORKER_NUM;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "true";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        object_cache::DirectReadTestHook::Reset();
        object_cache::ObjectReadAccessFlow::ResetTestCounters();
    }

    void TearDown() override
    {
        object_cache::DirectReadTestHook::Reset();
        object_cache::ObjectReadAccessFlow::ResetTestCounters();
        FLAGS_enable_client_direct_read = false;
        FLAGS_enable_client_direct_read_fallback = true;
        ExternalClusterTest::TearDown();
    }
};

TEST_F(ClientDirectReadHashRingTest, BootstrapLoadsHashRingFromEtcd)
{
    FLAGS_enable_client_direct_read = true;
    FLAGS_enable_client_direct_read_fallback = true;
    object_cache::DirectReadTestHook::SetForceDirectRead(true);
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    auto objectKey = ObjectKey();
    auto payload = BuildPayload();
    DS_ASSERT_OK(client->Put(objectKey, reinterpret_cast<uint8_t *>(payload.data()), payload.size(), CreateParam{}));

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));

    auto stats = object_cache::DirectReadTestHook::Snapshot();
    EXPECT_GE(stats.hashRingEtcdRefreshCount, 1ul);
    EXPECT_EQ(stats.routeQueryCount, 1ul);
    EXPECT_EQ(stats.metaQueryCount, 1ul);
}
}  // namespace st
}  // namespace datasystem
