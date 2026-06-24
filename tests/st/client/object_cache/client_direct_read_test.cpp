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

#include "datasystem/client/object_cache/direct_read/direct_read_flow.h"
#include "datasystem/client/object_cache/direct_read/direct_read_test_hook.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/object_cache/read_access/object_read_access_flow.h"
#include "oc_client_common.h"

DS_DECLARE_bool(enable_client_direct_read);
DS_DECLARE_bool(enable_client_direct_read_fallback);
DS_DECLARE_bool(enable_distributed_master);
DS_DECLARE_int32(client_direct_read_retry_count);
DS_DECLARE_string(master_address);

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
        FLAGS_enable_distributed_master = false;
        HostPort workerAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
        FLAGS_master_address = workerAddress.ToString();
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

TEST_F(ClientDirectReadTest, CrossNodeDirectTcpGetMatchesGatewayGet)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    auto objectKey = ObjectKey();
    auto payload = BuildPayload();
    DS_ASSERT_OK(client->Put(objectKey, reinterpret_cast<uint8_t *>(payload.data()), payload.size(), CreateParam{}));

    FLAGS_enable_client_direct_read = false;
    object_cache::DirectReadTestHook::Reset();
    std::vector<Optional<Buffer>> gatewayBuffers;
    DS_ASSERT_OK(client->Get({ objectKey }, 0, gatewayBuffers));
    ASSERT_EQ(gatewayBuffers.size(), 1ul);
    ASSERT_TRUE(gatewayBuffers[0]);
    gatewayBuffers[0]->RLatch();
    AssertBufferEqual(*gatewayBuffers[0], payload);

    FLAGS_enable_client_direct_read = true;
    object_cache::DirectReadTestHook::SetForceDirectRead(true);
    std::vector<Optional<Buffer>> directBuffers;
    DS_ASSERT_OK(client->Get({ objectKey }, 0, directBuffers));

    auto stats = object_cache::DirectReadTestHook::Snapshot();
    EXPECT_EQ(stats.directAttemptCount, 1ul);
    EXPECT_EQ(stats.routeQueryCount, 1ul);
    EXPECT_EQ(stats.metaQueryCount, 1ul);
    EXPECT_EQ(stats.dataQueryCount, 1ul);
    EXPECT_EQ(stats.pathFallbackCount, 0ul);
    EXPECT_TRUE(stats.lastFallbackReason.empty());

    ASSERT_EQ(directBuffers.size(), 1ul);
    ASSERT_TRUE(directBuffers[0]);
    ASSERT_EQ(directBuffers[0]->GetSize(), payload.size());
    directBuffers[0]->RLatch();
    AssertBufferEqual(*directBuffers[0], payload);
    gatewayBuffers[0]->UnRLatch();
    directBuffers[0]->UnRLatch();
}

TEST_F(ClientDirectReadTest, DataWorkerUnavailableFallsBack)
{
    FLAGS_enable_client_direct_read = true;
    FLAGS_enable_client_direct_read_fallback = true;
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.worker_worker_remote_get_failure",
                                          "100*return()"));

    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    object_cache::DirectReadTestHook::SetForceDirectRead(true);
    object_cache::DirectReadTestHook::SetPreferRemoteDataGet(true);

    auto objectKey = ObjectKey();
    auto payload = BuildPayload();
    DS_ASSERT_OK(client->Put(objectKey, reinterpret_cast<uint8_t *>(payload.data()), payload.size(), CreateParam{}));

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));

    auto stats = object_cache::DirectReadTestHook::Snapshot();
    EXPECT_EQ(stats.directAttemptCount, 1ul);
    EXPECT_EQ(stats.dataQueryCount, 1ul);
    EXPECT_EQ(stats.pathFallbackCount, 1ul);
    EXPECT_NE(stats.lastFallbackReason.find(object_cache::DirectReadFlow::kDataWorkerUnavailableFallbackReason),
              std::string::npos);

    ASSERT_EQ(buffers.size(), 1ul);
    ASSERT_TRUE(buffers[0]);
    ASSERT_EQ(buffers[0]->GetSize(), payload.size());
    buffers[0]->RLatch();
    AssertBufferEqual(*buffers[0], payload);
    buffers[0]->UnRLatch();
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
    EXPECT_EQ(stats.dataQueryCount, 1ul);
    EXPECT_EQ(stats.pathFallbackCount, 0ul);
    EXPECT_TRUE(stats.lastFallbackReason.empty());
    EXPECT_GE(object_cache::ObjectReadAccessFlow::MetaPhaseCountForTest(), 1ul);
}

TEST_F(ClientDirectReadTest, StaleRouteRecordsFallbackReason)
{
    FLAGS_enable_client_direct_read = true;
    FLAGS_enable_client_direct_read_fallback = true;
    FLAGS_client_direct_read_retry_count = 1;

    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    object_cache::DirectReadTestHook::SetForceDirectRead(true);
    object_cache::DirectReadTestHook::SetSimulateStaleRoute(true);

    auto objectKey = ObjectKey();
    auto payload = BuildPayload();
    DS_ASSERT_OK(client->Put(objectKey, reinterpret_cast<uint8_t *>(payload.data()), payload.size(), CreateParam{}));

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));

    auto stats = object_cache::DirectReadTestHook::Snapshot();
    EXPECT_EQ(stats.pathFallbackCount, 1ul);
    EXPECT_GE(stats.staleRouteRetryCount, 1ul);
    EXPECT_NE(stats.lastFallbackReason.find(object_cache::DirectReadFlow::kStaleRouteFallbackReason), std::string::npos);
    ASSERT_EQ(buffers.size(), 1ul);
    ASSERT_TRUE(buffers[0]);
}

TEST_F(ClientDirectReadTest, RedirectLoopFallsBackOnce)
{
    FLAGS_enable_client_direct_read = true;
    FLAGS_enable_client_direct_read_fallback = true;
    FLAGS_client_direct_read_retry_count = 1;

    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    object_cache::DirectReadTestHook::SetForceDirectRead(true);
    object_cache::DirectReadTestHook::SetSimulateRedirectLoop(true);

    auto objectKey = ObjectKey();
    auto payload = BuildPayload();
    DS_ASSERT_OK(client->Put(objectKey, reinterpret_cast<uint8_t *>(payload.data()), payload.size(), CreateParam{}));

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));

    auto stats = object_cache::DirectReadTestHook::Snapshot();
    EXPECT_EQ(stats.pathFallbackCount, 1ul);
    EXPECT_GE(stats.redirectRetryCount, 1ul);
    EXPECT_NE(stats.lastFallbackReason.find(object_cache::DirectReadFlow::kRedirectLoopFallbackReason), std::string::npos);
    ASSERT_EQ(buffers.size(), 1ul);
    ASSERT_TRUE(buffers[0]);
}

TEST_F(ClientDirectReadTest, MetaTimeoutFallsBackOnce)
{
    FLAGS_enable_client_direct_read = true;
    FLAGS_enable_client_direct_read_fallback = true;
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "master.slow_query_meta", "100*sleep(3000)"));

    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client, 60000, 500);
    object_cache::DirectReadTestHook::SetForceDirectRead(true);

    auto objectKey = ObjectKey();
    auto payload = BuildPayload();
    DS_ASSERT_OK(client->Put(objectKey, reinterpret_cast<uint8_t *>(payload.data()), payload.size(), CreateParam{}));

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));

    auto stats = object_cache::DirectReadTestHook::Snapshot();
    EXPECT_EQ(stats.pathFallbackCount, 1ul);
    EXPECT_NE(stats.lastFallbackReason.find(object_cache::DirectReadFlow::kMetaTimeoutFallbackReason), std::string::npos);
    ASSERT_EQ(buffers.size(), 1ul);
    ASSERT_TRUE(buffers[0]);
}

TEST_F(ClientDirectReadTest, FailedKeysPreservedAfterFallback)
{
    FLAGS_enable_client_direct_read = true;
    FLAGS_enable_client_direct_read_fallback = true;

    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    object_cache::DirectReadTestHook::SetForceDirectRead(true);
    object_cache::DirectReadTestHook::SetSimulateStaleRoute(true);

    auto existingKey = ObjectKey();
    auto missingKey = ObjectKey();
    auto payload = BuildPayload();
    DS_ASSERT_OK(
        client->Put(existingKey, reinterpret_cast<uint8_t *>(payload.data()), payload.size(), CreateParam{}));

    FLAGS_enable_client_direct_read = false;
    object_cache::DirectReadTestHook::Reset();
    std::vector<Optional<Buffer>> gatewayBuffers;
    DS_ASSERT_OK(client->Get({ existingKey, missingKey }, 0, gatewayBuffers));

    FLAGS_enable_client_direct_read = true;
    object_cache::DirectReadTestHook::SetForceDirectRead(true);
    object_cache::DirectReadTestHook::SetSimulateStaleRoute(true);
    std::vector<Optional<Buffer>> directBuffers;
    DS_ASSERT_OK(client->Get({ existingKey, missingKey }, 0, directBuffers));

    ASSERT_EQ(gatewayBuffers.size(), 2ul);
    ASSERT_EQ(directBuffers.size(), 2ul);
    EXPECT_EQ(static_cast<bool>(gatewayBuffers[0]), static_cast<bool>(directBuffers[0]));
    EXPECT_EQ(static_cast<bool>(gatewayBuffers[1]), static_cast<bool>(directBuffers[1]));
    if (gatewayBuffers[0] && directBuffers[0]) {
        gatewayBuffers[0]->RLatch();
        directBuffers[0]->RLatch();
        AssertBufferEqual(*gatewayBuffers[0], payload);
        AssertBufferEqual(*directBuffers[0], payload);
        gatewayBuffers[0]->UnRLatch();
        directBuffers[0]->UnRLatch();
    }

    auto stats = object_cache::DirectReadTestHook::Snapshot();
    EXPECT_EQ(stats.pathFallbackCount, 1ul);
    EXPECT_NE(stats.lastFallbackReason.find(object_cache::DirectReadFlow::kStaleRouteFallbackReason), std::string::npos);
}

TEST_F(ClientDirectReadTest, SameNodeGetDoesNotUseDirectRead)
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
}

TEST_F(ClientDirectReadTest, SameNodeWriteDoesNotUseDirectRead)
{
    FLAGS_enable_client_direct_read = true;
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    auto objectKey = ObjectKey();
    auto payload = BuildPayload();
    DS_ASSERT_OK(client->Put(objectKey, reinterpret_cast<uint8_t *>(payload.data()), payload.size(), CreateParam{}));

    auto stats = object_cache::DirectReadTestHook::Snapshot();
    EXPECT_EQ(stats.directAttemptCount, 0ul);
    EXPECT_EQ(stats.routeQueryCount, 0ul);
    EXPECT_EQ(stats.metaQueryCount, 0ul);
    EXPECT_EQ(stats.dataQueryCount, 0ul);
}

class ClientDirectReadCrossNodeTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        object_cache::DirectReadTestHook::Reset();
        object_cache::ObjectReadAccessFlow::ResetTestCounters();
        FLAGS_enable_distributed_master = false;
        HostPort workerAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
        FLAGS_master_address = workerAddress.ToString();
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

TEST_F(ClientDirectReadCrossNodeTest, CrossNodeGetWithLocalWorkerUsesGatewayPath)
{
    FLAGS_enable_client_direct_read = true;
    FLAGS_enable_client_direct_read_fallback = true;
    object_cache::DirectReadTestHook::Reset();

    std::shared_ptr<ObjectClient> writer;
    InitTestClient(0, writer);
    auto objectKey = ObjectKey();
    auto payload = BuildPayload();
    DS_ASSERT_OK(writer->Put(objectKey, reinterpret_cast<uint8_t *>(payload.data()), payload.size(), CreateParam{}));

    std::shared_ptr<ObjectClient> reader;
    InitTestClient(1, reader);
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(reader->Get({ objectKey }, 0, buffers));

    auto stats = object_cache::DirectReadTestHook::Snapshot();
    EXPECT_EQ(stats.directAttemptCount, 0ul);
    EXPECT_EQ(stats.routeQueryCount, 0ul);
    EXPECT_EQ(stats.metaQueryCount, 0ul);
    EXPECT_EQ(stats.dataQueryCount, 0ul);
    EXPECT_EQ(stats.pathFallbackCount, 0ul);

    ASSERT_EQ(buffers.size(), 1ul);
    ASSERT_TRUE(buffers[0]);
    buffers[0]->RLatch();
    AssertBufferEqual(*buffers[0], payload);
    buffers[0]->UnRLatch();
}

TEST_F(ClientDirectReadCrossNodeTest, CrossNodeWriteDoesNotUseDirectRead)
{
    FLAGS_enable_client_direct_read = true;
    std::shared_ptr<ObjectClient> writer;
    InitTestClient(0, writer);

    auto objectKey = ObjectKey();
    auto payload = BuildPayload();
    DS_ASSERT_OK(writer->Put(objectKey, reinterpret_cast<uint8_t *>(payload.data()), payload.size(), CreateParam{}));

    auto stats = object_cache::DirectReadTestHook::Snapshot();
    EXPECT_EQ(stats.directAttemptCount, 0ul);
    EXPECT_EQ(stats.routeQueryCount, 0ul);
    EXPECT_EQ(stats.metaQueryCount, 0ul);
    EXPECT_EQ(stats.dataQueryCount, 0ul);
}

TEST_F(ClientDirectReadCrossNodeTest, CrossNodeWriteVisibleToDirectRead)
{
    std::shared_ptr<ObjectClient> writer;
    InitTestClient(0, writer);
    std::shared_ptr<ObjectClient> reader;
    InitTestClient(1, reader);

    auto objectKey = ObjectKey();
    auto payload = BuildPayload();
    DS_ASSERT_OK(writer->Put(objectKey, reinterpret_cast<uint8_t *>(payload.data()), payload.size(), CreateParam{}));

    FLAGS_enable_client_direct_read = false;
    object_cache::DirectReadTestHook::Reset();
    std::vector<Optional<Buffer>> gatewayBuffers;
    DS_ASSERT_OK(reader->Get({ objectKey }, 0, gatewayBuffers));
    ASSERT_EQ(gatewayBuffers.size(), 1ul);
    ASSERT_TRUE(gatewayBuffers[0]);
    gatewayBuffers[0]->RLatch();
    AssertBufferEqual(*gatewayBuffers[0], payload);

    FLAGS_enable_client_direct_read = true;
    object_cache::DirectReadTestHook::SetForceDirectRead(true);
    std::vector<Optional<Buffer>> directBuffers;
    DS_ASSERT_OK(reader->Get({ objectKey }, 0, directBuffers));

    auto stats = object_cache::DirectReadTestHook::Snapshot();
    EXPECT_EQ(stats.directAttemptCount, 1ul);
    EXPECT_EQ(stats.pathFallbackCount, 0ul);

    ASSERT_EQ(directBuffers.size(), 1ul);
    ASSERT_TRUE(directBuffers[0]);
    directBuffers[0]->RLatch();
    AssertBufferEqual(*directBuffers[0], payload);
    gatewayBuffers[0]->UnRLatch();
    directBuffers[0]->UnRLatch();
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
        FLAGS_enable_distributed_master = true;
        HostPort workerAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
        FLAGS_master_address = workerAddress.ToString();
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

TEST_F(ClientDirectReadHashRingTest, MetaMovingRefreshesRingAndSucceeds)
{
    FLAGS_enable_client_direct_read = true;
    FLAGS_enable_client_direct_read_fallback = true;
    FLAGS_client_direct_read_retry_count = 1;
    object_cache::DirectReadTestHook::SetForceDirectRead(true);
    object_cache::DirectReadTestHook::SetSimulateMetaMovingResponses(1);

    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    auto objectKey = ObjectKey();
    auto payload = BuildPayload();
    DS_ASSERT_OK(client->Put(objectKey, reinterpret_cast<uint8_t *>(payload.data()), payload.size(), CreateParam{}));

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));

    auto stats = object_cache::DirectReadTestHook::Snapshot();
    EXPECT_GE(stats.movingRetryCount, 1ul);
    EXPECT_GE(stats.hashRingWorkerRefreshCount + stats.hashRingEtcdRefreshCount, 1ul);
    EXPECT_EQ(stats.pathFallbackCount, 0ul);
    EXPECT_EQ(stats.metaQueryCount, 2ul);

    ASSERT_EQ(buffers.size(), 1ul);
    ASSERT_TRUE(buffers[0]);
    buffers[0]->RLatch();
    AssertBufferEqual(*buffers[0], payload);
    buffers[0]->UnRLatch();
}

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
