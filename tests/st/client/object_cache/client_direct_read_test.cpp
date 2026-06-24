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

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "datasystem/client/object_cache/direct_read/direct_read_flow.h"
#include "datasystem/client/object_cache/direct_read/direct_read_test_hook.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/object_cache/read_access/object_read_meta_access_flow.h"
#include "datasystem/protos/hash_ring.pb.h"
#include "datasystem/utils/service_discovery.h"
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

void WaitForInjectCount(const char *injectName, int expectedCount = 1, uint64_t timeoutMs = 15000)
{
    Timer timer;
    while (timer.ElapsedMilliSecond() <= timeoutMs) {
        if (datasystem::inject::GetExecuteCount(injectName) >= static_cast<uint64_t>(expectedCount)) {
            datasystem::inject::Clear(injectName);
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    FAIL() << "Timed out waiting for inject point " << injectName;
}

void ResetDirectReadStatsWithForceDirectRead()
{
    object_cache::DirectReadTestHook::Reset();
    object_cache::DirectReadTestHook::SetForceDirectRead(true);
}

bool WaitHashRingReady(const std::string &etcdAddrs, int expectedWorkers, int timeoutSec = 60,
                       bool requireEmptyMigration = true)
{
    EtcdStore etcd(etcdAddrs);
    if (etcd.Init().IsError()) {
        return false;
    }
    Timer timer;
    while (timer.ElapsedSecond() < timeoutSec) {
        RangeSearchResult res;
        if (etcd.Get(ETCD_RING_PREFIX, "", res).IsOk()) {
            HashRingPb ring;
            if (ring.ParseFromString(res.value) && ring.workers_size() == expectedWorkers
                && (!requireEmptyMigration || (ring.add_node_info().empty() && ring.del_node_info().empty()))) {
                bool allActive = true;
                for (const auto &worker : ring.workers()) {
                    if (worker.second.state() != WorkerPb::ACTIVE) {
                        allActive = false;
                        break;
                    }
                }
                if (allActive) {
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    return true;
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}

bool WaitHashRingStable(const std::string &etcdAddrs, int expectedWorkers, int timeoutSec = 60)
{
    return WaitHashRingReady(etcdAddrs, expectedWorkers, timeoutSec, true);
}

bool TryGetDirectReadObject(const std::shared_ptr<ObjectClient> &client, const std::string &objectKey,
                            std::vector<Optional<Buffer>> &buffers, int timeoutSec = 60)
{
    Timer timer;
    while (timer.ElapsedSecond() < timeoutSec) {
        buffers.clear();
        if (client->Get({ objectKey }, 0, buffers).IsOk() && buffers.size() == 1ul && buffers[0]
            && buffers[0]->GetSize() == static_cast<uint64_t>(TEST_DATA_SIZE)) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return false;
}

struct CutbackRecoveryResult {
    bool observed = false;
    std::vector<Optional<Buffer>> buffers;
    object_cache::DirectReadStats stats;
};

CutbackRecoveryResult WaitForGatewayCutbackRecovery(const std::shared_ptr<ObjectClient> &client,
                                                    const std::string &objectKey, int timeoutSec = 60)
{
    CutbackRecoveryResult result;
    uint64_t seenCutbackAttempts = 0;
    Timer timer;
    while (timer.ElapsedSecond() < timeoutSec) {
        const auto statsBefore = object_cache::DirectReadTestHook::Snapshot();
        seenCutbackAttempts = std::max(seenCutbackAttempts, statsBefore.cutbackAttemptCount);
        result.buffers.clear();
        const Status rc = client->Get({ objectKey }, 0, result.buffers);
        const auto statsAfter = object_cache::DirectReadTestHook::Snapshot();
        seenCutbackAttempts = std::max(seenCutbackAttempts, statsAfter.cutbackAttemptCount);
        const uint64_t directAttemptsThisGet =
            statsAfter.directAttemptCount >= statsBefore.directAttemptCount
                ? statsAfter.directAttemptCount - statsBefore.directAttemptCount
                : 0ul;
        if (rc.IsOk() && result.buffers.size() == 1ul && result.buffers[0]
            && result.buffers[0]->GetSize() == static_cast<uint64_t>(TEST_DATA_SIZE) && seenCutbackAttempts >= 1ul
            && directAttemptsThisGet == 0ul) {
            result.observed = true;
            result.stats = statsAfter;
            result.stats.cutbackAttemptCount = seenCutbackAttempts;
            break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    return result;
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
        object_cache::ObjectReadMetaAccessFlow::ResetTestCounters();
        FLAGS_enable_distributed_master = false;
        HostPort workerAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
        FLAGS_master_address = workerAddress.ToString();
    }

    void TearDown() override
    {
        object_cache::DirectReadTestHook::Reset();
        object_cache::ObjectReadMetaAccessFlow::ResetTestCounters();
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
    EXPECT_GE(object_cache::ObjectReadMetaAccessFlow::MetaPhaseCountForTest(), 1ul);
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
        object_cache::ObjectReadMetaAccessFlow::ResetTestCounters();
        FLAGS_enable_distributed_master = false;
        HostPort workerAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
        FLAGS_master_address = workerAddress.ToString();
    }

    void TearDown() override
    {
        object_cache::DirectReadTestHook::Reset();
        object_cache::ObjectReadMetaAccessFlow::ResetTestCounters();
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
        datasystem::inject::Set("HashRing.SubmitScaleUpTask.skip", "return(1)");
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        object_cache::DirectReadTestHook::Reset();
        object_cache::ObjectReadMetaAccessFlow::ResetTestCounters();
        FLAGS_enable_distributed_master = true;
        HostPort workerAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
        FLAGS_master_address = workerAddress.ToString();
    }

    void TearDown() override
    {
        object_cache::DirectReadTestHook::Reset();
        object_cache::ObjectReadMetaAccessFlow::ResetTestCounters();
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
    EXPECT_GE(stats.hashRingWorkerRefreshCount + stats.hashRingEtcdRefreshCount, 1ul);
    EXPECT_EQ(stats.routeQueryCount, 1ul);
    EXPECT_EQ(stats.metaQueryCount, 1ul);
}

TEST_F(ClientDirectReadHashRingTest, SteadyStateRepeatedGetsDoNotRefreshRingPerLookup)
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
    auto bootstrapStats = object_cache::DirectReadTestHook::Snapshot();
    const uint64_t refreshAfterBootstrap =
        bootstrapStats.hashRingWorkerRefreshCount + bootstrapStats.hashRingEtcdRefreshCount;

    object_cache::DirectReadTestHook::Reset();
    object_cache::DirectReadTestHook::SetForceDirectRead(true);
    constexpr int kRepeatedGets = 5;
    for (int i = 0; i < kRepeatedGets; ++i) {
        buffers.clear();
        DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));
    }

    auto steadyStats = object_cache::DirectReadTestHook::Snapshot();
    EXPECT_EQ(steadyStats.hashRingWorkerRefreshCount + steadyStats.hashRingEtcdRefreshCount, 0ul);
    EXPECT_GE(steadyStats.directAttemptCount, static_cast<uint64_t>(kRepeatedGets));
    EXPECT_GE(steadyStats.routeQueryCount, static_cast<uint64_t>(kRepeatedGets));
    (void)refreshAfterBootstrap;
}

class ClientDirectReadRecoveryTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
        opts.masterIdx = 1;
        opts.workerGflagParams =
            " -node_timeout_s=1 -heartbeat_interval_ms=500 -node_dead_timeout_s=2 -client_reconnect_wait_s=1"
            " -auto_del_dead_node=true";

        std::string hostIp = "127.0.0.1";
        for (size_t i = 0; i < opts.numWorkers; i++) {
            HostPort hostPort(hostIp, GetFreePort());
            opts.workerConfigs.emplace_back(hostPort);
            std::string envName = "direct_read_host_id_env" + std::to_string(i);
            std::string envVal = "direct_read_host_id" + std::to_string(i);
            ASSERT_EQ(setenv(envName.c_str(), envVal.c_str(), 1), 0);
            opts.workerSpecifyGflagParams[i] = FormatString("-host_id_env_name=%s", envName);
        }

        datasystem::inject::Set("ListenWorker.CheckHeartbeat.interval", "call(500)");
        datasystem::inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(500)");
        datasystem::inject::Set("ClientWorkerCommonApi.SendHeartbeat.timeoutMs", "call(500)");
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        object_cache::DirectReadTestHook::Reset();
        object_cache::ObjectReadMetaAccessFlow::ResetTestCounters();
        FLAGS_enable_distributed_master = false;
        HostPort workerAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
        FLAGS_master_address = workerAddress.ToString();
        DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.PreShutDown.skip", "return(K_OK)"));
        DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "worker.PreShutDown.skip", "return(K_OK)"));
    }

    void TearDown() override
    {
        object_cache::DirectReadTestHook::Reset();
        object_cache::ObjectReadMetaAccessFlow::ResetTestCounters();
        FLAGS_enable_client_direct_read = false;
        FLAGS_enable_client_direct_read_fallback = true;
        ExternalClusterTest::TearDown();
    }

    void InitAffinityClient(std::shared_ptr<ObjectClient> &client)
    {
        ServiceDiscoveryOptions sdOpts;
        sdOpts.etcdAddress = cluster_->GetEtcdAddrs();
        sdOpts.hostIdEnvName = "direct_read_host_id_env0";
        sdOpts.affinityPolicy = ServiceAffinityPolicy::PREFERRED_SAME_NODE;
        auto serviceDiscovery = std::make_shared<ServiceDiscovery>(sdOpts);
        DS_ASSERT_OK(serviceDiscovery->Init());

        HostPort workerAddr;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddr));
        ConnectOptions connectOptions;
        connectOptions.host = workerAddr.Host();
        connectOptions.port = workerAddr.Port();
        connectOptions.connectTimeoutMs = 60000;
        connectOptions.accessKey = "QTWAOYTTINDUT2QVKYUC";
        connectOptions.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
        connectOptions.enableCrossNodeConnection = true;
        connectOptions.serviceDiscovery = serviceDiscovery;

        client = std::make_shared<ObjectClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }
};

TEST_F(ClientDirectReadRecoveryTest, StandbyWithoutLocalWorkerUsesDirectRead)
{
    FLAGS_enable_client_direct_read = true;
    FLAGS_enable_client_direct_read_fallback = true;

    std::shared_ptr<ObjectClient> writer;
    InitTestClient(1, writer);
    auto objectKey = ObjectKey();
    auto payload = BuildPayload();
    DS_ASSERT_OK(writer->Put(objectKey, reinterpret_cast<uint8_t *>(payload.data()), payload.size(), CreateParam{}));

    std::shared_ptr<ObjectClient> client;
    InitAffinityClient(client);

    datasystem::inject::Set("ObjClient.ShutDown", "return(K_OK)");
    datasystem::inject::Set("client.switch_worker_end", "call()");
    DS_ASSERT_OK(cluster_->QuicklyShutdownWorker(0));
    WaitForInjectCount("client.switch_worker_end");
    std::this_thread::sleep_for(std::chrono::seconds(2));

    object_cache::DirectReadTestHook::Reset();
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));

    auto stats = object_cache::DirectReadTestHook::Snapshot();
    EXPECT_GE(stats.directAttemptCount, 1ul);
    EXPECT_GE(stats.metaQueryCount, 1ul);
    EXPECT_TRUE(stats.dataQueryCount >= 1ul || stats.pathFallbackCount >= 1ul);

    ASSERT_EQ(buffers.size(), 1ul);
    ASSERT_TRUE(buffers[0]);
    buffers[0]->RLatch();
    AssertBufferEqual(*buffers[0], payload);
    buffers[0]->UnRLatch();
}

TEST_F(ClientDirectReadRecoveryTest, LocalWorkerRecoveryCutbackToGateway)
{
    FLAGS_enable_client_direct_read = true;
    FLAGS_enable_client_direct_read_fallback = true;

    std::shared_ptr<ObjectClient> writer;
    InitTestClient(1, writer);
    auto objectKey = ObjectKey();
    auto payload = BuildPayload();
    DS_ASSERT_OK(writer->Put(objectKey, reinterpret_cast<uint8_t *>(payload.data()), payload.size(), CreateParam{}));

    std::shared_ptr<ObjectClient> client;
    InitAffinityClient(client);

    datasystem::inject::Set("ObjClient.ShutDown", "return(K_OK)");
    datasystem::inject::Set("client.switch_worker_end", "call()");
    DS_ASSERT_OK(cluster_->QuicklyShutdownWorker(0));
    WaitForInjectCount("client.switch_worker_end");
    std::this_thread::sleep_for(std::chrono::seconds(2));

    object_cache::DirectReadTestHook::Reset();
    {
        std::vector<Optional<Buffer>> standbyBuffers;
        DS_ASSERT_OK(client->Get({ objectKey }, 0, standbyBuffers));
        auto standbyStats = object_cache::DirectReadTestHook::Snapshot();
        EXPECT_GE(standbyStats.directAttemptCount, 1ul);
        ASSERT_EQ(standbyBuffers.size(), 1ul);
        ASSERT_TRUE(standbyBuffers[0]);
        standbyBuffers[0]->RLatch();
        AssertBufferEqual(*standbyBuffers[0], payload);
        standbyBuffers[0]->UnRLatch();
    }

    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 0, "-client_reconnect_wait_s=1"));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 0));

    auto cutbackResult = WaitForGatewayCutbackRecovery(client, objectKey, 30);
    ASSERT_TRUE(cutbackResult.observed);
    EXPECT_GE(cutbackResult.stats.cutbackAttemptCount, 1ul);

    ASSERT_EQ(cutbackResult.buffers.size(), 1ul);
    ASSERT_TRUE(cutbackResult.buffers[0]);
    Buffer &recoveredBuffer = *cutbackResult.buffers[0];
    recoveredBuffer.RLatch();
    AssertBufferEqual(recoveredBuffer, payload);
    recoveredBuffer.UnRLatch();
}

TEST_F(ClientDirectReadRecoveryTest, RemoteOnlyClientNeverAttemptsCutback)
{
    FLAGS_enable_client_direct_read = true;
    FLAGS_enable_client_direct_read_fallback = true;

    std::shared_ptr<ObjectClient> writer;
    InitTestClient(1, writer);
    auto objectKey = ObjectKey();
    auto payload = BuildPayload();
    DS_ASSERT_OK(writer->Put(objectKey, reinterpret_cast<uint8_t *>(payload.data()), payload.size(), CreateParam{}));

    std::shared_ptr<ObjectClient> client;
    InitTestClient(1, client);

    object_cache::DirectReadTestHook::Reset();
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));

    auto stats = object_cache::DirectReadTestHook::Snapshot();
    EXPECT_EQ(stats.cutbackAttemptCount, 0ul);
    EXPECT_EQ(stats.directAttemptCount, 0ul);

    ASSERT_EQ(buffers.size(), 1ul);
    ASSERT_TRUE(buffers[0]);
    buffers[0]->RLatch();
    AssertBufferEqual(*buffers[0], payload);
    buffers[0]->UnRLatch();
}

class ClientDirectReadDistributedRecoveryTest : public ClientDirectReadRecoveryTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        ClientDirectReadRecoveryTest::SetClusterSetupOptions(opts);
        opts.enableDistributedMaster = "true";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        object_cache::DirectReadTestHook::Reset();
        object_cache::ObjectReadMetaAccessFlow::ResetTestCounters();
        FLAGS_enable_distributed_master = true;
        HostPort workerAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
        FLAGS_master_address = workerAddress.ToString();
        DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.PreShutDown.skip", "return(K_OK)"));
        DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "worker.PreShutDown.skip", "return(K_OK)"));
    }
};

TEST_F(ClientDirectReadDistributedRecoveryTest, LocalWorkerRecoveryCutbackWithDistributedRing)
{
    FLAGS_enable_client_direct_read = true;
    FLAGS_enable_client_direct_read_fallback = true;

    std::shared_ptr<ObjectClient> writer;
    InitTestClient(1, writer);
    auto objectKey = ObjectKey();
    auto payload = BuildPayload();
    DS_ASSERT_OK(writer->Put(objectKey, reinterpret_cast<uint8_t *>(payload.data()), payload.size(), CreateParam{}));

    std::shared_ptr<ObjectClient> client;
    InitAffinityClient(client);

    datasystem::inject::Set("ObjClient.ShutDown", "return(K_OK)");
    datasystem::inject::Set("client.switch_worker_end", "call()");
    DS_ASSERT_OK(cluster_->QuicklyShutdownWorker(0));
    WaitForInjectCount("client.switch_worker_end");
    std::this_thread::sleep_for(std::chrono::seconds(2));

    object_cache::DirectReadTestHook::Reset();
    {
        std::vector<Optional<Buffer>> standbyBuffers;
        DS_ASSERT_OK(client->Get({ objectKey }, 0, standbyBuffers));
        auto standbyStats = object_cache::DirectReadTestHook::Snapshot();
        EXPECT_GE(standbyStats.directAttemptCount, 1ul);
        ASSERT_EQ(standbyBuffers.size(), 1ul);
        ASSERT_TRUE(standbyBuffers[0]);
    }

    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 0, "-client_reconnect_wait_s=1"));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 0));
    std::this_thread::sleep_for(std::chrono::seconds(3));

    auto cutbackResult = WaitForGatewayCutbackRecovery(client, objectKey, 90);
    ASSERT_TRUE(cutbackResult.observed);
    EXPECT_GE(cutbackResult.stats.cutbackAttemptCount, 1ul);

    ASSERT_EQ(cutbackResult.buffers.size(), 1ul);
    ASSERT_TRUE(cutbackResult.buffers[0]);
    Buffer &recoveredBuffer = *cutbackResult.buffers[0];
    recoveredBuffer.RLatch();
    AssertBufferEqual(recoveredBuffer, payload);
    recoveredBuffer.UnRLatch();
}

TEST_F(ClientDirectReadHashRingTest, ColocatedInlineDataSkipsRemoteDataRpc)
{
    FLAGS_enable_client_direct_read = true;
    FLAGS_enable_client_direct_read_fallback = true;
    object_cache::DirectReadTestHook::SetForceDirectRead(true);
    object_cache::DirectReadTestHook::SetPreferRemoteDataGet(false);

    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    auto objectKey = ObjectKey();
    auto payload = BuildPayload();
    DS_ASSERT_OK(client->Put(objectKey, reinterpret_cast<uint8_t *>(payload.data()), payload.size(), CreateParam{}));

    object_cache::DirectReadTestHook::Reset();
    object_cache::DirectReadTestHook::SetForceDirectRead(true);
    object_cache::DirectReadTestHook::SetPreferRemoteDataGet(false);

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));

    auto stats = object_cache::DirectReadTestHook::Snapshot();
    EXPECT_GE(stats.directAttemptCount, 1ul);
    EXPECT_GE(stats.metaQueryCount, 1ul);
    EXPECT_GE(stats.inlineDataHitCount, 1ul);
    EXPECT_EQ(stats.dataQueryCount, 0ul);
    EXPECT_EQ(stats.pathFallbackCount, 0ul);

    ASSERT_EQ(buffers.size(), 1ul);
    ASSERT_TRUE(buffers[0]);
    buffers[0]->RLatch();
    AssertBufferEqual(*buffers[0], payload);
    buffers[0]->UnRLatch();
}

TEST_F(ClientDirectReadHashRingTest, RemoteDataFallbackWhenInlineBypassed)
{
    FLAGS_enable_client_direct_read = true;
    FLAGS_enable_client_direct_read_fallback = true;
    object_cache::DirectReadTestHook::SetForceDirectRead(true);
    object_cache::DirectReadTestHook::SetPreferRemoteDataGet(true);

    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    auto objectKey = ObjectKey();
    auto payload = BuildPayload();
    DS_ASSERT_OK(client->Put(objectKey, reinterpret_cast<uint8_t *>(payload.data()), payload.size(), CreateParam{}));

    object_cache::DirectReadTestHook::Reset();
    object_cache::DirectReadTestHook::SetForceDirectRead(true);
    object_cache::DirectReadTestHook::SetPreferRemoteDataGet(true);

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));

    auto stats = object_cache::DirectReadTestHook::Snapshot();
    EXPECT_GE(stats.dataQueryCount, 1ul);
    EXPECT_EQ(stats.inlineDataHitCount, 0ul);

    ASSERT_EQ(buffers.size(), 1ul);
    ASSERT_TRUE(buffers[0]);
    buffers[0]->RLatch();
    AssertBufferEqual(*buffers[0], payload);
    buffers[0]->UnRLatch();
}

class ClientDirectReadHashRingScaleTest : public ClientDirectReadHashRingTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        ClientDirectReadHashRingTest::SetClusterSetupOptions(opts);
        opts.numWorkers = 3;
        (void)datasystem::inject::Clear("HashRing.SubmitScaleUpTask.skip");
    }
};

TEST_F(ClientDirectReadHashRingScaleTest, ReadSurvivesWorkerScaleDownAndUp)
{
    FLAGS_enable_client_direct_read = true;
    FLAGS_enable_client_direct_read_fallback = true;
    object_cache::DirectReadTestHook::SetForceDirectRead(true);
    std::this_thread::sleep_for(std::chrono::seconds(3));

    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    auto objectKey = ObjectKey();
    auto payload = BuildPayload();
    DS_ASSERT_OK(client->Put(objectKey, reinterpret_cast<uint8_t *>(payload.data()), payload.size(), CreateParam{}));

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));
    ASSERT_TRUE(buffers[0]);
    buffers[0]->RLatch();
    AssertBufferEqual(*buffers[0], payload);
    buffers[0]->UnRLatch();

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "SubmitScaleDownTask.skip", "return()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "SubmitScaleDownTask.skip", "return()"));
    DS_ASSERT_OK(cluster_->KillWorker(2));
    std::this_thread::sleep_for(std::chrono::seconds(8));

    ResetDirectReadStatsWithForceDirectRead();
    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));
    auto statsScaledDown = object_cache::DirectReadTestHook::Snapshot();
    EXPECT_GE(statsScaledDown.directAttemptCount, 1ul);
    EXPECT_GE(statsScaledDown.metaQueryCount, 1ul);
    EXPECT_GE(statsScaledDown.hashRingWorkerRefreshCount + statsScaledDown.hashRingEtcdRefreshCount, 1ul);
    EXPECT_GE(statsScaledDown.routeQueryCount, 1ul);

    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 2, "-client_reconnect_wait_s=1"));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 2));
    std::this_thread::sleep_for(std::chrono::seconds(8));

    ResetDirectReadStatsWithForceDirectRead();
    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));
    auto statsScaledUp = object_cache::DirectReadTestHook::Snapshot();
    EXPECT_GE(statsScaledUp.directAttemptCount, 1ul);
    EXPECT_GE(statsScaledUp.metaQueryCount, 1ul);
    EXPECT_GE(statsScaledUp.hashRingWorkerRefreshCount + statsScaledUp.hashRingEtcdRefreshCount, 1ul);

    ASSERT_TRUE(buffers[0]);
    buffers[0]->RLatch();
    AssertBufferEqual(*buffers[0], payload);
    buffers[0]->UnRLatch();
}
}  // namespace st
}  // namespace datasystem
