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

#include <gtest/gtest.h>

#include <string>

#include "ut/common.h"
#include "datasystem/common/object_cache/read_only_hash_ring_view.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/hash_ring.pb.h"

namespace datasystem {
namespace ut {
namespace {
HashRingPb BuildSingleWorkerRing(const std::string &workerAddr, uint32_t token)
{
    HashRingPb ring;
    ring.set_cluster_has_init(true);
    auto &worker = (*ring.mutable_workers())[workerAddr];
    worker.set_state(WorkerPb::ACTIVE);
    worker.set_worker_uuid("worker-uuid-1");
    worker.add_hash_tokens(token);
    return ring;
}

HashRingPb BuildUuidKeyedWorkerRing(const std::string &workerHostPort, const std::string &workerUuid, uint32_t token)
{
    HashRingPb ring;
    ring.set_cluster_has_init(true);
    const std::string workerKey = workerHostPort + ";" + workerUuid;
    auto &worker = (*ring.mutable_workers())[workerKey];
    worker.set_state(WorkerPb::ACTIVE);
    worker.set_worker_uuid(workerUuid);
    worker.add_hash_tokens(token);
    return ring;
}
}  // namespace

TEST(ReadOnlyHashRingViewTest, ResolvesMetaAddressFromHashToken)
{
    const std::string workerAddr = "127.0.0.1:9000";
    object_cache::ReadOnlyHashRingView view;
    DS_ASSERT_OK(view.UpdateFromPb(BuildSingleWorkerRing(workerAddr, 100), 1));

    HostPort metaAddress;
    DS_ASSERT_OK(view.GetMetaAddress("a_key_hash_to_99", metaAddress));
    EXPECT_EQ(metaAddress.ToString(), workerAddr);
}
TEST(ReadOnlyHashRingViewTest, DetectsHealthyWorkerAddress)
{
    const std::string workerAddr = "127.0.0.1:9001";
    object_cache::ReadOnlyHashRingView view;
    DS_ASSERT_OK(view.UpdateFromPb(BuildSingleWorkerRing(workerAddr, 100), 1));

    HostPort workerAddress;
    DS_ASSERT_OK(workerAddress.ParseString(workerAddr));
    EXPECT_TRUE(view.HasHealthyWorkerAtAddress(workerAddress));

    HostPort missingAddress;
    DS_ASSERT_OK(missingAddress.ParseString("127.0.0.1:9002"));
    EXPECT_FALSE(view.HasHealthyWorkerAtAddress(missingAddress));
}

TEST(ReadOnlyHashRingViewTest, DetectsHealthyWorkerAddressWithUuidKeyedRing)
{
    const std::string workerHostPort = "127.0.0.1:9003";
    object_cache::ReadOnlyHashRingView view;
    DS_ASSERT_OK(view.UpdateFromPb(BuildUuidKeyedWorkerRing(workerHostPort, "worker-uuid-2", 100), 2));

    HostPort workerAddress;
    DS_ASSERT_OK(workerAddress.ParseString(workerHostPort));
    EXPECT_TRUE(view.HasHealthyWorkerAtAddress(workerAddress));
    EXPECT_TRUE(view.HasJoinableWorkerAtAddress(workerAddress));
}

TEST(ReadOnlyHashRingViewTest, DetectsHealthyWorkerWhenRingKeyUsesHostPortUuidFormat)
{
    const std::string workerHostPort = "127.0.0.1:9004";
    const std::string workerUuid = "1782318777481036487";
    object_cache::ReadOnlyHashRingView view;
    HashRingPb ring;
    ring.set_cluster_has_init(true);
    const std::string workerKey = workerHostPort + ";" + workerUuid;
    auto &worker = (*ring.mutable_workers())[workerKey];
    worker.set_state(WorkerPb::ACTIVE);
    worker.set_worker_uuid(workerUuid);
    worker.add_hash_tokens(100);
    DS_ASSERT_OK(view.UpdateFromPb(ring, 3));

    HostPort workerAddress;
    DS_ASSERT_OK(workerAddress.ParseString(workerHostPort));
    EXPECT_TRUE(view.HasHealthyWorkerAtAddress(workerAddress));
}

TEST(ReadOnlyHashRingViewTest, RejectsUnknownVersionWorkerSnapshotWhenEtcdVersionKnown)
{
    const std::string workerAddr = "127.0.0.1:9000";
    object_cache::ReadOnlyHashRingView view;
    DS_ASSERT_OK(view.UpdateFromPb(BuildSingleWorkerRing(workerAddr, 100), 5));
    EXPECT_EQ(view.Version(), 5);

    bool versionChanged = true;
    const std::string staleWorkerAddr = "127.0.0.1:9009";
    DS_ASSERT_OK(view.UpdateFromPb(BuildSingleWorkerRing(staleWorkerAddr, 200), -1, &versionChanged));
    EXPECT_FALSE(versionChanged);
    EXPECT_EQ(view.Version(), 5);

    HostPort metaAddress;
    DS_ASSERT_OK(view.GetMetaAddress("a_key_hash_to_99", metaAddress));
    EXPECT_EQ(metaAddress.ToString(), workerAddr);
}

TEST(ReadOnlyHashRingViewTest, RejectsStaleWorkerSnapshotWithLowerRevision)
{
    const std::string workerAddr = "127.0.0.1:9000";
    object_cache::ReadOnlyHashRingView view;
    DS_ASSERT_OK(view.UpdateFromPb(BuildSingleWorkerRing(workerAddr, 100), 5));

    bool versionChanged = true;
    const std::string staleWorkerAddr = "127.0.0.1:9009";
    DS_ASSERT_OK(view.UpdateFromPb(BuildSingleWorkerRing(staleWorkerAddr, 200), 3, &versionChanged));
    EXPECT_FALSE(versionChanged);
    EXPECT_EQ(view.Version(), 5);
}
}  // namespace ut
}  // namespace datasystem
