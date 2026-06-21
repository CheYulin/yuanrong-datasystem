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
}  // namespace ut
}  // namespace datasystem
