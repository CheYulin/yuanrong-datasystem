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

#include "ut/common.h"
#include <string>
#include <vector>

#include "datasystem/common/object_cache/read_access/object_read_data_access.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/protos/master_object.pb.h"

namespace datasystem {
namespace ut {
namespace {
class FakeRemoteClient : public object_cache::IObjectReadRemoteDataClient {
public:
    Status FetchRemote(const master::QueryMetaInfoPb &queryMeta, const object_cache::ObjectReadSpec &spec,
                       GetObjectRemoteRspPb &rsp, std::vector<RpcMessage> &payloads) override
    {
        ++fetchCount;
        (void)queryMeta;
        (void)spec;
        rsp.set_data_size(4);
        payloads.emplace_back();
        DS_ASSERT_OK(payloads.back().ZeroCopyBuffer("data", 4));
        return Status::OK();
    }

    int fetchCount = 0;
};

master::QueryMetaInfoPb BuildInlineMeta(uint32_t payloadIndex)
{
    master::QueryMetaInfoPb queryMeta;
    queryMeta.mutable_meta()->set_object_key("inline-key");
    queryMeta.add_payload_indexs(payloadIndex);
    return queryMeta;
}
}  // namespace

TEST(ObjectReadDataAccessTest, PlanObjectReadDataPathInlineWhenPayloadIndexesPresent)
{
    auto queryMeta = BuildInlineMeta(0);
    EXPECT_EQ(object_cache::PlanObjectReadDataPath(queryMeta), object_cache::ObjectReadDataPath::kInlineFromMeta);
}

TEST(ObjectReadDataAccessTest, PlanObjectReadDataPathRemoteWhenNoInlinePayload)
{
    master::QueryMetaInfoPb queryMeta;
    queryMeta.mutable_meta()->set_object_key("remote-key");
    queryMeta.set_address("127.0.0.1:9101");
    EXPECT_EQ(object_cache::PlanObjectReadDataPath(queryMeta), object_cache::ObjectReadDataPath::kRemote);
}

TEST(ObjectReadDataAccessTest, ExtractInlinePayloadsMovesMetaPoolBytes)
{
    auto queryMeta = BuildInlineMeta(0);
    std::vector<RpcMessage> metaPool;
    metaPool.emplace_back();
    char data[] = "abcd";
    DS_ASSERT_OK(metaPool.back().ZeroCopyBuffer(data, 4));

    std::vector<RpcMessage> out;
    object_cache::ObjectReadL0Outcome outcome = object_cache::ObjectReadL0Outcome::kRemote;
    DS_ASSERT_OK(object_cache::ExtractInlinePayloads(queryMeta, metaPool, out, &outcome));
    EXPECT_EQ(outcome, object_cache::ObjectReadL0Outcome::kInlineHit);
    ASSERT_EQ(out.size(), 1ul);
    EXPECT_EQ(out[0].Size(), 4ul);
}

TEST(ObjectReadDataAccessTest, FetchObjectReadDataFallsBackToRemoteWhenInlineNotOffered)
{
    master::QueryMetaInfoPb queryMeta;
    queryMeta.mutable_meta()->set_object_key("remote-key");
    queryMeta.set_address("127.0.0.1:9101");

    FakeRemoteClient remote;
    std::vector<RpcMessage> metaPool;
    std::vector<RpcMessage> out;
    object_cache::ObjectReadSpec spec;
    object_cache::ObjectReadL0Outcome outcome = object_cache::ObjectReadL0Outcome::kInlineHit;

    DS_ASSERT_OK(object_cache::FetchObjectReadData(queryMeta, spec, metaPool, remote, out, nullptr, &outcome));
    EXPECT_EQ(outcome, object_cache::ObjectReadL0Outcome::kRemote);
    EXPECT_EQ(remote.fetchCount, 1);
    ASSERT_EQ(out.size(), 1ul);
}

TEST(ObjectReadDataAccessTest, FetchObjectReadDataUsesInlineWithoutRemoteRpc)
{
    auto queryMeta = BuildInlineMeta(0);
    std::vector<RpcMessage> metaPool;
    metaPool.emplace_back();
    char data[] = "inline";
    DS_ASSERT_OK(metaPool.back().ZeroCopyBuffer(data, 6));

    FakeRemoteClient remote;
    std::vector<RpcMessage> out;
    object_cache::ObjectReadL0Outcome outcome = object_cache::ObjectReadL0Outcome::kRemote;
    DS_ASSERT_OK(object_cache::FetchObjectReadData(queryMeta, {}, metaPool, remote, out, nullptr, &outcome));
    EXPECT_EQ(outcome, object_cache::ObjectReadL0Outcome::kInlineHit);
    EXPECT_EQ(remote.fetchCount, 0);
    ASSERT_EQ(out.size(), 1ul);
}
}  // namespace ut
}  // namespace datasystem
