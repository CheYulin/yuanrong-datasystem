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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/common/object_cache/read_access/object_read_access_flow.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/master_object.pb.h"

namespace datasystem {
namespace ut {
namespace {
class FakeRouteProvider : public object_cache::IObjectReadRouteProvider {
public:
    Status GetMetaAddress(const std::string &objectKey, HostPort &metaAddress) override
    {
        ++routeQueryCount;
        (void)objectKey;
        RETURN_IF_NOT_OK(metaAddress.ParseString("127.0.0.1:9100"));
        return Status::OK();
    }

    Status RefreshRouteIfNeeded() override
    {
        ++refreshCount;
        return Status::OK();
    }

    int routeQueryCount = 0;
    int refreshCount = 0;
};

class FakeMetaClient : public object_cache::IObjectReadMetaClient {
public:
    Status QueryMeta(const HostPort &metaAddress, const std::vector<std::string> &objectKeys, int64_t subTimeoutMs,
                     master::QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads) override
    {
        ++metaQueryCount;
        (void)metaAddress;
        (void)subTimeoutMs;
        (void)payloads;
        for (const auto &key : objectKeys) {
            auto *meta = rsp.add_query_metas();
            meta->mutable_meta()->set_object_key(key);
        }
        return Status::OK();
    }

    int metaQueryCount = 0;
};

class FakeDataClient : public object_cache::IObjectReadDataClient {
public:
    Status ReadData(const master::QueryMetaInfoPb &queryMeta, int64_t subTimeoutMs, size_t objectIndex,
                    GetObjectRemoteRspPb &rsp, std::vector<RpcMessage> &payloads) override
    {
        (void)queryMeta;
        (void)subTimeoutMs;
        (void)objectIndex;
        (void)rsp;
        (void)payloads;
        return Status(K_NOT_SUPPORTED, "fake");
    }
};
}  // namespace

TEST(ObjectReadAccessFlowTest, ExecuteMetaPhaseGroupsByRouteAndQueriesMeta)
{
    object_cache::ObjectReadAccessFlow::ResetTestCounters();
    auto routeProvider = std::make_shared<FakeRouteProvider>();
    auto metaClient = std::make_shared<FakeMetaClient>();
    auto dataClient = std::make_shared<FakeDataClient>();
    object_cache::ObjectReadAccessFlow flow(routeProvider, metaClient, dataClient);

    object_cache::ObjectReadAccessRequest request;
    request.objectKeys = { "key-a", "key-b" };
    request.subTimeoutMs = 1000;

    object_cache::ObjectReadAccessMetaResult result;
    DS_ASSERT_OK(flow.ExecuteMetaPhase(request, result));

    EXPECT_EQ(routeProvider->refreshCount, 1);
    EXPECT_EQ(routeProvider->routeQueryCount, 2);
    EXPECT_EQ(metaClient->metaQueryCount, 1);
    EXPECT_EQ(result.metaRsp.query_metas_size(), 2);
    EXPECT_GE(object_cache::ObjectReadAccessFlow::MetaPhaseCountForTest(), 1ul);
}

TEST(ObjectReadAccessFlowTest, MetaMovingReturnsTryAgain)
{
    class MovingMetaClient : public FakeMetaClient {
    public:
        Status QueryMeta(const HostPort &metaAddress, const std::vector<std::string> &objectKeys, int64_t subTimeoutMs,
                         master::QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads) override
        {
            (void)metaAddress;
            (void)objectKeys;
            (void)subTimeoutMs;
            (void)payloads;
            rsp.set_meta_is_moving(true);
            return Status::OK();
        }
    };

    auto routeProvider = std::make_shared<FakeRouteProvider>();
    auto metaClient = std::make_shared<MovingMetaClient>();
    auto dataClient = std::make_shared<FakeDataClient>();
    object_cache::ObjectReadAccessFlow flow(routeProvider, metaClient, dataClient);

    object_cache::ObjectReadAccessRequest request;
    request.objectKeys = { "moving-key" };
    object_cache::ObjectReadAccessMetaResult result;
    auto rc = flow.ExecuteMetaPhase(request, result);
    EXPECT_EQ(rc.GetCode(), K_TRY_AGAIN);
}
}  // namespace ut
}  // namespace datasystem
