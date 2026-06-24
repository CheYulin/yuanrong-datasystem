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

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "ut/common.h"
#include "datasystem/common/object_cache/read_access/query_meta_orchestrating_meta_client.h"
#include "datasystem/common/object_cache/read_access/query_meta_transport.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/master_object.pb.h"

namespace datasystem {
namespace ut {
namespace {
class MockQueryMetaTransport : public object_cache::IQueryMetaTransport {
public:
    std::atomic<int32_t> queryCount{ 0 };

    Status QueryMetaOnce(const HostPort &, const std::vector<std::string> &, int64_t, bool enableRedirect,
                         master::QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads) override
    {
        ++queryCount;
        if (enableRedirect && queryCount.load() == 1) {
            rsp.set_meta_is_moving(true);
            rsp.add_info();
            return Status::OK();
        }
        rsp.Clear();
        rsp.add_query_metas()->mutable_meta()->set_object_key("obj");
        payloads.emplace_back();
        return Status::OK();
    }
};

TEST(QueryMetaOrchestratingMetaClientTest, RetriesMetaMovingInsideOrchestrator)
{
    auto transport = std::make_shared<MockQueryMetaTransport>();
    object_cache::QueryMetaOrchestratingMetaClient::Options options;
    options.moving.maxMovingRetries = 2;
    options.moving.movingRetryExceededStatus = Status(K_TRY_AGAIN, "meta_is_moving");

    object_cache::QueryMetaOrchestratingMetaClient client(transport, options);
    HostPort metaAddress;
    ASSERT_TRUE(metaAddress.ParseString("127.0.0.1:9100").IsOk());

    master::QueryMetaRspPb rsp;
    std::vector<RpcMessage> payloads;
    ASSERT_TRUE(client.QueryMeta(metaAddress, { "obj" }, 1000, rsp, payloads).IsOk());

    EXPECT_GE(transport->queryCount.load(), 2);
    ASSERT_EQ(rsp.query_metas_size(), 1);
    EXPECT_EQ(rsp.query_metas(0).meta().object_key(), "obj");
}
}  // namespace
}  // namespace ut
}  // namespace datasystem
