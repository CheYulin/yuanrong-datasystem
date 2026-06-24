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

#include "datasystem/common/object_cache/read_access/query_meta_merge_helper.h"
#include "datasystem/common/object_cache/read_access/query_meta_redirect_helper.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/master_object.pb.h"

namespace datasystem {
namespace ut {
namespace {
TEST(QueryMetaMergeHelperTest, MergeQueryMetaResponsesCopiesAllFields)
{
    master::QueryMetaRspPb dest;
    dest.add_query_metas()->mutable_meta()->set_object_key("existing");

    master::QueryMetaRspPb src;
    src.add_query_metas()->mutable_meta()->set_object_key("new");
    *src.add_not_exist_ids() = "missing";
    src.add_deleting_versions(42);
    *src.add_not_exist_ids_is_deleting() = "deleting";
    src.set_meta_is_moving(true);

    object_cache::MergeQueryMetaResponses(dest, src);

    ASSERT_EQ(dest.query_metas_size(), 2);
    EXPECT_EQ(dest.query_metas(1).meta().object_key(), "new");
    ASSERT_EQ(dest.not_exist_ids_size(), 1);
    EXPECT_EQ(dest.not_exist_ids(0), "missing");
    ASSERT_EQ(dest.deleting_versions_size(), 1);
    EXPECT_EQ(dest.deleting_versions(0), 42);
    ASSERT_EQ(dest.not_exist_ids_is_deleting_size(), 1);
    EXPECT_EQ(dest.not_exist_ids_is_deleting(0), "deleting");
    EXPECT_TRUE(dest.meta_is_moving());
}

TEST(QueryMetaMergeHelperTest, AppendQueryMetaPayloadsOffsetsIndexes)
{
    master::QueryMetaRspPb rsp;
    auto *meta = rsp.add_query_metas();
    meta->add_payload_indexs(0);
    meta->add_payload_indexs(1);

    std::vector<RpcMessage> basePayloads(2);
    std::vector<RpcMessage> newPayloads(1);
    ASSERT_TRUE(object_cache::AppendQueryMetaPayloads(basePayloads, rsp, newPayloads).IsOk());
    ASSERT_EQ(basePayloads.size(), 3U);
    EXPECT_EQ(meta->payload_indexs(0), 2);
    EXPECT_EQ(meta->payload_indexs(1), 3);
}

TEST(QueryMetaRedirectHelperTest, RetryWhileMetaIsMovingHonorsRetryBudget)
{
    master::QueryMetaRspPb rsp;
    rsp.set_meta_is_moving(true);
    rsp.add_info();

    int32_t attempts = 0;
    object_cache::QueryMetaMovingRetryOptions options;
    options.maxMovingRetries = 1;
    options.movingRetryExceededStatus = Status(K_TRY_AGAIN, "meta_is_moving");

    const Status status = object_cache::RetryWhileMetaIsMoving(
        rsp,
        [&]() {
            ++attempts;
            rsp.set_meta_is_moving(true);
            rsp.add_info();
            return Status::OK();
        },
        [&]() { rsp.Clear(); }, options);

    EXPECT_EQ(status.GetCode(), K_TRY_AGAIN);
    EXPECT_EQ(attempts, 1);
}

TEST(QueryMetaRedirectHelperTest, FollowQueryMetaRedirectsMergesRedirectResponse)
{
    master::QueryMetaRspPb rsp;
    auto *info = rsp.add_info();
    info->set_redirect_meta_address("127.0.0.1:9200");
    *info->add_change_meta_ids() = "obj1";

    int32_t redirectQueries = 0;
    object_cache::QueryMetaAtMasterFn queryMeta = [&](const HostPort &metaAddress, const std::vector<std::string> &objectKeys,
                                        bool enableRedirect, master::QueryMetaRspPb &queryRsp,
                                        std::vector<RpcMessage> &payloads) -> Status {
        ++redirectQueries;
        EXPECT_EQ(metaAddress.ToString(), "127.0.0.1:9200");
        EXPECT_EQ(objectKeys.size(), 1U);
        EXPECT_EQ(objectKeys[0], "obj1");
        EXPECT_FALSE(enableRedirect);
        auto *redirectMeta = queryRsp.add_query_metas();
        redirectMeta->mutable_meta()->set_object_key("obj1");
        redirectMeta->add_payload_indexs(0);
        payloads.emplace_back();
        return Status::OK();
    };

    std::vector<RpcMessage> payloads;
    auto *primaryMeta = rsp.add_query_metas();
    primaryMeta->mutable_meta()->set_object_key("existing");
    primaryMeta->add_payload_indexs(0);
    payloads.emplace_back();

    ASSERT_TRUE(object_cache::FollowQueryMetaRedirects(queryMeta, {}, rsp, payloads).IsOk());
    EXPECT_EQ(redirectQueries, 1);
    ASSERT_EQ(rsp.query_metas_size(), 2);
    EXPECT_EQ(rsp.query_metas(0).meta().object_key(), "existing");
    EXPECT_EQ(rsp.query_metas(0).payload_indexs(0), 0U);
    EXPECT_EQ(rsp.query_metas(1).meta().object_key(), "obj1");
    EXPECT_EQ(rsp.query_metas(1).payload_indexs(0), 1U);
    EXPECT_EQ(rsp.info_size(), 0);
    EXPECT_EQ(payloads.size(), 2U);
}

TEST(QueryMetaRedirectHelperTest, QueryMetaWithRedirectAndMovingRunsPrimaryThenRedirect)
{
    master::QueryMetaRspPb rsp;
    std::vector<RpcMessage> payloads;

    int32_t primaryQueries = 0;
    int32_t redirectQueries = 0;
    object_cache::QueryMetaAtMasterFn queryMeta = [&](const HostPort &metaAddress, const std::vector<std::string> &,
                                        bool enableRedirect, master::QueryMetaRspPb &queryRsp,
                                        std::vector<RpcMessage> &queryPayloads) -> Status {
        if (enableRedirect) {
            ++primaryQueries;
            EXPECT_EQ(metaAddress.ToString(), "127.0.0.1:9100");
            auto *primaryMeta = queryRsp.add_query_metas();
            primaryMeta->mutable_meta()->set_object_key("primary");
            primaryMeta->add_payload_indexs(0);
            auto *info = queryRsp.add_info();
            info->set_redirect_meta_address("127.0.0.1:9200");
            *info->add_change_meta_ids() = "redirected";
            queryPayloads.emplace_back();
            return Status::OK();
        }
        ++redirectQueries;
        auto *redirectMeta = queryRsp.add_query_metas();
        redirectMeta->mutable_meta()->set_object_key("redirected");
        redirectMeta->add_payload_indexs(0);
        queryPayloads.emplace_back();
        return Status::OK();
    };

    HostPort primaryAddress;
    ASSERT_TRUE(primaryAddress.ParseString("127.0.0.1:9100").IsOk());
    ASSERT_TRUE(object_cache::QueryMetaWithRedirectAndMoving(primaryAddress, { "primary" }, queryMeta, {}, {}, rsp,
                                                             payloads)
                    .IsOk());
    EXPECT_EQ(primaryQueries, 1);
    EXPECT_EQ(redirectQueries, 1);
    ASSERT_EQ(rsp.query_metas_size(), 2);
    EXPECT_EQ(payloads.size(), 2U);
    EXPECT_EQ(rsp.query_metas(0).payload_indexs_size(), 1);
    EXPECT_EQ(rsp.query_metas(0).payload_indexs(0), 0U);
    EXPECT_EQ(rsp.query_metas(1).payload_indexs_size(), 1);
    EXPECT_EQ(rsp.query_metas(1).payload_indexs(0), 1U);
}

TEST(QueryMetaRedirectHelperTest, QueryMetaWithRedirectAndMovingOffsetsRedirectPayloadIndexesWhenPrimaryHasPayload)
{
    master::QueryMetaRspPb rsp;
    std::vector<RpcMessage> payloads;

    object_cache::QueryMetaAtMasterFn queryMeta = [&](const HostPort &, const std::vector<std::string> &,
                                        bool enableRedirect, master::QueryMetaRspPb &queryRsp,
                                        std::vector<RpcMessage> &queryPayloads) -> Status {
        if (enableRedirect) {
            auto *primaryMeta = queryRsp.add_query_metas();
            primaryMeta->mutable_meta()->set_object_key("primary");
            primaryMeta->add_payload_indexs(0);
            auto *info = queryRsp.add_info();
            info->set_redirect_meta_address("127.0.0.1:9200");
            *info->add_change_meta_ids() = "redirected";
            queryPayloads.emplace_back();
            return Status::OK();
        }
        auto *redirectMeta = queryRsp.add_query_metas();
        redirectMeta->mutable_meta()->set_object_key("redirected");
        redirectMeta->add_payload_indexs(0);
        queryPayloads.emplace_back();
        return Status::OK();
    };

    HostPort primaryAddress;
    ASSERT_TRUE(primaryAddress.ParseString("127.0.0.1:9100").IsOk());
    ASSERT_TRUE(object_cache::QueryMetaWithRedirectAndMoving(primaryAddress, { "primary" }, queryMeta, {}, {}, rsp,
                                                             payloads)
                    .IsOk());

    ASSERT_EQ(rsp.query_metas_size(), 2);
    ASSERT_EQ(payloads.size(), 2U);
    EXPECT_EQ(rsp.query_metas(0).meta().object_key(), "primary");
    EXPECT_EQ(rsp.query_metas(0).payload_indexs(0), 0U);
    EXPECT_EQ(rsp.query_metas(1).meta().object_key(), "redirected");
    EXPECT_EQ(rsp.query_metas(1).payload_indexs(0), 1U);
}

TEST(QueryMetaRedirectHelperTest, FollowQueryMetaRedirectsRejectsEmptyRedirectAddress)
{
    master::QueryMetaRspPb rsp;
    rsp.add_info()->set_redirect_meta_address("");

    std::vector<RpcMessage> payloads;
    const Status status = object_cache::FollowQueryMetaRedirects(
        [](const HostPort &, const std::vector<std::string> &, bool, master::QueryMetaRspPb &,
           std::vector<RpcMessage> &) { return Status::OK(); },
        {}, rsp, payloads);

    EXPECT_EQ(status.GetCode(), K_RUNTIME_ERROR);
}

TEST(QueryMetaRedirectHelperTest, WorkerTolerantRedirectAllowsMovingOnRedirectResponse)
{
    master::QueryMetaRspPb rsp;
    auto *info = rsp.add_info();
    info->set_redirect_meta_address("127.0.0.1:9200");
    *info->add_change_meta_ids() = "obj1";

    object_cache::QueryMetaRedirectFollowOptions options;
    options.rejectMovingOnRedirect = false;
    options.rejectNestedRedirectInfo = false;

    object_cache::QueryMetaAtMasterFn queryMeta = [&](const HostPort &, const std::vector<std::string> &, bool enableRedirect,
                                        master::QueryMetaRspPb &queryRsp, std::vector<RpcMessage> &) -> Status {
        EXPECT_FALSE(enableRedirect);
        queryRsp.set_meta_is_moving(true);
        queryRsp.add_info()->set_redirect_meta_address("127.0.0.1:9300");
        return Status::OK();
    };

    std::vector<RpcMessage> payloads;
    ASSERT_TRUE(object_cache::FollowQueryMetaRedirects(queryMeta, options, rsp, payloads).IsOk());
    EXPECT_TRUE(rsp.meta_is_moving());
}

TEST(QueryMetaRedirectHelperTest, RetryWhileMetaIsMovingInvokesBeforeMovingRetry)
{
    master::QueryMetaRspPb rsp;
    rsp.set_meta_is_moving(true);
    rsp.add_info();

    int32_t refreshCount = 0;
    object_cache::QueryMetaMovingRetryOptions options;
    options.maxMovingRetries = 2;
    options.beforeMovingRetry = [&]() {
        ++refreshCount;
        return Status::OK();
    };

    ASSERT_TRUE(object_cache::RetryWhileMetaIsMoving(
                    rsp,
                    [&]() {
                        rsp.Clear();
                        rsp.set_meta_is_moving(false);
                        return Status::OK();
                    },
                    [&]() { rsp.Clear(); }, options)
                    .IsOk());
    EXPECT_EQ(refreshCount, 1);
}
}  // namespace
}  // namespace ut
}  // namespace datasystem
