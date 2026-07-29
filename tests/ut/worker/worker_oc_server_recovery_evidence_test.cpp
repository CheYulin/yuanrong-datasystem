/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#include <atomic>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include "datasystem/worker/object_cache/recovery/object_cache_recovery_state.h"
#include "datasystem/worker/worker_health_check.h"
#include "datasystem/worker/worker_oc_server.h"
namespace datasystem::ut {
namespace {
using worker::WorkerRecoveryEvidenceBuilder;
using worker::WorkerRecoveryEvidenceReport;
using worker::WorkerRecoveryGeneration;
using TopologyAdmissionToken = worker::WorkerRuntimeFacade::TopologyAdmissionToken;

constexpr auto NORMAL = cluster::TopologyAvailabilityLevel::NORMAL;
WorkerRecoveryEvidenceReport CompleteReport(const std::string &detail = "complete report")
{
    WorkerRecoveryEvidenceBuilder builder;
    return builder.MarkMembershipReady("membership ready")
        .MarkTopologyReady("topology ready")
        .MarkMetadataReady("metadata ready")
        .MarkSlotReady("slot ready")
        .MarkOwnershipReady("ownership ready")
        .MarkResourceReady("resource ready")
        .BuildReport(detail);
}

WorkerRecoveryEvidenceReport IncompleteReport(const std::string &detail = "recovery evidence pending")
{
    WorkerRecoveryEvidenceBuilder builder;
    return builder.MarkMembershipReady("membership ready")
        .MarkTopologyReady("topology ready")
        .MarkResourceReady("resource ready")
        .BuildReport(detail);
}

}  // namespace

class WorkerOCServerRecoveryEvidenceTest : public testing::Test {
protected:
    void SetUp() override
    {
        server_ = std::make_unique<worker::WorkerOCServer>(HostPort{}, HostPort{}, HostPort{}, nullptr);
    }

    Status Request(const WorkerRecoveryEvidenceReport &report, const std::function<Status()> &dispatch)
    {
        return server_->RequestObjectCacheRecoveryEvidenceIfNeeded(NORMAL, report, dispatch);
    }

    TopologyAdmissionToken Observe(cluster::TopologyAvailabilityLevel level)
    {
        return server_->workerRuntime_.ObserveTopologyAvailability(
            level, [](bool open) { SetTopologyServingAdmission(open); });
    }

    Status CommitRefresh(const TopologyAdmissionToken &token, const WorkerRecoveryEvidenceReport &report,
                         const std::function<Status()> &dispatch)
    {
        return server_->RefreshTopologyServingAdmission(token, report, dispatch);
    }

    Status Refresh(const WorkerRecoveryEvidenceReport &report, const std::function<Status()> &dispatch)
    {
        return CommitRefresh(Observe(NORMAL), report, dispatch);
    }

    Status CommitTerminal(WorkerRecoveryGeneration generation, const WorkerRecoveryEvidenceReport &report,
                          const std::function<Status()> &dispatch)
    {
        return server_->HandleObjectCacheRecoveryEvidenceReady(generation, NORMAL, report, dispatch);
    }

    void MakeRecoveryRequestable()
    {
        server_->workerRuntime_.MarkLocalIsolated(worker::WorkerIsolationReason::TOPOLOGY_PASSIVE_SCALE_DOWN,
                                                  "recovery evidence required");
    }

    void AuthorizeGeneration(WorkerRecoveryGeneration generation)
    {
        ASSERT_TRUE(server_->workerRuntime_.BeginRecoveryEvidenceGeneration(generation, "authorized generation"));
        (void)server_->workerRuntime_.ObserveTopologyAvailability(NORMAL, [](bool) {});
    }

    void SetRequestInFlight(bool inFlight)
    {
        server_->objectCacheRecoveryRequestInFlight_.store(inFlight);
    }

    bool RequestInFlight() const
    {
        return server_->objectCacheRecoveryRequestInFlight_.load();
    }

    worker::WorkerRuntimeFacade &Runtime()
    {
        return server_->workerRuntime_;
    }

private:
    std::unique_ptr<worker::WorkerOCServer> server_;
};

TEST_F(WorkerOCServerRecoveryEvidenceTest, ConcurrentIncompleteReportsRequestRecoveryOnce)
{
    MakeRecoveryRequestable();
    std::atomic<int> dispatchCount{ 0 };
    auto dispatch = [&dispatchCount] {
        dispatchCount.fetch_add(1);
        return Status::OK();
    };

    constexpr int threadCount = 16;
    std::atomic<bool> start{ false };
    std::vector<std::thread> threads;
    threads.reserve(threadCount);
    for (int i = 0; i < threadCount; ++i) {
        threads.emplace_back([&] {
            while (!start.load()) {
                std::this_thread::yield();
            }
            (void)Request(IncompleteReport(), dispatch);
        });
    }
    start.store(true);
    for (auto &thread : threads) {
        thread.join();
    }
    EXPECT_EQ(dispatchCount.load(), 1);
    EXPECT_TRUE(RequestInFlight());
}

TEST_F(WorkerOCServerRecoveryEvidenceTest, ReadyHandlerRejectsStaleGenerationAndDispatchesCurrentOnce)
{
    object_cache::ObjectCacheRecoveryState recoveryState;
    Status callbackStatus = Status::OK();
    int dispatchCount = 0;
    auto dispatch = [&] {
        ++dispatchCount;
        return Status::OK();
    };
    recoveryState.RegisterRecoveryEvidenceReadyHandler(
        [&](WorkerRecoveryGeneration generation, const WorkerRecoveryEvidenceReport &report) {
            callbackStatus = CommitTerminal(generation, report, dispatch);
        });
    const auto staleGeneration = recoveryState.BeginRecoveryEvidenceGeneration("stale");
    const auto currentGeneration = recoveryState.BeginRecoveryEvidenceGeneration("current");
    AuthorizeGeneration(currentGeneration);
    EXPECT_TRUE(CommitTerminal(staleGeneration, IncompleteReport("superseded terminal"), dispatch).IsOk());
    EXPECT_EQ(dispatchCount, 0);
    EXPECT_FALSE(recoveryState.MarkOwnershipReconciliationReady(staleGeneration, "stale terminal"));
    EXPECT_EQ(dispatchCount, 0);
    EXPECT_TRUE(recoveryState.MarkOwnershipReconciliationReady(currentGeneration, "current terminal"));

    EXPECT_TRUE(callbackStatus.IsOk());
    EXPECT_EQ(dispatchCount, 1);
    EXPECT_TRUE(RequestInFlight());
    EXPECT_FALSE(worker::IsComplete(Runtime().GetSnapshot().evidence));
}

TEST_F(WorkerOCServerRecoveryEvidenceTest, RefreshFailurePropagatesPreservesCommitAndRetries)
{
    ASSERT_TRUE(Runtime().TryCompleteRecovery(CompleteReport().evidence, "initially running"));
    MakeRecoveryRequestable();
    const auto rawReport = CompleteReport("raw terminal evidence");
    int dispatchCount = 0;
    worker::WorkerRuntimeStateSnapshot committedSnapshot;
    auto dispatch = [&] {
        ++dispatchCount;
        committedSnapshot = Runtime().GetSnapshot();
        return dispatchCount == 1 ? Status(K_RUNTIME_ERROR, "terminal dispatch failed") : Status::OK();
    };

    EXPECT_EQ(Refresh(rawReport, dispatch).GetCode(), K_RUNTIME_ERROR);
    EXPECT_FALSE(RequestInFlight());
    const auto failedSnapshot = Runtime().GetSnapshot();
    EXPECT_FALSE(worker::IsComplete(failedSnapshot.evidence));
    EXPECT_NE(failedSnapshot.detail.find(rawReport.detail), std::string::npos);
    EXPECT_EQ(failedSnapshot.evidence.membershipReady, committedSnapshot.evidence.membershipReady);
    EXPECT_EQ(failedSnapshot.evidence.topologyReady, committedSnapshot.evidence.topologyReady);
    EXPECT_EQ(failedSnapshot.evidence.metadataReady, committedSnapshot.evidence.metadataReady);
    EXPECT_EQ(failedSnapshot.evidence.slotReady, committedSnapshot.evidence.slotReady);
    EXPECT_EQ(failedSnapshot.evidence.ownershipReady, committedSnapshot.evidence.ownershipReady);
    EXPECT_EQ(failedSnapshot.evidence.resourceReady, committedSnapshot.evidence.resourceReady);
    EXPECT_EQ(failedSnapshot.detail, committedSnapshot.detail);
    EXPECT_TRUE(Refresh(rawReport, dispatch).IsOk());
    EXPECT_EQ(dispatchCount, 2);
    EXPECT_TRUE(RequestInFlight());
}

TEST_F(WorkerOCServerRecoveryEvidenceTest, IncompleteRefreshRemainsMigrationFailClosed)
{
    ASSERT_TRUE(Runtime().TryCompleteRecovery(CompleteReport().evidence, "initially running"));
    MakeRecoveryRequestable();
    int dispatchCount = 0;

    EXPECT_TRUE(Refresh(IncompleteReport("raw incomplete evidence"), [&] {
                    ++dispatchCount;
                    return Status::OK();
                }).IsOk());

    EXPECT_EQ(dispatchCount, 1);
    EXPECT_FALSE(worker::IsComplete(Runtime().GetSnapshot().evidence));
    EXPECT_EQ(Runtime().CheckAdmission(worker::WorkerAdmissionKind::MIGRATION_TARGET, "MigrateData").GetCode(),
              K_NOT_READY);
}

TEST_F(WorkerOCServerRecoveryEvidenceTest, AuthorizedCompleteGenerationCommitsAndForwardsComplete)
{
    constexpr WorkerRecoveryGeneration generation = 7;
    AuthorizeGeneration(generation);
    SetRequestInFlight(true);
    int dispatchCount = 0;
    const auto report = CompleteReport("authorized terminal evidence");

    EXPECT_TRUE(CommitTerminal(generation, report, [&] {
                    ++dispatchCount;
                    return Status::OK();
                }).IsOk());

    const auto snapshot = Runtime().GetSnapshot();
    EXPECT_TRUE(worker::IsComplete(snapshot.evidence));
    EXPECT_NE(snapshot.detail.find(report.detail), std::string::npos);
    EXPECT_EQ(dispatchCount, 0);
    EXPECT_FALSE(RequestInFlight());
    EXPECT_TRUE(Runtime().CheckAdmission(worker::WorkerAdmissionKind::MIGRATION_TARGET, "MigrateData").IsOk());
}

TEST_F(WorkerOCServerRecoveryEvidenceTest, BasicCommitRequestsFreshEvidenceFromRuntimePostCommitReport)
{
    ASSERT_TRUE(Runtime().TryCompleteRecovery(CompleteReport().evidence, "initially running"));
    MakeRecoveryRequestable();
    int dispatchCount = 0;

    EXPECT_TRUE(Refresh(CompleteReport(), [&] {
                    ++dispatchCount;
                    return Status::OK();
                }).IsOk());

    const auto snapshot = Runtime().GetSnapshot();
    EXPECT_EQ(snapshot.mode, worker::WorkerServiceMode::RUNNING);
    EXPECT_FALSE(worker::IsComplete(snapshot.evidence));
    EXPECT_EQ(dispatchCount, 1);
    EXPECT_TRUE(RequestInFlight());
}

TEST_F(WorkerOCServerRecoveryEvidenceTest, NonServingObserveFailClosesBeforeCommitHelper)
{
    ASSERT_TRUE(Runtime().TryCompleteRecovery(CompleteReport().evidence, "initially running"));
    int dispatchCount = 0;

    const auto token = Observe(cluster::TopologyAvailabilityLevel::ROLE_ISOLATED);
    EXPECT_EQ(Runtime().GetSnapshot().mode, worker::WorkerServiceMode::LOCAL_ISOLATED);
    EXPECT_TRUE(CommitRefresh(token, CompleteReport(), [&] {
                    ++dispatchCount;
                    return Status::OK();
                }).IsOk());

    EXPECT_EQ(dispatchCount, 0);
    EXPECT_EQ(Runtime().GetSnapshot().mode, worker::WorkerServiceMode::LOCAL_ISOLATED);
}
}  // namespace datasystem::ut
