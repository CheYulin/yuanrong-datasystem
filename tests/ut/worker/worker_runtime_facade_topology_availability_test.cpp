/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Worker runtime facade topology availability tests.
 */
#include "datasystem/worker/runtime/worker_runtime_facade.h"

#include <atomic>
#include <chrono>
#include <future>
#include <mutex>
#include <thread>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/worker/worker_health_check.h"
#include "gtest/gtest.h"

namespace datasystem::worker {
namespace {
WorkerRecoveryEvidenceReport CompleteTopologyRecoveryReport()
{
    WorkerRecoveryEvidenceBuilder builder;
    return builder.MarkMembershipReady("membership ready")
        .MarkTopologyReady("topology ready")
        .MarkMetadataReady("metadata ready")
        .MarkSlotReady("slot ready")
        .MarkOwnershipReady("ownership ready")
        .MarkResourceReady("resource ready")
        .BuildReport("complete recovery evidence");
}

WorkerRecoveryEvidenceReport IncompleteObjectCacheRecoveryReport()
{
    WorkerRecoveryEvidenceBuilder builder;
    return builder.MarkMembershipReady("membership ready")
        .MarkTopologyReady("topology ready")
        .MarkMetadataReady("metadata ready")
        .MarkResourceReady("resource ready")
        .BuildReport("slot and ownership evidence missing");
}

constexpr auto NORMAL_TOPOLOGY = cluster::TopologyAvailabilityLevel::NORMAL;

bool WaitForInjectPoint(const std::string &name, uint64_t initialCount,
                        std::chrono::steady_clock::duration deadlockGuard)
{
    const auto deadline = std::chrono::steady_clock::now() + deadlockGuard;
    while (std::chrono::steady_clock::now() < deadline) {
        if (inject::GetExecuteCount(name) > initialCount) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return inject::GetExecuteCount(name) > initialCount;
}

bool ApplyTopologyAvailability(WorkerRuntimeFacade &runtime, cluster::TopologyAvailabilityLevel level,
                               const WorkerRecoveryEvidenceReport *recoveryReport = nullptr)
{
    bool serving = false;
    const auto token = runtime.ObserveTopologyAvailability(level, [&serving](bool open) { serving = open; });
    if (!runtime.CommitTopologyAvailability(token, recoveryReport, [&serving](bool open) { serving = open; })) {
        return false;
    }
    return serving;
}

enum class RecoveryCommitEntry { TOPOLOGY, RESOURCE, EVIDENCE };

class WorkerRuntimeFacadeRecoveryCommitTest : public testing::TestWithParam<RecoveryCommitEntry> {
protected:
    void SetUpCommit()
    {
        const auto report = CompleteTopologyRecoveryReport();
        if (GetParam() == RecoveryCommitEntry::RESOURCE) {
            ASSERT_TRUE(runtime_.BeginRecoveryEvidenceGeneration(generation_, "steady generation"));
            auto runningToken = runtime_.ObserveTopologyAvailability(NORMAL_TOPOLOGY, [](const auto &) {});
            ASSERT_TRUE(runtime_.CommitTopologyAvailability(runningToken, &report, [](const auto &) {}));
            runtime_.MarkOutOfMemory("allocation failed");
        } else {
            runtime_.MarkRecovering(WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE, "recovery pending",
                                    WorkerRecoveryPhase::METADATA);
            ASSERT_TRUE(runtime_.BeginRecoveryEvidenceGeneration(generation_, "recovery generation"));
        }
        token_ = runtime_.ObserveTopologyAvailability(NORMAL_TOPOLOGY, [](const auto &) {});
    }

    bool Commit(const WorkerRuntimeFacade::AdmissionCommitHandler &handler)
    {
        const auto report = CompleteTopologyRecoveryReport();
        switch (GetParam()) {
            case RecoveryCommitEntry::TOPOLOGY:
                return runtime_.CommitTopologyAvailability(token_, &report, handler);
            case RecoveryCommitEntry::RESOURCE:
                return runtime_.CommitResourceRecovery(token_, report, handler);
            case RecoveryCommitEntry::EVIDENCE:
                return runtime_.CommitRecoveryEvidence(generation_, NORMAL_TOPOLOGY, report, handler);
        }
        return false;
    }

    static constexpr WorkerRecoveryGeneration generation_{ 2 };
    WorkerRuntimeFacade runtime_;
    WorkerRuntimeFacade::TopologyAdmissionToken token_;
};

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, DelayedGenerationOneReportCannotCommitAfterGenerationTwoBegins)
{
    WorkerRuntimeFacade runtime;
    auto report = CompleteTopologyRecoveryReport();
    (void)runtime.ObserveTopologyAvailability(NORMAL_TOPOLOGY, [](bool) {});
    ASSERT_TRUE(runtime.BeginRecoveryEvidenceGeneration(2, "generation one"));
    EXPECT_TRUE(runtime.CommitRecoveryEvidence(2, NORMAL_TOPOLOGY, report, [](bool) {}));
    ASSERT_TRUE(runtime.BeginRecoveryEvidenceGeneration(3, "generation two"));
    EXPECT_FALSE(runtime.CommitRecoveryEvidence(2, NORMAL_TOPOLOGY, report, [](bool) {
        ADD_FAILURE() << "stale generation one report reopened the outer admission gate";
    }));
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, AuthoritativeCloseInvalidatesObservedReopenToken)
{
    WorkerRuntimeFacade runtime;
    auto report = CompleteTopologyRecoveryReport();
    ASSERT_TRUE(runtime.BeginRecoveryEvidenceGeneration(2, "recovery"));
    const auto reopenToken = runtime.ObserveTopologyAvailability(NORMAL_TOPOLOGY, [](bool) {});
    bool outerGate = true;
    runtime.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "isolated",
                              [&outerGate](bool open) { outerGate = open; });
    EXPECT_FALSE(outerGate);
    EXPECT_FALSE(runtime.CommitTopologyAvailability(reopenToken, &report, [](bool) {
        ADD_FAILURE() << "token observed before authoritative close was committed";
    }));

    const auto shutdownToken = runtime.ObserveTopologyAvailability(NORMAL_TOPOLOGY, [](bool) {});
    outerGate = true;
    runtime.MarkStopping(WorkerIsolationReason::PROCESS_STOPPING, "shutdown",
                         [&outerGate](bool open) { outerGate = open; });
    EXPECT_FALSE(outerGate);
    EXPECT_FALSE(runtime.CommitTopologyAvailability(
        shutdownToken, &report, [](bool) { ADD_FAILURE() << "token observed before shutdown was committed"; }));
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, TopologyEpochAdvanceInvalidatesObservedReopenToken)
{
    WorkerRuntimeFacade runtime;
    auto report = CompleteTopologyRecoveryReport();
    ASSERT_TRUE(runtime.BeginRecoveryEvidenceGeneration(2, "recovery"));
    const auto staleToken = runtime.ObserveTopologyAvailability(NORMAL_TOPOLOGY, [](bool) {});
    (void)runtime.ObserveTopologyAvailability(cluster::TopologyAvailabilityLevel::CONTROL_DEGRADED, [](bool) {});
    EXPECT_FALSE(runtime.CommitTopologyAvailability(
        staleToken, &report, [](bool) { ADD_FAILURE() << "token from an earlier topology epoch was committed"; }));
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, RecoveryEvidenceCommitRejectsTopologyEpochAba)
{
    WorkerRuntimeFacade runtime;
    auto report = CompleteTopologyRecoveryReport();
    (void)runtime.ObserveTopologyAvailability(NORMAL_TOPOLOGY, [](bool) {});
    ASSERT_TRUE(runtime.BeginRecoveryEvidenceGeneration(2, "recovery"));
    (void)runtime.ObserveTopologyAvailability(cluster::TopologyAvailabilityLevel::CONTROL_DEGRADED, [](bool) {});
    (void)runtime.ObserveTopologyAvailability(NORMAL_TOPOLOGY, [](bool) {});
    EXPECT_FALSE(runtime.CommitRecoveryEvidence(
        2, NORMAL_TOPOLOGY, report, [](bool) { ADD_FAILURE() << "recovery evidence crossed a topology epoch ABA"; }));
}

TEST_P(WorkerRuntimeFacadeRecoveryCommitTest, StalledPreparationCannotBlockIsolationOrReopenAdmission)
{
    constexpr const char *injectPoint = "WorkerRecoveryController.BeforeMarkRunning";
    constexpr auto deadlockGuard = std::chrono::seconds(30);
    SetUpCommit();
    const auto initialCount = inject::GetExecuteCount(injectPoint);
    ASSERT_TRUE(inject::Set(injectPoint, "pause()").IsOk());

    std::atomic<uint32_t> recoveryOpenCount{ 0 };
    auto recovery = std::async(std::launch::async, [&] {
        return Commit([&recoveryOpenCount](const WorkerAdmissionGateUpdate &update) {
            if (update.open) {
                recoveryOpenCount.fetch_add(1, std::memory_order_relaxed);
            }
        });
    });
    const bool preparationPaused = WaitForInjectPoint(injectPoint, initialCount, deadlockGuard);

    std::atomic<bool> isolationGateClosed{ false };
    auto isolation = std::async(std::launch::async, [&] {
        runtime_.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION,
                                   "keepalive failed during recovery",
                                   [&isolationGateClosed](const WorkerAdmissionGateUpdate &update) {
                                       isolationGateClosed.store(!update.open);
                                   });
    });
    const auto isolationWait = isolation.wait_for(deadlockGuard);

    EXPECT_TRUE(preparationPaused);
    EXPECT_EQ(isolationWait, std::future_status::ready);
    EXPECT_TRUE(inject::Clear(injectPoint).IsOk());
    isolation.get();
    const bool recoveryCommitted = recovery.get();

    const auto snapshot = runtime_.GetSnapshot();
    EXPECT_FALSE(recoveryCommitted);
    EXPECT_EQ(recoveryOpenCount.load(std::memory_order_relaxed), 0U);
    EXPECT_TRUE(isolationGateClosed.load());
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::LOCAL_ISOLATED);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION);
    EXPECT_EQ(runtime_.CheckAdmission(WorkerAdmissionKind::NORMAL_WRITE, "Create").GetCode(), K_NOT_READY);
    EXPECT_EQ(runtime_.CheckAdmission(WorkerAdmissionKind::RECOVERY_RPC, "PushMetaToWorker").GetCode(), K_NOT_READY);
}

TEST_P(WorkerRuntimeFacadeRecoveryCommitTest, DelayedGateCallbackCannotBlockIsolationOrApplyStaleOpen)
{
    constexpr auto deadlockGuard = std::chrono::seconds(30);
    SetUpCommit();
    bool outerGate = false;
    uint64_t appliedRevision = 0;
    std::mutex gateMutex;
    const auto applyGate = [&](const WorkerAdmissionGateUpdate &update) {
        std::lock_guard<std::mutex> lock(gateMutex);
        if (update.revision > appliedRevision) {
            appliedRevision = update.revision;
            outerGate = update.open;
        }
    };
    std::promise<void> callbackEntered;
    std::promise<void> releaseCallback;
    auto releaseFuture = releaseCallback.get_future().share();

    auto recovery = std::async(std::launch::async, [&] {
        return Commit([&](const WorkerAdmissionGateUpdate &update) {
            callbackEntered.set_value();
            releaseFuture.wait();
            applyGate(update);
        });
    });
    auto enteredFuture = callbackEntered.get_future();
    const auto enteredWait = enteredFuture.wait_for(deadlockGuard);

    auto isolation = std::async(std::launch::async, [&] {
        runtime_.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION,
                                   "isolation supersedes delayed gate notification",
                                   [&](const WorkerAdmissionGateUpdate &update) { applyGate(update); });
    });
    const auto isolationWait = isolation.wait_for(deadlockGuard);

    EXPECT_EQ(enteredWait, std::future_status::ready);
    EXPECT_EQ(isolationWait, std::future_status::ready);
    releaseCallback.set_value();
    EXPECT_TRUE(recovery.get());
    isolation.get();

    std::lock_guard<std::mutex> gateLock(gateMutex);
    EXPECT_FALSE(outerGate);
    EXPECT_EQ(runtime_.GetSnapshot().mode, WorkerServiceMode::LOCAL_ISOLATED);
}

INSTANTIATE_TEST_SUITE_P(AllRecoveryEntries, WorkerRuntimeFacadeRecoveryCommitTest,
                         testing::Values(RecoveryCommitEntry::TOPOLOGY, RecoveryCommitEntry::RESOURCE,
                                         RecoveryCommitEntry::EVIDENCE));

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, TopologyAdmissionSinkRejectsStaleOpenRevision)
{
    ASSERT_TRUE(SetHealthProbe().IsOk());
    const WorkerAdmissionGateUpdate staleOpen{ true, NextWorkerAdmissionGateRevision() };
    const WorkerAdmissionGateUpdate newerClose{ false, NextWorkerAdmissionGateRevision() };

    SetTopologyServingAdmission(newerClose);
    SetTopologyServingAdmission(staleOpen);

    EXPECT_FALSE(IsHealthy());
    SetTopologyServingAdmission(true);
    SetUnhealthy();
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, NonServingGenerationBindsToFirstServingObservation)
{
    WorkerRuntimeFacade runtime;
    runtime.MarkRecovering(WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE, "restart reconciliation pending",
                           WorkerRecoveryPhase::METADATA);
    ASSERT_TRUE(runtime.BeginRecoveryEvidenceGeneration(2, "restart generation"));
    const auto token = runtime.ObserveTopologyAvailability(NORMAL_TOPOLOGY, [](bool) {});
    auto incompleteReport = IncompleteObjectCacheRecoveryReport();
    bool outerGate = false;
    EXPECT_TRUE(
        runtime.CommitTopologyAvailability(token, &incompleteReport, [&outerGate](bool open) { outerGate = open; }));
    EXPECT_TRUE(outerGate);

    outerGate = false;
    EXPECT_TRUE(runtime.CommitRecoveryEvidence(2, NORMAL_TOPOLOGY, CompleteTopologyRecoveryReport(),
                                               [&outerGate](bool open) { outerGate = open; }));
    EXPECT_TRUE(outerGate);
    EXPECT_TRUE(runtime.CheckAdmission(WorkerAdmissionKind::MIGRATION_TARGET, "MigrateData").IsOk());
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, RecoveringPhaseDoesNotPinUnboundGeneration)
{
    WorkerRuntimeFacade runtime;
    runtime.MarkRecovering(WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE, "initial recovery",
                           WorkerRecoveryPhase::METADATA);
    ASSERT_TRUE(runtime.BeginRecoveryEvidenceGeneration(2, "restart generation"));
    runtime.MarkRecovering(WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE, "reconciliation dispatched",
                           WorkerRecoveryPhase::METADATA);
    (void)runtime.ObserveTopologyAvailability(NORMAL_TOPOLOGY, [](bool) {});

    bool outerGate = false;
    EXPECT_TRUE(runtime.CommitRecoveryEvidence(2, NORMAL_TOPOLOGY, CompleteTopologyRecoveryReport(),
                                               [&outerGate](bool open) { outerGate = open; }));
    EXPECT_TRUE(outerGate);
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, BoundGenerationCannotRebindToLaterSameServingLevel)
{
    WorkerRuntimeFacade runtime;
    runtime.MarkRecovering(WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE, "restart reconciliation pending",
                           WorkerRecoveryPhase::METADATA);
    ASSERT_TRUE(runtime.BeginRecoveryEvidenceGeneration(2, "restart generation"));
    (void)runtime.ObserveTopologyAvailability(NORMAL_TOPOLOGY, [](bool) {});
    (void)runtime.ObserveTopologyAvailability(NORMAL_TOPOLOGY, [](bool) {});

    EXPECT_FALSE(runtime.CommitRecoveryEvidence(2, NORMAL_TOPOLOGY, CompleteTopologyRecoveryReport(), [](bool) {
        ADD_FAILURE() << "bound generation rebound to a later NORMAL epoch";
    }));
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, GenerationBeginIsIdempotentOnlyForCurrentGeneration)
{
    WorkerRuntimeFacade runtime;
    EXPECT_TRUE(runtime.BeginRecoveryEvidenceGeneration(1, "attach current generation"));
    EXPECT_FALSE(runtime.BeginRecoveryEvidenceGeneration(0, "invalid generation"));
    ASSERT_TRUE(runtime.BeginRecoveryEvidenceGeneration(2, "next generation"));
    EXPECT_TRUE(runtime.BeginRecoveryEvidenceGeneration(2, "reattach current generation"));
    EXPECT_FALSE(runtime.BeginRecoveryEvidenceGeneration(1, "rollback generation"));
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, TopologyAvailableOpensRuntimeWhenEvidenceCompletes)
{
    WorkerRuntimeFacade runtime;
    auto report = CompleteTopologyRecoveryReport();
    ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::NORMAL, &report);

    auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RUNNING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::NONE);
    EXPECT_TRUE(snapshot.evidence.membershipReady);
    EXPECT_TRUE(snapshot.evidence.topologyReady);
    EXPECT_TRUE(snapshot.evidence.metadataReady);
    EXPECT_TRUE(snapshot.evidence.slotReady);
    EXPECT_TRUE(snapshot.evidence.ownershipReady);
    EXPECT_TRUE(snapshot.evidence.resourceReady);
    EXPECT_NE(snapshot.detail.find("ready=membership,topology,metadata,slot,ownership,resource"), std::string::npos);

    ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::CONTROL_DEGRADED, &report);
    snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RUNNING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::NONE);
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, TopologyAvailableOpensServingWhileReconciliationIsPending)
{
    WorkerRuntimeFacade runtime;
    auto report = IncompleteObjectCacheRecoveryReport();

    const bool opened = ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::NORMAL, &report);

    const auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RUNNING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::NONE);
    EXPECT_EQ(snapshot.recoveryPhase, WorkerRecoveryPhase::SLOT);
    EXPECT_TRUE(snapshot.evidence.metadataReady);
    EXPECT_FALSE(snapshot.evidence.slotReady);
    EXPECT_FALSE(snapshot.evidence.ownershipReady);
    EXPECT_TRUE(opened);
    EXPECT_TRUE(runtime.CheckAdmission(WorkerAdmissionKind::NORMAL_WRITE, "Create").IsOk());
    EXPECT_EQ(runtime.CheckAdmission(WorkerAdmissionKind::MIGRATION_TARGET, "MigrateData").GetCode(), K_NOT_READY);
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, NormalRecoveryRequestsObjectCacheEvidenceWhenMetadataIsPending)
{
    WorkerRuntimeFacade runtime;
    runtime.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "keepalive failed");
    auto report = IncompleteObjectCacheRecoveryReport();
    report.evidence.metadataReady = false;

    EXPECT_TRUE(runtime.ShouldRequestObjectCacheRecoveryEvidence(cluster::TopologyAvailabilityLevel::NORMAL, report));

    runtime.MarkRecovering(WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE, "topology reconciliation pending",
                           WorkerRecoveryPhase::TOPOLOGY);
    EXPECT_TRUE(runtime.ShouldRequestObjectCacheRecoveryEvidence(cluster::TopologyAvailabilityLevel::NORMAL, report));

    ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::NORMAL, &report);
    EXPECT_TRUE(runtime.ShouldRequestObjectCacheRecoveryEvidence(cluster::TopologyAvailabilityLevel::NORMAL, report));

    auto completeReport = CompleteTopologyRecoveryReport();
    EXPECT_FALSE(
        runtime.ShouldRequestObjectCacheRecoveryEvidence(cluster::TopologyAvailabilityLevel::NORMAL, completeReport));
    EXPECT_FALSE(
        runtime.ShouldRequestObjectCacheRecoveryEvidence(cluster::TopologyAvailabilityLevel::CONTROL_DEGRADED, report));

    WorkerRuntimeFacade runningRuntime;
    ApplyTopologyAvailability(runningRuntime, cluster::TopologyAvailabilityLevel::NORMAL, &completeReport);
    EXPECT_FALSE(
        runningRuntime.ShouldRequestObjectCacheRecoveryEvidence(cluster::TopologyAvailabilityLevel::NORMAL, report));
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest,
     ControlDegradedReopensLocalIsolationWithoutObjectCacheRecoveryEvidence)
{
    WorkerRuntimeFacade runtime;
    runtime.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "keepalive failed");
    runtime.MarkRecovering(WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE, "membership recovered",
                           WorkerRecoveryPhase::MEMBERSHIP);
    ASSERT_TRUE(runtime.BeginRecoveryEvidenceGeneration(2, "background reconciliation"));
    auto report = IncompleteObjectCacheRecoveryReport();

    const bool opened =
        ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::CONTROL_DEGRADED, &report);

    const auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RUNNING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::NONE);
    EXPECT_TRUE(opened);
    EXPECT_EQ(runtime.CheckAdmission(WorkerAdmissionKind::MIGRATION_TARGET, "MigrateData").GetCode(), K_NOT_READY);
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, TopologyAvailableWithoutEvidenceReportsMetadataPhase)
{
    WorkerRuntimeFacade runtime;

    ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::NORMAL);

    const auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RECOVERING);
    EXPECT_EQ(snapshot.recoveryPhase, WorkerRecoveryPhase::METADATA);
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, RefreshTopologyAdmissionOpensAfterRecoveryEvidenceCompletes)
{
    WorkerRuntimeFacade runtime;
    auto incompleteReport = IncompleteObjectCacheRecoveryReport();
    EXPECT_TRUE(ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::NORMAL, &incompleteReport));
    EXPECT_EQ(runtime.CheckAdmission(WorkerAdmissionKind::MIGRATION_TARGET, "MigrateData").GetCode(), K_NOT_READY);

    auto completeReport = CompleteTopologyRecoveryReport();
    EXPECT_TRUE(ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::NORMAL, &completeReport));

    const auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RUNNING);
    EXPECT_TRUE(snapshot.evidence.metadataReady);
    EXPECT_TRUE(snapshot.evidence.slotReady);
    EXPECT_TRUE(snapshot.evidence.ownershipReady);
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, TopologyRecoveryCanReopenLocalIsolationWithFreshEvidence)
{
    WorkerRuntimeFacade runtime;
    runtime.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "keepalive failed");
    runtime.MarkRecovering(WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE, "membership recovered",
                           WorkerRecoveryPhase::MEMBERSHIP);
    ASSERT_TRUE(runtime.BeginRecoveryEvidenceGeneration(2, "reconciliation"));

    auto completeReport = CompleteTopologyRecoveryReport();
    ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::NORMAL, &completeReport);

    const auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RUNNING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::NONE);
    EXPECT_TRUE(snapshot.evidence.metadataReady);
    EXPECT_TRUE(snapshot.evidence.slotReady);
    EXPECT_TRUE(snapshot.evidence.ownershipReady);
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, TopologyCommitCannotBypassPinnedGenerationEpoch)
{
    WorkerRuntimeFacade runtime;
    runtime.MarkRecovering(WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE, "recovering",
                           WorkerRecoveryPhase::TOPOLOGY);
    ASSERT_TRUE(runtime.BeginRecoveryEvidenceGeneration(2, "generation two"));
    (void)runtime.ObserveTopologyAvailability(cluster::TopologyAvailabilityLevel::NOT_READY, [](bool) {});
    const auto token = runtime.ObserveTopologyAvailability(NORMAL_TOPOLOGY, [](bool) {});
    const auto completeReport = CompleteTopologyRecoveryReport();
    bool outerGate = false;

    EXPECT_TRUE(
        runtime.CommitTopologyAvailability(token, &completeReport, [&outerGate](bool open) { outerGate = open; }));
    EXPECT_TRUE(outerGate);
    EXPECT_TRUE(runtime.CheckAdmission(WorkerAdmissionKind::NORMAL_WRITE, "Create").IsOk());
    EXPECT_EQ(runtime.CheckAdmission(WorkerAdmissionKind::MIGRATION_TARGET, "MigrateData").GetCode(), K_NOT_READY);

    runtime.MarkRecovering(WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE, "new recovery",
                           WorkerRecoveryPhase::TOPOLOGY);
    ASSERT_TRUE(runtime.BeginRecoveryEvidenceGeneration(3, "generation three"));
    const auto reboundToken = runtime.ObserveTopologyAvailability(NORMAL_TOPOLOGY, [](bool) {});
    EXPECT_TRUE(runtime.CommitTopologyAvailability(reboundToken, &completeReport, [](bool) {}));
    EXPECT_TRUE(runtime.CheckAdmission(WorkerAdmissionKind::MIGRATION_TARGET, "MigrateData").IsOk());
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, StaleIncompleteTopologyRecoveryCannotDemoteRunningWorker)
{
    WorkerRuntimeFacade runtime;
    ASSERT_TRUE(runtime.TryCompleteRecovery(CompleteTopologyRecoveryReport().evidence, "recovered"));

    auto incompleteReport = IncompleteObjectCacheRecoveryReport();
    ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::NORMAL, &incompleteReport);

    const auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RUNNING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::NONE);
    EXPECT_TRUE(IsServingMode(snapshot.mode));
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, OnlyResourceRecoveryCanReopenOutOfMemory)
{
    WorkerRuntimeFacade runtime;
    ASSERT_TRUE(runtime.BeginRecoveryEvidenceGeneration(2, "steady generation"));
    const auto oldToken = runtime.ObserveTopologyAvailability(NORMAL_TOPOLOGY, [](bool) {});
    const auto completeReport = CompleteTopologyRecoveryReport();
    ASSERT_TRUE(runtime.CommitTopologyAvailability(oldToken, &completeReport, [](bool) {}));
    runtime.MarkOutOfMemory("allocation failed");
    EXPECT_TRUE(runtime.GetSnapshot().evidence.metadataReady);
    EXPECT_TRUE(runtime.GetSnapshot().evidence.slotReady);
    EXPECT_TRUE(runtime.GetSnapshot().evidence.ownershipReady);

    EXPECT_FALSE(runtime.CommitTopologyAvailability(oldToken, &completeReport, [](bool) {}));
    const auto genericToken = runtime.ObserveTopologyAvailability(NORMAL_TOPOLOGY, [](bool) {});
    EXPECT_FALSE(runtime.CommitTopologyAvailability(genericToken, &completeReport, [](bool) {}));
    EXPECT_EQ(runtime.GetSnapshot().mode, WorkerServiceMode::OUT_OF_MEMORY);

    const auto resourceToken = runtime.ObserveTopologyAvailability(NORMAL_TOPOLOGY, [](bool) {});
    bool outerGate = false;
    EXPECT_TRUE(
        runtime.CommitResourceRecovery(resourceToken, completeReport, [&outerGate](bool open) { outerGate = open; }));

    const auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RUNNING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::NONE);
    EXPECT_TRUE(snapshot.evidence.resourceReady);
    EXPECT_TRUE(outerGate);
    EXPECT_EQ(runtime.CheckAdmission(WorkerAdmissionKind::MIGRATION_TARGET, "MigrateData").GetCode(), K_NOT_READY);
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, ResourceRecoveryRejectsStaleTopologyToken)
{
    WorkerRuntimeFacade runtime;
    ASSERT_TRUE(runtime.TryCompleteRecovery(CompleteTopologyRecoveryReport().evidence, "steady"));
    runtime.MarkOutOfMemory("allocation failed");
    const auto staleToken = runtime.ObserveTopologyAvailability(NORMAL_TOPOLOGY, [](bool) {});
    runtime.MarkOutOfMemory("new allocation failure");

    EXPECT_FALSE(runtime.CommitResourceRecovery(staleToken, CompleteTopologyRecoveryReport(), [](bool) {
        ADD_FAILURE() << "stale resource recovery token reopened admission";
    }));
    EXPECT_EQ(runtime.GetSnapshot().mode, WorkerServiceMode::OUT_OF_MEMORY);
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, ServingGateRequiresRunningRuntimeState)
{
    WorkerRuntimeFacade runtime;
    auto report = CompleteTopologyRecoveryReport();
    ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::NORMAL, &report);

    EXPECT_TRUE(IsServingMode(runtime.GetSnapshot().mode));
    EXPECT_TRUE(ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::CONTROL_DEGRADED));
    EXPECT_FALSE(ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::ROLE_ISOLATED));
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, IsolatedAndNotReadyLevelsCloseRuntimeState)
{
    WorkerRuntimeFacade runtime;

    ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::NOT_READY);
    auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::JOINING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::STARTUP_NOT_READY);

    ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::ROLE_ISOLATED);
    snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::LOCAL_ISOLATED);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::TOPOLOGY_PASSIVE_SCALE_DOWN);
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, HashRingSelfPassiveScaleDownDoesNotKillWorker)
{
    WorkerRuntimeFacade runtime;
    WorkerRecoveryEvidenceReport completeReport = CompleteTopologyRecoveryReport();

    ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::NORMAL, &completeReport);
    ASSERT_EQ(runtime.GetSnapshot().mode, WorkerServiceMode::RUNNING);

    ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::ROLE_ISOLATED, &completeReport);
    auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::LOCAL_ISOLATED);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::TOPOLOGY_PASSIVE_SCALE_DOWN);
    EXPECT_NE(snapshot.mode, WorkerServiceMode::STOPPING);
    EXPECT_EQ(
        runtime.CheckAdmission(WorkerAdmissionKind::NORMAL_WRITE, "legacy hash-ring passive scale-down").GetCode(),
        K_NOT_READY);

    ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::NORMAL, &completeReport);
    snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RUNNING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::NONE);
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, PassiveIsolationReopensMigrationOnlyAfterFreshGenerationCompletes)
{
    WorkerRuntimeFacade runtime;
    WorkerRecoveryEvidenceReport completeReport = CompleteTopologyRecoveryReport();

    ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::NORMAL, &completeReport);
    ASSERT_EQ(runtime.GetSnapshot().mode, WorkerServiceMode::RUNNING);
    ASSERT_TRUE(runtime.CheckAdmission(WorkerAdmissionKind::MIGRATION_TARGET, "MigrateData").IsOk());

    ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::ROLE_ISOLATED, &completeReport);
    auto snapshot = runtime.GetSnapshot();
    ASSERT_EQ(snapshot.mode, WorkerServiceMode::LOCAL_ISOLATED);
    ASSERT_EQ(snapshot.reason, WorkerIsolationReason::TOPOLOGY_PASSIVE_SCALE_DOWN);

    EXPECT_TRUE(ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::NORMAL, &completeReport));

    snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RUNNING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::NONE);
    EXPECT_FALSE(IsComplete(snapshot.evidence));
    EXPECT_TRUE(runtime.CheckAdmission(WorkerAdmissionKind::NORMAL_WRITE, "Create").IsOk());
    EXPECT_EQ(runtime.CheckAdmission(WorkerAdmissionKind::MIGRATION_TARGET, "MigrateData").GetCode(), K_NOT_READY);

    ASSERT_TRUE(runtime.BeginRecoveryEvidenceGeneration(2, "fresh network recovery"));
    EXPECT_TRUE(runtime.CommitRecoveryEvidence(2, NORMAL_TOPOLOGY, completeReport, [](bool) {}));
    EXPECT_TRUE(runtime.CheckAdmission(WorkerAdmissionKind::MIGRATION_TARGET, "MigrateData").IsOk());
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, VoluntaryScaleDownIsNotReopenedByTopologyRecoveryEvidence)
{
    WorkerRuntimeFacade runtime;
    WorkerRecoveryEvidenceReport completeReport = CompleteTopologyRecoveryReport();

    ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::NORMAL, &completeReport);
    ASSERT_EQ(runtime.GetSnapshot().mode, WorkerServiceMode::RUNNING);

    runtime.MarkDraining("voluntary scale-down drain started");
    ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::NORMAL, &completeReport);

    auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::DRAINING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::NONE);
    EXPECT_EQ(runtime.CheckAdmission(WorkerAdmissionKind::NORMAL_WRITE, "Create").GetCode(), K_NOT_READY);
    EXPECT_EQ(runtime.CheckAdmission(WorkerAdmissionKind::MIGRATION_TARGET, "MigrateData").GetCode(), K_NOT_READY);

    ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::CONTROL_DEGRADED, &completeReport);
    snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::DRAINING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::NONE);
}

TEST(WorkerRuntimeFacadeTopologyAvailabilityTest, ShuttingDownIsTerminal)
{
    WorkerRuntimeFacade runtime;
    ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::SHUTTING_DOWN);
    ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::NORMAL);

    const auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::STOPPING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::PROCESS_STOPPING);
}
}  // namespace
}  // namespace datasystem::worker
