/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker runtime facade tests.
 */
#include "datasystem/worker/runtime/worker_runtime_facade.h"

#include <chrono>
#include <future>
#include <thread>

#include "gtest/gtest.h"

namespace datasystem::worker {
namespace {
WorkerRunningEvidence CompleteEvidence()
{
    WorkerRunningEvidence evidence;
    evidence.membershipReady = true;
    evidence.topologyReady = true;
    evidence.metadataReady = true;
    evidence.slotReady = true;
    evidence.ownershipReady = true;
    evidence.resourceReady = true;
    return evidence;
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

TEST(WorkerRuntimeFacadeTest, CentralizesAdmissionRecoveryAndScaleDownTerminalState)
{
    WorkerRuntimeFacade runtime;

    EXPECT_TRUE(runtime.TryCompleteRecovery(CompleteEvidence(), "startup evidence ready"));
    EXPECT_TRUE(runtime.CheckAdmission(WorkerAdmissionKind::NORMAL_READ, "Get").IsOk());

    runtime.MarkDraining("voluntary scale-down drain started");

    EXPECT_FALSE(runtime.TryCompleteRecovery(CompleteEvidence(), "late failure recovery evidence"));
    EXPECT_TRUE(runtime.CheckAdmission(WorkerAdmissionKind::NORMAL_READ, "Get").IsOk());
    EXPECT_EQ(runtime.CheckAdmission(WorkerAdmissionKind::NORMAL_WRITE, "Set").GetCode(), K_NOT_READY);
    EXPECT_EQ(runtime.CheckAdmission(WorkerAdmissionKind::MIGRATION_TARGET, "MigrateData").GetCode(), K_NOT_READY);
    EXPECT_EQ(runtime.GetSnapshot().mode, WorkerServiceMode::DRAINING);
}

TEST(WorkerRuntimeFacadeTest, AppliesTopologyAvailabilityWithObjectCacheEvidence)
{
    WorkerRuntimeFacade runtime;
    runtime.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "keepalive failed");

    WorkerRecoveryEvidenceBuilder pendingBuilder;
    const auto pending = pendingBuilder.MarkMembershipReady().MarkTopologyReady().BuildReport("metadata pending");
    EXPECT_TRUE(runtime.ShouldRequestObjectCacheRecoveryEvidence(cluster::TopologyAvailabilityLevel::NORMAL, pending));

    WorkerRecoveryEvidenceBuilder completeBuilder;
    const auto complete =
        completeBuilder.MarkMetadataReady().MarkSlotReady().MarkOwnershipReady().MarkResourceReady().BuildReport(
            "object-cache recovery complete");
    EXPECT_TRUE(ApplyTopologyAvailability(runtime, cluster::TopologyAvailabilityLevel::NORMAL, &complete));
    EXPECT_FALSE(
        runtime.ShouldRequestObjectCacheRecoveryEvidence(cluster::TopologyAvailabilityLevel::NORMAL, complete));
    EXPECT_EQ(runtime.GetSnapshot().mode, WorkerServiceMode::RUNNING);
    EXPECT_TRUE(runtime.CheckAdmission(WorkerAdmissionKind::NORMAL_WRITE, "Create").IsOk());
}

TEST(WorkerRuntimeFacadeTest, TransitionPendingAllowsControlPlaneAdmissionWithoutGuard)
{
    WorkerRuntimeFacade runtime;
    ASSERT_TRUE(runtime.TryCompleteRecovery(CompleteEvidence(), "ready"));

    WorkerRuntimeFacade::AdmissionGuard writeGuard;
    ASSERT_TRUE(runtime.AcquireAdmissionGuard(WorkerAdmissionKind::NORMAL_WRITE, "Create", writeGuard).IsOk());

    std::promise<void> transitionStarted;
    auto transition = std::async(std::launch::async, [&runtime, &transitionStarted]() {
        transitionStarted.set_value();
        runtime.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "local");
    });
    if (transitionStarted.get_future().wait_for(std::chrono::seconds(1)) != std::future_status::ready) {
        writeGuard = WorkerRuntimeFacade::AdmissionGuard();
        ADD_FAILURE() << "transition thread did not start";
        transition.wait();
        return;
    }
    const auto pendingDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (runtime.CheckAdmission(WorkerAdmissionKind::NORMAL_READ, "Get").IsOk()
           && std::chrono::steady_clock::now() < pendingDeadline) {
        std::this_thread::yield();
    }
    if (runtime.CheckAdmission(WorkerAdmissionKind::NORMAL_READ, "Get").IsOk()) {
        writeGuard = WorkerRuntimeFacade::AdmissionGuard();
        ADD_FAILURE() << "transition intent did not close normal admission";
        transition.wait();
        return;
    }

    WorkerRuntimeFacade::AdmissionGuard diagnosticGuard;
    EXPECT_TRUE(runtime.AcquireAdmissionGuard(WorkerAdmissionKind::DIAGNOSTIC_RPC, "Inspect", diagnosticGuard).IsOk());

    WorkerRuntimeFacade::AdmissionGuard normalGuard;
    EXPECT_EQ(runtime.AcquireAdmissionGuard(WorkerAdmissionKind::NORMAL_READ, "Get", normalGuard).GetCode(),
              K_NOT_READY);

    writeGuard = WorkerRuntimeFacade::AdmissionGuard();
    EXPECT_EQ(transition.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    EXPECT_EQ(runtime.GetSnapshot().mode, WorkerServiceMode::LOCAL_ISOLATED);
}
}  // namespace
}  // namespace datasystem::worker
