/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker runtime facade.
 */
#include "datasystem/worker/runtime/worker_runtime_facade.h"

#include <mutex>
#include <optional>
#include <utility>

#include "datasystem/worker/runtime/worker_admission_facade.h"
#include "datasystem/worker/runtime/worker_recovery_controller.h"
#include "datasystem/worker/runtime/worker_runtime_state.h"

namespace datasystem::worker {
namespace {
WorkerRecoveryEvidenceReport TopologyAvailableEvidenceReport(const WorkerRecoveryEvidenceReport *recoveryReport)
{
    WorkerRecoveryEvidenceBuilder builder;
    builder.MarkMembershipReady("topology runtime reported membership available")
        .MarkTopologyReady("topology runtime reported topology available");
    std::string detail = "topology available";
    if (recoveryReport != nullptr) {
        if (recoveryReport->evidence.metadataReady) {
            builder.MarkMetadataReady(recoveryReport->detail);
        }
        if (recoveryReport->evidence.slotReady) {
            builder.MarkSlotReady(recoveryReport->detail);
        }
        if (recoveryReport->evidence.ownershipReady) {
            builder.MarkOwnershipReady(recoveryReport->detail);
        }
        if (recoveryReport->evidence.resourceReady) {
            builder.MarkResourceReady(recoveryReport->detail);
        }
        detail += "; " + recoveryReport->detail;
    } else {
        detail += "; waiting for recovery evidence";
    }
    return builder.BuildReport(detail);
}

WorkerRecoveryEvidenceReport ControlDegradedEvidenceReport(const WorkerRecoveryEvidenceReport *recoveryReport)
{
    WorkerRecoveryEvidenceBuilder builder;
    builder.MarkMembershipReady("control backend degraded globally")
        .MarkTopologyReady("topology scope classified control backend as globally degraded")
        .MarkMetadataReady("object-cache metadata recovery is not required for global control backend degradation")
        .MarkSlotReady("slot recovery is not required for global control backend degradation")
        .MarkOwnershipReady("ownership reconciliation is not required for global control backend degradation");
    std::string detail = "control backend globally degraded";
    if (recoveryReport != nullptr && recoveryReport->evidence.resourceReady) {
        builder.MarkResourceReady(recoveryReport->detail);
        detail += "; " + recoveryReport->detail;
    } else {
        builder.MarkResourceReady("resource recovery is not required for global control backend degradation");
    }
    auto report = builder.BuildReport(detail);
    report.evidence.metadataReady = recoveryReport != nullptr && recoveryReport->evidence.metadataReady;
    report.evidence.slotReady = recoveryReport != nullptr && recoveryReport->evidence.slotReady;
    report.evidence.ownershipReady = recoveryReport != nullptr && recoveryReport->evidence.ownershipReady;
    return report;
}

bool IsTopologyServingLevel(cluster::TopologyAvailabilityLevel level)
{
    return level == cluster::TopologyAvailabilityLevel::NORMAL
           || level == cluster::TopologyAvailabilityLevel::CONTROL_DEGRADED;
}

WorkerRecoveryEvidenceReport BasicServingEvidenceReport(const WorkerRecoveryEvidenceReport &recoveryReport)
{
    auto report = recoveryReport;
    report.evidence.metadataReady = false;
    report.evidence.slotReady = false;
    report.evidence.ownershipReady = false;
    report.detail = "generation epoch does not authorize complete recovery evidence; " + report.detail;
    return report;
}

void NotifyOuterGate(const WorkerRuntimeFacade::AdmissionCommitHandler &handler,
                     const WorkerAdmissionGateUpdate &update)
{
    if (handler != nullptr) {
        handler(update);
    }
}

enum class RecoveryCommitKind { TOPOLOGY, RESOURCE, EVIDENCE };
}  // namespace

class WorkerRuntimeFacade::AdmissionGuard::Impl {
public:
    explicit Impl(WorkerRuntimeStateReadGuard guard) : guard_(std::move(guard))
    {
    }
    ~Impl() = default;

private:
    WorkerRuntimeStateReadGuard guard_;
};

WorkerRuntimeFacade::AdmissionGuard::AdmissionGuard() = default;
WorkerRuntimeFacade::AdmissionGuard::~AdmissionGuard() = default;
WorkerRuntimeFacade::AdmissionGuard::AdmissionGuard(AdmissionGuard &&) noexcept = default;
WorkerRuntimeFacade::AdmissionGuard &WorkerRuntimeFacade::AdmissionGuard::operator=(AdmissionGuard &&) noexcept =
    default;

WorkerRuntimeFacade::AdmissionGuard::AdmissionGuard(std::unique_ptr<Impl> impl) : impl_(std::move(impl))
{
}

class WorkerRuntimeFacade::Impl {
public:
    Impl() : admission(state), recovery(state)
    {
    }
    ~Impl() = default;

    // Keep state before members that store references to it; C++ destroys members in reverse declaration order.
    WorkerRuntimeStateManager state;
    WorkerAdmissionFacade admission;
    WorkerRecoveryController recovery;
    std::mutex admissionCommitMutex;
    TopologyAdmissionToken topologyToken{ 1, 1, cluster::TopologyAvailabilityLevel::NOT_READY };
    // Unset until the generation's first serving observation. Once set, topology changes never rebind it.
    std::optional<uint64_t> generationCommitEpoch;
    std::atomic<bool> outerGateOpen{ false };

    struct RecoveryCommitPlan {
        RecoveryCommitKind kind;
        TopologyAdmissionToken token;
        std::optional<uint64_t> generationCommitEpoch;
        WorkerRecoveryEvidenceReport report;
        WorkerRecoveryController::PreparedRecovery prepared;
        bool skipPreparation{ false };
    };

    struct RecoveryCommitResult {
        bool committed{ false };
        std::optional<WorkerAdmissionGateUpdate> gateUpdate;
    };

    void InvalidateGenerationCommitBinding()
    {
        if (!generationCommitEpoch.has_value()) {
            generationCommitEpoch = topologyToken.topologyEpoch;
        }
    }

    WorkerAdmissionGateUpdate CommitOuterGateLocked(bool open)
    {
        outerGateOpen.store(open, std::memory_order_release);
        return WorkerAdmissionGateUpdate{ open, NextWorkerAdmissionGateRevision() };
    }

    bool CaptureRecoveryCommitLocked(RecoveryCommitKind kind, const TopologyAdmissionToken &token,
                                     const WorkerRecoveryEvidenceReport *recoveryReport, RecoveryCommitPlan &plan)
    {
        if (token.generation != topologyToken.generation || token.topologyEpoch != topologyToken.topologyEpoch
            || token.level != topologyToken.level || !IsTopologyServingLevel(token.level)) {
            return false;
        }
        const auto snapshot = state.GetSnapshot();
        if (kind == RecoveryCommitKind::RESOURCE) {
            if (recoveryReport == nullptr || !recoveryReport->evidence.resourceReady
                || snapshot.mode != WorkerServiceMode::OUT_OF_MEMORY) {
                return false;
            }
        } else if (snapshot.mode == WorkerServiceMode::OUT_OF_MEMORY) {
            return false;
        }
        if (kind == RecoveryCommitKind::EVIDENCE
            && (!generationCommitEpoch.has_value() || *generationCommitEpoch != token.topologyEpoch)) {
            return false;
        }

        std::optional<WorkerRecoveryEvidenceReport> basicReport;
        const WorkerRecoveryEvidenceReport *effectiveReport = recoveryReport;
        if (kind != RecoveryCommitKind::EVIDENCE && recoveryReport != nullptr
            && (!generationCommitEpoch.has_value() || *generationCommitEpoch != token.topologyEpoch)) {
            basicReport = BasicServingEvidenceReport(*recoveryReport);
            effectiveReport = &*basicReport;
        }
        plan.kind = kind;
        plan.token = token;
        plan.generationCommitEpoch = generationCommitEpoch;
        plan.report = token.level == cluster::TopologyAvailabilityLevel::CONTROL_DEGRADED
                          ? ControlDegradedEvidenceReport(effectiveReport)
                          : TopologyAvailableEvidenceReport(effectiveReport);
        plan.skipPreparation = snapshot.mode == WorkerServiceMode::RUNNING
                               && (effectiveReport == nullptr || !IsComplete(effectiveReport->evidence));
        return true;
    }

    RecoveryCommitResult CommitRecoveryLocked(const RecoveryCommitPlan &plan)
    {
        RecoveryCommitResult result;
        if (plan.token.generation != topologyToken.generation || plan.token.topologyEpoch != topologyToken.topologyEpoch
            || plan.token.level != topologyToken.level || generationCommitEpoch != plan.generationCommitEpoch
            || !IsTopologyServingLevel(plan.token.level)) {
            return result;
        }
        auto snapshot = state.GetSnapshot();
        if (plan.kind == RecoveryCommitKind::RESOURCE) {
            if (snapshot.mode != WorkerServiceMode::OUT_OF_MEMORY || !plan.report.evidence.resourceReady) {
                return result;
            }
            state.MarkRecovering(WorkerIsolationReason::OUT_OF_MEMORY,
                                 "resource recovery publication; validating recovery evidence",
                                 WorkerRecoveryPhase::RESOURCE);
            snapshot = state.GetSnapshot();
        } else if (snapshot.mode == WorkerServiceMode::OUT_OF_MEMORY) {
            return result;
        }

        if (snapshot.mode == WorkerServiceMode::LOCAL_ISOLATED
            && snapshot.reason == WorkerIsolationReason::TOPOLOGY_PASSIVE_SCALE_DOWN) {
            state.MarkRecovering(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION,
                                 "topology available; validating recovery evidence", WorkerRecoveryPhase::TOPOLOGY);
            snapshot = state.GetSnapshot();
        }

        bool serving = false;
        if (snapshot.mode == WorkerServiceMode::RUNNING && !IsComplete(plan.report.evidence)) {
            serving = true;
        } else {
            const auto recovered = recovery.CommitPreparedRecovery(plan.token.generation, plan.prepared);
            if (!recovered.has_value()) {
                return result;
            }
            serving = *recovered;
        }
        result.committed = true;
        result.gateUpdate = CommitOuterGateLocked(serving);
        return result;
    }
};

WorkerRuntimeFacade::WorkerRuntimeFacade() : impl_(std::make_unique<Impl>())
{
}

WorkerRuntimeFacade::~WorkerRuntimeFacade() = default;

WorkerRuntimeStateSnapshot WorkerRuntimeFacade::GetSnapshot() const
{
    return impl_->state.GetSnapshot();
}

void WorkerRuntimeFacade::MarkStarting(std::string detail)
{
    impl_->state.MarkStarting(std::move(detail));
}

void WorkerRuntimeFacade::MarkJoining(std::string detail)
{
    impl_->state.MarkJoining(std::move(detail));
}

void WorkerRuntimeFacade::MarkDraining(std::string detail)
{
    impl_->state.MarkDraining(std::move(detail));
}

void WorkerRuntimeFacade::MarkLocalIsolated(WorkerIsolationReason reason, std::string detail,
                                            const AdmissionCommitHandler &commitHandler)
{
    WorkerAdmissionGateUpdate gateUpdate;
    {
        std::lock_guard<std::mutex> lock(impl_->admissionCommitMutex);
        impl_->InvalidateGenerationCommitBinding();
        ++impl_->topologyToken.topologyEpoch;
        impl_->topologyToken.level = cluster::TopologyAvailabilityLevel::ROLE_ISOLATED;
        impl_->state.MarkLocalIsolated(reason, std::move(detail));
        gateUpdate = impl_->CommitOuterGateLocked(false);
    }
    NotifyOuterGate(commitHandler, gateUpdate);
}

void WorkerRuntimeFacade::MarkOutOfMemory(std::string detail)
{
    std::lock_guard<std::mutex> lock(impl_->admissionCommitMutex);
    const auto previousEpoch = impl_->topologyToken.topologyEpoch;
    ++impl_->topologyToken.topologyEpoch;
    if (impl_->generationCommitEpoch == previousEpoch) {
        impl_->generationCommitEpoch = impl_->topologyToken.topologyEpoch;
    }
    impl_->state.MarkOutOfMemory(std::move(detail));
}

void WorkerRuntimeFacade::MarkRecovering(WorkerIsolationReason reason, std::string detail, WorkerRecoveryPhase phase,
                                         const AdmissionCommitHandler &commitHandler)
{
    WorkerAdmissionGateUpdate gateUpdate;
    {
        std::lock_guard<std::mutex> lock(impl_->admissionCommitMutex);
        ++impl_->topologyToken.topologyEpoch;
        impl_->topologyToken.level = cluster::TopologyAvailabilityLevel::NOT_READY;
        impl_->state.MarkRecovering(reason, std::move(detail), phase);
        gateUpdate = impl_->CommitOuterGateLocked(false);
    }
    NotifyOuterGate(commitHandler, gateUpdate);
}

void WorkerRuntimeFacade::MarkStopping(WorkerIsolationReason reason, std::string detail,
                                       const AdmissionCommitHandler &commitHandler)
{
    WorkerAdmissionGateUpdate gateUpdate;
    {
        std::lock_guard<std::mutex> lock(impl_->admissionCommitMutex);
        impl_->InvalidateGenerationCommitBinding();
        ++impl_->topologyToken.topologyEpoch;
        impl_->topologyToken.level = cluster::TopologyAvailabilityLevel::SHUTTING_DOWN;
        impl_->state.MarkStopping(reason, std::move(detail));
        gateUpdate = impl_->CommitOuterGateLocked(false);
    }
    NotifyOuterGate(commitHandler, gateUpdate);
}

bool WorkerRuntimeFacade::BeginRecoveryEvidenceGeneration(WorkerRecoveryGeneration generation, std::string detail)
{
    std::lock_guard<std::mutex> lock(impl_->admissionCommitMutex);
    if (generation == impl_->topologyToken.generation) {
        return true;
    }
    if (generation < impl_->topologyToken.generation
        || !impl_->state.BeginRecoveryEvidenceGeneration(generation, std::move(detail))) {
        return false;
    }
    impl_->topologyToken.generation = generation;
    ++impl_->topologyToken.topologyEpoch;
    if (IsTopologyServingLevel(impl_->topologyToken.level)) {
        impl_->generationCommitEpoch = impl_->topologyToken.topologyEpoch;
    } else {
        impl_->generationCommitEpoch.reset();
    }
    return true;
}

bool WorkerRuntimeFacade::TryCompleteRecovery(const WorkerRunningEvidence &evidence, const std::string &detail)
{
    return impl_->recovery.TryCompleteRecovery(evidence, detail);
}

Status WorkerRuntimeFacade::CheckAdmission(WorkerAdmissionKind kind, const std::string &operation) const
{
    return impl_->admission.Check(kind, operation);
}

Status WorkerRuntimeFacade::AcquireAdmissionGuard(WorkerAdmissionKind kind, const std::string &operation,
                                                  AdmissionGuard &guard) const
{
    std::optional<WorkerRuntimeStateReadGuard> stateGuard;
    RETURN_IF_NOT_OK(impl_->admission.AcquireGuard(kind, operation, stateGuard));
    if (stateGuard.has_value()) {
        guard = AdmissionGuard(std::make_unique<AdmissionGuard::Impl>(std::move(*stateGuard)));
    }
    return Status::OK();
}

Status WorkerRuntimeFacade::AcquireNormalReadGuard(const std::string &operation, AdmissionGuard &guard) const
{
    return AcquireAdmissionGuard(WorkerAdmissionKind::NORMAL_READ, operation, guard);
}

WorkerRuntimeFacade::TopologyAdmissionToken WorkerRuntimeFacade::ObserveTopologyAvailability(
    cluster::TopologyAvailabilityLevel level, const AdmissionCommitHandler &commitHandler)
{
    std::optional<WorkerAdmissionGateUpdate> gateUpdate;
    TopologyAdmissionToken observedTopology;
    {
        std::lock_guard<std::mutex> lock(impl_->admissionCommitMutex);
        if (!IsTopologyServingLevel(level) && !impl_->generationCommitEpoch.has_value()) {
            impl_->InvalidateGenerationCommitBinding();
        }
        ++impl_->topologyToken.topologyEpoch;
        impl_->topologyToken.level = level;
        if (IsTopologyServingLevel(level) && !impl_->generationCommitEpoch.has_value()) {
            impl_->generationCommitEpoch = impl_->topologyToken.topologyEpoch;
        }
        if (!IsTopologyServingLevel(level)) {
            ApplyNonServingTopologyAvailabilityLocked(level);
            gateUpdate = impl_->CommitOuterGateLocked(false);
        }
        observedTopology = impl_->topologyToken;
    }
    if (gateUpdate.has_value()) {
        NotifyOuterGate(commitHandler, *gateUpdate);
    }
    return observedTopology;
}

bool WorkerRuntimeFacade::CommitTopologyAvailability(const TopologyAdmissionToken &token,
                                                     const WorkerRecoveryEvidenceReport *recoveryReport,
                                                     const AdmissionCommitHandler &commitHandler)
{
    Impl::RecoveryCommitPlan plan;
    std::optional<WorkerAdmissionGateUpdate> immediateUpdate;
    {
        std::lock_guard<std::mutex> lock(impl_->admissionCommitMutex);
        if (!impl_->CaptureRecoveryCommitLocked(RecoveryCommitKind::TOPOLOGY, token, recoveryReport, plan)) {
            return false;
        }
        if (plan.skipPreparation) {
            immediateUpdate = impl_->CommitOuterGateLocked(true);
        }
    }
    if (immediateUpdate.has_value()) {
        NotifyOuterGate(commitHandler, *immediateUpdate);
        return true;
    }

    plan.prepared = impl_->recovery.PrepareRecovery(plan.report.evidence, plan.report.detail);
    Impl::RecoveryCommitResult result;
    {
        std::lock_guard<std::mutex> lock(impl_->admissionCommitMutex);
        result = impl_->CommitRecoveryLocked(plan);
    }
    impl_->recovery.FinishRecovery();
    if (result.gateUpdate.has_value()) {
        NotifyOuterGate(commitHandler, *result.gateUpdate);
    }
    return result.committed;
}

bool WorkerRuntimeFacade::CommitResourceRecovery(const TopologyAdmissionToken &token,
                                                 const WorkerRecoveryEvidenceReport &recoveryReport,
                                                 const AdmissionCommitHandler &commitHandler)
{
    Impl::RecoveryCommitPlan plan;
    {
        std::lock_guard<std::mutex> lock(impl_->admissionCommitMutex);
        if (!impl_->CaptureRecoveryCommitLocked(RecoveryCommitKind::RESOURCE, token, &recoveryReport, plan)) {
            return false;
        }
    }
    plan.prepared = impl_->recovery.PrepareRecovery(plan.report.evidence, plan.report.detail);
    Impl::RecoveryCommitResult result;
    {
        std::lock_guard<std::mutex> lock(impl_->admissionCommitMutex);
        result = impl_->CommitRecoveryLocked(plan);
    }
    impl_->recovery.FinishRecovery();
    if (result.gateUpdate.has_value()) {
        NotifyOuterGate(commitHandler, *result.gateUpdate);
    }
    return result.committed;
}

bool WorkerRuntimeFacade::CommitRecoveryEvidence(WorkerRecoveryGeneration generation,
                                                 cluster::TopologyAvailabilityLevel level,
                                                 const WorkerRecoveryEvidenceReport &recoveryReport,
                                                 const AdmissionCommitHandler &commitHandler)
{
    Impl::RecoveryCommitPlan plan;
    std::optional<WorkerAdmissionGateUpdate> immediateUpdate;
    {
        std::lock_guard<std::mutex> lock(impl_->admissionCommitMutex);
        if (generation != impl_->topologyToken.generation || level != impl_->topologyToken.level
            || !impl_->CaptureRecoveryCommitLocked(RecoveryCommitKind::EVIDENCE, impl_->topologyToken, &recoveryReport,
                                                   plan)) {
            return false;
        }
        if (plan.skipPreparation) {
            immediateUpdate = impl_->CommitOuterGateLocked(true);
        }
    }
    if (immediateUpdate.has_value()) {
        NotifyOuterGate(commitHandler, *immediateUpdate);
        return true;
    }

    plan.prepared = impl_->recovery.PrepareRecovery(plan.report.evidence, plan.report.detail);
    Impl::RecoveryCommitResult result;
    {
        std::lock_guard<std::mutex> lock(impl_->admissionCommitMutex);
        result = impl_->CommitRecoveryLocked(plan);
    }
    impl_->recovery.FinishRecovery();
    if (result.gateUpdate.has_value()) {
        NotifyOuterGate(commitHandler, *result.gateUpdate);
    }
    return result.committed;
}

void WorkerRuntimeFacade::ApplyNonServingTopologyAvailabilityLocked(cluster::TopologyAvailabilityLevel level)
{
    switch (level) {
        case cluster::TopologyAvailabilityLevel::ROLE_ISOLATED:
            impl_->state.MarkLocalIsolated(WorkerIsolationReason::TOPOLOGY_PASSIVE_SCALE_DOWN,
                                           "topology availability is role-isolated");
            break;
        case cluster::TopologyAvailabilityLevel::NOT_READY:
            impl_->state.MarkJoining("topology availability is not ready");
            break;
        case cluster::TopologyAvailabilityLevel::SHUTTING_DOWN:
            impl_->state.MarkStopping(WorkerIsolationReason::PROCESS_STOPPING, "topology runtime is shutting down");
            break;
        default:
            impl_->state.MarkRecovering(WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE,
                                        "unknown topology availability", WorkerRecoveryPhase::TOPOLOGY);
            break;
    }
}

bool WorkerRuntimeFacade::ShouldRequestObjectCacheRecoveryEvidence(
    cluster::TopologyAvailabilityLevel level, const WorkerRecoveryEvidenceReport &recoveryReport) const
{
    if (level != cluster::TopologyAvailabilityLevel::NORMAL || IsComplete(recoveryReport.evidence)) {
        return false;
    }
    const auto runtimeState = GetSnapshot();
    if (runtimeState.mode == WorkerServiceMode::LOCAL_ISOLATED) {
        return true;
    }
    if (runtimeState.mode == WorkerServiceMode::RUNNING && !IsComplete(runtimeState.evidence)) {
        return true;
    }
    return runtimeState.mode == WorkerServiceMode::RECOVERING
           && runtimeState.reason == WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE
           && runtimeState.recoveryPhase != WorkerRecoveryPhase::RESOURCE;
}

void WorkerRuntimeFacade::PublishMetrics() const
{
    impl_->state.PublishMetrics();
}
}  // namespace datasystem::worker
