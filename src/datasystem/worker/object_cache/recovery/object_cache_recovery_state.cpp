/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Object-cache recovery state aggregation.
 */
#include "datasystem/worker/object_cache/recovery/object_cache_recovery_state.h"

#include <exception>
#include <utility>

#include "datasystem/worker/object_cache/recovery/object_cache_recovery_evidence.h"

namespace datasystem {
namespace object_cache {

ObjectCacheRecoveryState::ObjectCacheRecoveryState()
    : lastMetadataRecoveryEvidence_(BuildMetadataRecoveryEvidenceReport(MetaDataRecoveryManager::RecoverySummary{})),
      lastOwnershipRecoveryEvidence_(BuildOwnershipRecoveryEvidenceReport(true, "no ownership reconciliation pending"))
{
}

ObjectCacheRecoveryState::~ObjectCacheRecoveryState() = default;

worker::WorkerRecoveryEvidenceReport ObjectCacheRecoveryState::GetLastMetadataRecoveryEvidenceReport() const
{
    std::lock_guard<std::mutex> lock(recoveryEvidenceGenerationMutex_);
    return lastMetadataRecoveryEvidence_;
}

bool ObjectCacheRecoveryState::SetMetadataRecoverySummary(worker::WorkerRecoveryGeneration generation,
                                                          const MetaDataRecoveryManager::RecoverySummary &summary)
{
    auto metadataReport = BuildMetadataRecoveryEvidenceReport(summary);
    auto ownershipReport = BuildOwnershipRecoveryEvidenceReport(metadataReport.evidence.metadataReady,
                                                                metadataReport.evidence.metadataReady
                                                                    ? "metadata owner reconciliation confirmed"
                                                                    : "metadata owner reconciliation incomplete");
    std::lock_guard<std::mutex> lock(recoveryEvidenceGenerationMutex_);
    if (generation == 0 || generation != recoveryEvidenceGeneration_) {
        return false;
    }
    lastMetadataRecoveryEvidence_ = std::move(metadataReport);
    lastOwnershipRecoveryEvidence_ = std::move(ownershipReport);
    return true;
}

bool ObjectCacheRecoveryState::SetMetadataRecoveryEvidenceReport(worker::WorkerRecoveryGeneration generation,
                                                                 worker::WorkerRecoveryEvidenceReport report)
{
    std::lock_guard<std::mutex> lock(recoveryEvidenceGenerationMutex_);
    if (generation == 0 || generation != recoveryEvidenceGeneration_) {
        return false;
    }
    lastMetadataRecoveryEvidence_ = std::move(report);
    return true;
}

worker::WorkerRecoveryEvidenceReport ObjectCacheRecoveryState::GetLastOwnershipRecoveryEvidenceReport() const
{
    std::lock_guard<std::mutex> lock(recoveryEvidenceGenerationMutex_);
    return lastOwnershipRecoveryEvidence_;
}

bool ObjectCacheRecoveryState::SetOwnershipRecoveryEvidenceReport(worker::WorkerRecoveryGeneration generation,
                                                                  worker::WorkerRecoveryEvidenceReport report)
{
    std::lock_guard<std::mutex> lock(recoveryEvidenceGenerationMutex_);
    if (generation == 0 || generation != recoveryEvidenceGeneration_) {
        return false;
    }
    lastOwnershipRecoveryEvidence_ = std::move(report);
    return true;
}

bool ObjectCacheRecoveryState::PublishRecoveryCompletionIfCurrent(worker::WorkerRecoveryGeneration generation,
                                                                  const std::function<void()> &publish)
{
    std::lock_guard<std::mutex> lock(recoveryEvidenceGenerationMutex_);
    if (generation == 0 || generation != recoveryEvidenceGeneration_) {
        return false;
    }
    publish();
    return true;
}

bool ObjectCacheRecoveryState::MarkOwnershipReconciliationReady(worker::WorkerRecoveryGeneration generation,
                                                                const std::string &detail)
{
    worker::WorkerRecoveryEvidenceReport readyReport;
    {
        std::lock_guard<std::mutex> lock(recoveryEvidenceGenerationMutex_);
        if (generation == 0 || generation != recoveryEvidenceGeneration_) {
            return false;
        }
        worker::WorkerRecoveryEvidenceBuilder builder;
        lastMetadataRecoveryEvidence_ = builder.MarkMetadataReady(detail).BuildReport(detail);
        lastOwnershipRecoveryEvidence_ = BuildOwnershipRecoveryEvidenceReport(true, detail);
        recoveryEvidenceReport_ = builder.MarkOwnershipReady(detail).BuildReport(detail);
        readyReport = recoveryEvidenceReport_;
    }
    RecoveryEvidenceReadyHandler handler;
    {
        std::lock_guard<std::mutex> lock(recoveryEvidenceReadyHandlerMutex_);
        handler = recoveryEvidenceReadyHandler_;
    }
    if (handler != nullptr) {
        handler(generation, readyReport);
    }
    return true;
}

void ObjectCacheRecoveryState::RegisterRecoveryEvidenceReadyHandler(RecoveryEvidenceReadyHandler handler)
{
    std::lock_guard<std::mutex> lock(recoveryEvidenceReadyHandlerMutex_);
    recoveryEvidenceReadyHandler_ = std::move(handler);
}

void ObjectCacheRecoveryState::RegisterRecoveryEvidenceReadyHandler(std::function<void()> handler)
{
    RegisterRecoveryEvidenceReadyHandler(
        [handler = std::move(handler)](worker::WorkerRecoveryGeneration, const worker::WorkerRecoveryEvidenceReport &) {
            if (handler != nullptr) {
                handler();
            }
        });
}

uint64_t ObjectCacheRecoveryState::MarkResourceRecoveryRequired(memory::CacheType cacheType)
{
    std::lock_guard<std::mutex> lock(resourceRecoveryMutex_);
    if (cacheType == memory::CacheType::DISK) {
        diskRecoveryRequired_ = true;
    } else {
        memoryRecoveryRequired_ = true;
    }
    return ++resourceRecoveryGeneration_;
}

ObjectCacheRecoveryState::ResourceRecoverySnapshot ObjectCacheRecoveryState::GetResourceRecoverySnapshot() const
{
    std::lock_guard<std::mutex> lock(resourceRecoveryMutex_);
    return { memoryRecoveryRequired_, diskRecoveryRequired_, resourceRecoveryGeneration_ };
}

bool ObjectCacheRecoveryState::PublishResourceRecoveryIfCurrent(uint64_t generation,
                                                                const std::function<bool()> &publish)
{
    std::lock_guard<std::mutex> lock(resourceRecoveryMutex_);
    if (generation != resourceRecoveryGeneration_) {
        return false;
    }
    const bool open = publish();
    if (open) {
        memoryRecoveryRequired_ = false;
        diskRecoveryRequired_ = false;
    }
    return open;
}

worker::WorkerRecoveryEvidenceReport ObjectCacheRecoveryState::BuildObjectCacheRecoveryEvidenceReport(
    const SlotRecoveryEvidenceProvider &slotEvidenceProvider,
    const OwnershipRecoveryEvidenceProvider &ownershipEvidenceProvider,
    const ResourceRecoveredProvider &resourceRecovered, uint64_t *resourceRecoveryGeneration) const
{
    const auto metadataReport = GetLastMetadataRecoveryEvidenceReport();
    worker::WorkerRecoveryEvidenceBuilder builder;
    const auto slotReport =
        slotEvidenceProvider == nullptr ? builder.BuildReport("slot_manager_unavailable") : slotEvidenceProvider();
    const auto ownershipReport = ownershipEvidenceProvider == nullptr
                                     ? builder.BuildReport("ownership_evidence_unavailable")
                                     : ownershipEvidenceProvider();
    const auto resourceSnapshot = GetResourceRecoverySnapshot();
    if (resourceRecoveryGeneration != nullptr) {
        *resourceRecoveryGeneration = resourceSnapshot.generation;
    }
    const bool memoryReady =
        !resourceSnapshot.memoryRequired || (resourceRecovered != nullptr && resourceRecovered(CacheType::MEMORY));
    const bool diskReady =
        !resourceSnapshot.diskRequired || (resourceRecovered != nullptr && resourceRecovered(CacheType::DISK));
    return object_cache::BuildObjectCacheRecoveryEvidenceReport(metadataReport, slotReport, ownershipReport,
                                                                memoryReady && diskReady);
}

worker::WorkerRecoveryGeneration ObjectCacheRecoveryState::BeginRecoveryEvidenceGeneration(const std::string &detail)
{
    return TryBeginRecoveryEvidenceGeneration(0, detail);
}

worker::WorkerRecoveryGeneration ObjectCacheRecoveryState::TryBeginRecoveryEvidenceGeneration(
    worker::WorkerRecoveryGeneration expectedCurrent, const std::string &detail)
{
    worker::WorkerRecoveryEvidenceBuilder builder;
    OwnershipFanoutTerminalHandler oldHandler;
    worker::WorkerRecoveryGeneration generation;
    {
        std::lock_guard<std::mutex> lock(recoveryEvidenceGenerationMutex_);
        if (expectedCurrent != 0 && expectedCurrent != recoveryEvidenceGeneration_) {
            return 0;
        }
        if (ownershipFanoutGeneration_ == recoveryEvidenceGeneration_ && !ownershipFanoutTerminal_) {
            return 0;
        }
        generation = ++recoveryEvidenceGeneration_;
        recoveryEvidenceReport_ = worker::WorkerRecoveryEvidenceReport{};
        recoveryEvidenceReport_.detail = detail;
        lastMetadataRecoveryEvidence_ = builder.BuildReport(detail);
        lastOwnershipRecoveryEvidence_ = builder.BuildReport("ownership reconciliation pending");
        ownershipFanoutGeneration_ = 0;
        expectedOwnershipOwners_.clear();
        pendingOwnershipOwners_.clear();
        ownershipFanoutTerminal_ = true;
        oldHandler = std::move(ownershipFanoutTerminalHandler_);
    }
    return generation;
}

worker::WorkerRecoveryGeneration ObjectCacheRecoveryState::CurrentRecoveryEvidenceGeneration() const
{
    std::lock_guard<std::mutex> lock(recoveryEvidenceGenerationMutex_);
    return recoveryEvidenceGeneration_;
}

worker::WorkerRecoveryEvidenceReport ObjectCacheRecoveryState::TrackEvidenceForGeneration(
    worker::WorkerRecoveryGeneration generation, worker::WorkerRecoveryEvidenceReport report)
{
    std::lock_guard<std::mutex> lock(recoveryEvidenceGenerationMutex_);
    if (generation == 0 || generation != recoveryEvidenceGeneration_) {
        return {};
    }
    recoveryEvidenceReport_ = std::move(report);
    return recoveryEvidenceReport_;
}

bool ObjectCacheRecoveryState::BeginOwnershipFanout(worker::WorkerRecoveryGeneration generation,
                                                    const std::set<std::string> &expectedOwners,
                                                    OwnershipFanoutTerminalHandler terminalHandler)
{
    std::lock_guard<std::mutex> lock(recoveryEvidenceGenerationMutex_);
    if (generation == 0 || generation != recoveryEvidenceGeneration_ || expectedOwners.empty()
        || (ownershipFanoutGeneration_ == generation && !ownershipFanoutTerminal_)) {
        return false;
    }
    ownershipFanoutGeneration_ = generation;
    expectedOwnershipOwners_ = expectedOwners;
    pendingOwnershipOwners_ = expectedOwners;
    ownershipFanoutTerminal_ = false;
    ownershipFanoutTerminalHandler_ = std::move(terminalHandler);
    return true;
}

bool ObjectCacheRecoveryState::CompleteOwnershipOwner(worker::WorkerRecoveryGeneration generation,
                                                      const std::string &owner, const Status &status)
{
    OwnershipFanoutTerminalHandler handler;
    Status terminalStatus;
    {
        std::lock_guard<std::mutex> lock(recoveryEvidenceGenerationMutex_);
        if (generation == 0 || generation != recoveryEvidenceGeneration_ || generation != ownershipFanoutGeneration_
            || ownershipFanoutTerminal_ || expectedOwnershipOwners_.count(owner) == 0
            || pendingOwnershipOwners_.erase(owner) == 0) {
            return false;
        }
        if (status.IsError() || pendingOwnershipOwners_.empty()) {
            ownershipFanoutTerminal_ = true;
            terminalStatus = status;
            handler = std::move(ownershipFanoutTerminalHandler_);
        }
    }
    if (handler != nullptr) {
        try {
            handler(generation, terminalStatus);
        } catch (const std::exception &) {
        } catch (...) {
        }
    }
    return true;
}

}  // namespace object_cache
}  // namespace datasystem
