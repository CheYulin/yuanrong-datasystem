/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Object-cache recovery state aggregation.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_RECOVERY_OBJECT_CACHE_RECOVERY_STATE_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_RECOVERY_OBJECT_CACHE_RECOVERY_STATE_H

#include <cstdint>
#include <functional>
#include <mutex>
#include <set>
#include <string>

#include "datasystem/common/shared_memory/arena_group_key.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/metadata_recovery_manager.h"
#include "datasystem/worker/runtime/worker_recovery_evidence.h"

namespace datasystem {
namespace object_cache {

class ObjectCacheRecoveryState {
public:
    using SlotRecoveryEvidenceProvider = std::function<worker::WorkerRecoveryEvidenceReport()>;
    using OwnershipRecoveryEvidenceProvider = std::function<worker::WorkerRecoveryEvidenceReport()>;
    using ResourceRecoveredProvider = std::function<bool(CacheType)>;
    using RecoveryEvidenceReadyHandler =
        std::function<void(worker::WorkerRecoveryGeneration, const worker::WorkerRecoveryEvidenceReport &)>;
    using OwnershipFanoutTerminalHandler = std::function<void(worker::WorkerRecoveryGeneration, const Status &)>;

    struct ResourceRecoverySnapshot {
        bool memoryRequired{ false };
        bool diskRequired{ false };
        uint64_t generation{ 0 };
    };

    ObjectCacheRecoveryState();
    ~ObjectCacheRecoveryState();

    ObjectCacheRecoveryState(const ObjectCacheRecoveryState &) = delete;
    ObjectCacheRecoveryState &operator=(const ObjectCacheRecoveryState &) = delete;

    worker::WorkerRecoveryEvidenceReport GetLastMetadataRecoveryEvidenceReport() const;
    bool SetMetadataRecoverySummary(worker::WorkerRecoveryGeneration generation,
                                    const MetaDataRecoveryManager::RecoverySummary &summary);
    bool SetMetadataRecoveryEvidenceReport(worker::WorkerRecoveryGeneration generation,
                                           worker::WorkerRecoveryEvidenceReport report);
    worker::WorkerRecoveryEvidenceReport GetLastOwnershipRecoveryEvidenceReport() const;
    bool SetOwnershipRecoveryEvidenceReport(worker::WorkerRecoveryGeneration generation,
                                            worker::WorkerRecoveryEvidenceReport report);
    bool PublishRecoveryCompletionIfCurrent(worker::WorkerRecoveryGeneration generation,
                                            const std::function<void()> &publish);
    bool MarkOwnershipReconciliationReady(worker::WorkerRecoveryGeneration generation, const std::string &detail);
    void RegisterRecoveryEvidenceReadyHandler(RecoveryEvidenceReadyHandler handler);
    void RegisterRecoveryEvidenceReadyHandler(std::function<void()> handler);

    uint64_t MarkResourceRecoveryRequired(memory::CacheType cacheType);
    ResourceRecoverySnapshot GetResourceRecoverySnapshot() const;
    bool PublishResourceRecoveryIfCurrent(uint64_t generation, const std::function<bool()> &publish);
    worker::WorkerRecoveryEvidenceReport BuildObjectCacheRecoveryEvidenceReport(
        const SlotRecoveryEvidenceProvider &slotEvidenceProvider,
        const OwnershipRecoveryEvidenceProvider &ownershipEvidenceProvider,
        const ResourceRecoveredProvider &resourceRecovered, uint64_t *resourceRecoveryGeneration = nullptr) const;

    worker::WorkerRecoveryGeneration BeginRecoveryEvidenceGeneration(const std::string &detail);
    worker::WorkerRecoveryGeneration TryBeginRecoveryEvidenceGeneration(
        worker::WorkerRecoveryGeneration expectedCurrent, const std::string &detail);
    worker::WorkerRecoveryGeneration CurrentRecoveryEvidenceGeneration() const;
    worker::WorkerRecoveryEvidenceReport TrackEvidenceForGeneration(worker::WorkerRecoveryGeneration generation,
                                                                    worker::WorkerRecoveryEvidenceReport report);
    bool BeginOwnershipFanout(worker::WorkerRecoveryGeneration generation, const std::set<std::string> &expectedOwners,
                              OwnershipFanoutTerminalHandler terminalHandler);
    bool CompleteOwnershipOwner(worker::WorkerRecoveryGeneration generation, const std::string &owner,
                                const Status &status);

private:
    mutable std::mutex recoveryEvidenceGenerationMutex_;
    worker::WorkerRecoveryGeneration recoveryEvidenceGeneration_{ 1 };
    worker::WorkerRecoveryEvidenceReport lastMetadataRecoveryEvidence_;
    worker::WorkerRecoveryEvidenceReport lastOwnershipRecoveryEvidence_;
    worker::WorkerRecoveryEvidenceReport recoveryEvidenceReport_;
    worker::WorkerRecoveryGeneration ownershipFanoutGeneration_{ 0 };
    std::set<std::string> expectedOwnershipOwners_;
    std::set<std::string> pendingOwnershipOwners_;
    bool ownershipFanoutTerminal_{ true };
    OwnershipFanoutTerminalHandler ownershipFanoutTerminalHandler_;

    mutable std::mutex resourceRecoveryMutex_;
    bool memoryRecoveryRequired_{ false };
    bool diskRecoveryRequired_{ false };
    uint64_t resourceRecoveryGeneration_{ 0 };

    mutable std::mutex recoveryEvidenceReadyHandlerMutex_;
    RecoveryEvidenceReadyHandler recoveryEvidenceReadyHandler_;
};

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_RECOVERY_OBJECT_CACHE_RECOVERY_STATE_H
