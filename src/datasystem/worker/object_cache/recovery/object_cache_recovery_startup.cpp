/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Object-cache recovery startup hooks.
 */
#include "datasystem/worker/object_cache/recovery/object_cache_recovery_startup.h"

#include "datasystem/common/log/logging.h"
#include "datasystem/worker/object_cache/recovery/object_cache_recovery_state.h"
#include "datasystem/worker/runtime/worker_runtime_facade.h"

namespace datasystem {
namespace object_cache {
bool MarkRestartReconciliationPending(worker::WorkerRuntimeFacade *runtime, ObjectCacheRecoveryState *recoveryState,
                                      bool isRestart, bool controlBackendAvailableAtStartup, bool enableReconciliation)
{
    if (!isRestart || !controlBackendAvailableAtStartup || !enableReconciliation) {
        return true;
    }
    worker::WorkerRecoveryGeneration generation = 0;
    if (recoveryState != nullptr) {
        generation = recoveryState->BeginRecoveryEvidenceGeneration("restart reconciliation pending");
    }
    if (runtime == nullptr) {
        return true;
    }
    runtime->MarkRecovering(worker::WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE,
                            "restart reconciliation pending", worker::WorkerRecoveryPhase::METADATA);
    if (!runtime->BeginRecoveryEvidenceGeneration(generation, "restart reconciliation pending")) {
        LOG(ERROR) << "Runtime rejected restart recovery generation " << generation
                   << "; admission remains fail-closed";
        return false;
    }
    return true;
}
}  // namespace object_cache
}  // namespace datasystem
