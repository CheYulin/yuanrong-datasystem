/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Tests for object-cache recovery state aggregation.
 */
#include "datasystem/worker/object_cache/recovery/object_cache_recovery_state.h"

#include <chrono>
#include <cstdlib>
#include <future>
#include <memory>
#include <stdexcept>
#include <thread>

#include <unistd.h>

#include <gtest/gtest.h>

#include "datasystem/worker/object_cache/recovery/object_cache_recovery_evidence.h"
#include "datasystem/worker/object_cache/recovery/object_cache_recovery_startup.h"
#include "datasystem/worker/runtime/worker_runtime_facade.h"

namespace datasystem {
namespace object_cache {
namespace {

worker::WorkerRecoveryEvidenceReport CompleteObjectCacheReport()
{
    worker::WorkerRecoveryEvidenceBuilder builder;
    return builder.MarkMetadataReady().MarkSlotReady().MarkOwnershipReady().MarkResourceReady().BuildReport("complete");
}

TEST(ObjectCacheRecoveryStateTest, MetadataSummaryUpdatesLatestEvidence)
{
    ObjectCacheRecoveryState state;
    const auto generation = state.CurrentRecoveryEvidenceGeneration();
    EXPECT_TRUE(state.GetLastMetadataRecoveryEvidenceReport().evidence.metadataReady);

    MetaDataRecoveryManager::RecoverySummary partial;
    partial.requestedCount = 2;
    partial.recoveredCount = 1;
    partial.failedIds = { "object-a" };
    state.SetMetadataRecoverySummary(generation, partial);
    EXPECT_FALSE(state.GetLastMetadataRecoveryEvidenceReport().evidence.metadataReady);

    MetaDataRecoveryManager::RecoverySummary complete;
    complete.requestedCount = 2;
    complete.recoveredCount = 2;
    state.SetMetadataRecoverySummary(generation, complete);
    EXPECT_TRUE(state.GetLastMetadataRecoveryEvidenceReport().evidence.metadataReady);
}

TEST(ObjectCacheRecoveryStateTest, ResourceRecoveryUsesGenerationToRejectStaleCommit)
{
    ObjectCacheRecoveryState state;
    const auto staleGeneration = state.MarkResourceRecoveryRequired(memory::CacheType::MEMORY);
    const auto currentGeneration = state.MarkResourceRecoveryRequired(memory::CacheType::DISK);

    EXPECT_FALSE(state.PublishResourceRecoveryIfCurrent(staleGeneration, [] { return true; }));
    auto snapshot = state.GetResourceRecoverySnapshot();
    EXPECT_TRUE(snapshot.memoryRequired);
    EXPECT_TRUE(snapshot.diskRequired);
    EXPECT_EQ(snapshot.generation, currentGeneration);

    EXPECT_TRUE(state.PublishResourceRecoveryIfCurrent(currentGeneration, [] { return true; }));
    snapshot = state.GetResourceRecoverySnapshot();
    EXPECT_FALSE(snapshot.memoryRequired);
    EXPECT_FALSE(snapshot.diskRequired);
}

TEST(ObjectCacheRecoveryStateTest, EvidenceGenerationInvalidatesOldReport)
{
    ObjectCacheRecoveryState state;
    const auto oldGeneration = state.BeginRecoveryEvidenceGeneration("old");
    EXPECT_TRUE(state.TrackEvidenceForGeneration(oldGeneration, CompleteObjectCacheReport()).evidence.ownershipReady);

    const auto newGeneration = state.BeginRecoveryEvidenceGeneration("new");
    EXPECT_FALSE(state.TrackEvidenceForGeneration(oldGeneration, CompleteObjectCacheReport()).evidence.ownershipReady);
    EXPECT_TRUE(state.TrackEvidenceForGeneration(newGeneration, CompleteObjectCacheReport()).evidence.ownershipReady);
}

TEST(ObjectCacheRecoveryStateTest, CurrentGenerationOwnerFailureTerminatesFanoutOnce)
{
    // Kills removal of the terminal/duplicate guard.
    ObjectCacheRecoveryState state;
    const auto generation = state.BeginRecoveryEvidenceGeneration("network recovery");
    int terminalCount = 0;
    ASSERT_TRUE(
        state.BeginOwnershipFanout(generation, { "owner-a", "owner-b" },
                                   [&](worker::WorkerRecoveryGeneration completedGeneration, const Status &status) {
                                       EXPECT_EQ(completedGeneration, generation);
                                       ++terminalCount;
                                       EXPECT_EQ(status.GetCode(), K_RUNTIME_ERROR);
                                       throw std::runtime_error("terminal failed");
                                   }));

    EXPECT_NO_THROW(
        EXPECT_TRUE(state.CompleteOwnershipOwner(generation, "owner-a", Status(K_RUNTIME_ERROR, "rpc failed"))));
    EXPECT_FALSE(state.CompleteOwnershipOwner(generation, "owner-a", Status(K_RUNTIME_ERROR, "duplicate")));
    EXPECT_FALSE(state.CompleteOwnershipOwner(generation, "owner-b", Status::OK()));
    EXPECT_EQ(terminalCount, 1);
}

TEST(ObjectCacheRecoveryStateTest, StaleOwnerCompletionCannotTerminateNewFanout)
{
    // Kills omission of the generation comparison.
    ObjectCacheRecoveryState state;
    const auto oldGeneration = state.BeginRecoveryEvidenceGeneration("old");
    ASSERT_NE(oldGeneration, 0);
    int oldTerminalCount = 0;
    ASSERT_TRUE(
        state.BeginOwnershipFanout(oldGeneration, { "shared-owner", "old-failing-owner" },
                                   [&](worker::WorkerRecoveryGeneration completedGeneration, const Status &status) {
                                       EXPECT_EQ(completedGeneration, oldGeneration);
                                       EXPECT_TRUE(status.IsError());
                                       ++oldTerminalCount;
                                   }));
    EXPECT_TRUE(
        state.CompleteOwnershipOwner(oldGeneration, "old-failing-owner", Status(K_RUNTIME_ERROR, "old fanout failed")));
    EXPECT_EQ(oldTerminalCount, 1);

    const auto currentGeneration = state.BeginRecoveryEvidenceGeneration("current");
    ASSERT_NE(currentGeneration, 0);
    ASSERT_GT(currentGeneration, oldGeneration);
    int terminalCount = 0;
    ASSERT_TRUE(
        state.BeginOwnershipFanout(currentGeneration, { "shared-owner", "current-peer" },
                                   [&](worker::WorkerRecoveryGeneration completedGeneration, const Status &status) {
                                       EXPECT_EQ(completedGeneration, currentGeneration);
                                       EXPECT_TRUE(status.IsOk());
                                       ++terminalCount;
                                   }));

    EXPECT_FALSE(
        state.CompleteOwnershipOwner(oldGeneration, "shared-owner", Status(K_RUNTIME_ERROR, "late old failure")));
    EXPECT_EQ(terminalCount, 0);
    EXPECT_TRUE(state.CompleteOwnershipOwner(currentGeneration, "shared-owner", Status::OK()));
    EXPECT_EQ(terminalCount, 0);
    EXPECT_TRUE(state.CompleteOwnershipOwner(currentGeneration, "current-peer", Status::OK()));
    EXPECT_EQ(terminalCount, 1);
}

TEST(ObjectCacheRecoveryStateTest, AllDistinctOwnersCompleteFanoutOnce)
{
    // Kills first-success completion and vector/count-only aggregation.
    ObjectCacheRecoveryState state;
    const auto generation = state.BeginRecoveryEvidenceGeneration("network recovery");
    int terminalCount = 0;
    ASSERT_TRUE(
        state.BeginOwnershipFanout(generation, { "owner-a", "owner-b", "owner-c" },
                                   [&](worker::WorkerRecoveryGeneration completedGeneration, const Status &status) {
                                       EXPECT_EQ(completedGeneration, generation);
                                       EXPECT_TRUE(status.IsOk());
                                       ++terminalCount;
                                   }));
    int replacementCount = 0;
    EXPECT_FALSE(state.BeginOwnershipFanout(
        generation, { "replacement" }, [&](worker::WorkerRecoveryGeneration, const Status &) { ++replacementCount; }));

    EXPECT_TRUE(state.CompleteOwnershipOwner(generation, "owner-c", Status::OK()));
    EXPECT_TRUE(state.CompleteOwnershipOwner(generation, "owner-a", Status::OK()));
    EXPECT_FALSE(state.CompleteOwnershipOwner(generation, "owner-a", Status::OK()));
    EXPECT_EQ(terminalCount, 0);
    EXPECT_TRUE(state.CompleteOwnershipOwner(generation, "owner-b", Status::OK()));
    EXPECT_FALSE(state.CompleteOwnershipOwner(generation, "owner-b", Status::OK()));
    EXPECT_EQ(terminalCount, 1);
    EXPECT_EQ(replacementCount, 0);
}

TEST(ObjectCacheRecoveryStateDeathTest, TerminalCompletionDestroysHandlerOutsideMutex)
{
    ASSERT_EXIT(
        {
            ::alarm(1);
            ObjectCacheRecoveryState state;
            bool captureDestroyed = false;
            struct ReenteringCapture {
                ~ReenteringCapture()
                {
                    (void)state->CurrentRecoveryEvidenceGeneration();
                    *destroyed = true;
                }

                ObjectCacheRecoveryState *state;
                bool *destroyed;
            };
            auto capture = std::make_shared<ReenteringCapture>();
            capture->state = &state;
            capture->destroyed = &captureDestroyed;
            const auto generation = state.BeginRecoveryEvidenceGeneration("active");
            if (!state.BeginOwnershipFanout(
                    generation, { "owner" },
                    [capture](worker::WorkerRecoveryGeneration, const Status &) { (void)capture; })) {
                std::_Exit(2);
            }
            capture.reset();

            if (!state.CompleteOwnershipOwner(generation, "owner", Status::OK())) {
                std::_Exit(3);
            }
            if (!captureDestroyed) {
                std::_Exit(4);
            }
            std::_Exit(0);
        },
        ::testing::ExitedWithCode(0), "");
}

TEST(ObjectCacheRecoveryStateTest, BuildsObjectCacheEvidenceFromInjectedReadinessCallbacks)
{
    ObjectCacheRecoveryState state;
    state.MarkResourceRecoveryRequired(memory::CacheType::MEMORY);
    const auto diskGeneration = state.MarkResourceRecoveryRequired(memory::CacheType::DISK);

    worker::WorkerRecoveryEvidenceBuilder slotBuilder;
    auto slotProvider = [&slotBuilder] { return slotBuilder.MarkSlotReady("slots complete").BuildReport("slots"); };

    std::vector<CacheType> resourceChecks;
    auto resourceRecovered = [&resourceChecks](CacheType cacheType) {
        resourceChecks.emplace_back(cacheType);
        return cacheType == CacheType::MEMORY;
    };
    auto ownershipProvider = [] { return BuildOwnershipRecoveryEvidenceReport(true, "master ownership confirmed"); };

    uint64_t evidenceGeneration = 0;
    auto report = state.BuildObjectCacheRecoveryEvidenceReport(slotProvider, ownershipProvider, resourceRecovered,
                                                               &evidenceGeneration);

    EXPECT_EQ(evidenceGeneration, diskGeneration);
    EXPECT_TRUE(report.evidence.metadataReady);
    EXPECT_TRUE(report.evidence.slotReady);
    EXPECT_FALSE(report.evidence.resourceReady);
    EXPECT_TRUE(report.evidence.ownershipReady);
    ASSERT_EQ(resourceChecks.size(), 2);
    EXPECT_EQ(resourceChecks[0], CacheType::MEMORY);
    EXPECT_EQ(resourceChecks[1], CacheType::DISK);
}

TEST(ObjectCacheRecoveryStateTest, NewRecoveryGenerationClearsOwnershipUntilMasterEvidenceArrives)
{
    ObjectCacheRecoveryState state;
    worker::WorkerRecoveryEvidenceBuilder slotBuilder;
    auto slotProvider = [&slotBuilder] { return slotBuilder.MarkSlotReady("slots complete").BuildReport("slots"); };
    auto resourceRecovered = [](CacheType) { return true; };

    const auto generation = state.BeginRecoveryEvidenceGeneration("network recovery pending");
    worker::WorkerRecoveryEvidenceBuilder metadataBuilder;
    state.SetMetadataRecoveryEvidenceReport(
        generation, metadataBuilder.MarkMetadataReady("metadata recovered").BuildReport("metadata recovered"));
    auto pending = state.BuildObjectCacheRecoveryEvidenceReport(
        slotProvider, [&state] { return state.GetLastOwnershipRecoveryEvidenceReport(); }, resourceRecovered);

    EXPECT_TRUE(pending.evidence.metadataReady);
    EXPECT_TRUE(pending.evidence.slotReady);
    EXPECT_FALSE(pending.evidence.ownershipReady);
    EXPECT_NE(pending.detail.find("ownership reconciliation pending"), std::string::npos);

    state.SetOwnershipRecoveryEvidenceReport(
        generation, BuildOwnershipRecoveryEvidenceReport(true, "master ownership reconciliation complete"));
    auto complete = state.BuildObjectCacheRecoveryEvidenceReport(
        slotProvider, [&state] { return state.GetLastOwnershipRecoveryEvidenceReport(); }, resourceRecovered);

    EXPECT_TRUE(complete.evidence.ownershipReady);
    EXPECT_NE(complete.detail.find("master ownership reconciliation complete"), std::string::npos);
}

TEST(ObjectCacheRecoveryStateTest, StaleOwnershipCompletionDoesNotOverwriteLatestEvidenceOrPublishReady)
{
    ObjectCacheRecoveryState state;
    int callbackCount = 0;
    state.RegisterRecoveryEvidenceReadyHandler([&callbackCount] { ++callbackCount; });
    const auto staleGeneration = state.BeginRecoveryEvidenceGeneration("old");
    const auto currentGeneration = state.BeginRecoveryEvidenceGeneration("new");

    EXPECT_FALSE(state.MarkOwnershipReconciliationReady(staleGeneration, "old completion"));
    EXPECT_EQ(callbackCount, 0);
    EXPECT_FALSE(state.GetLastMetadataRecoveryEvidenceReport().evidence.metadataReady);
    EXPECT_FALSE(state.GetLastOwnershipRecoveryEvidenceReport().evidence.ownershipReady);

    EXPECT_TRUE(state.MarkOwnershipReconciliationReady(currentGeneration, "current completion"));
    EXPECT_EQ(callbackCount, 1);
    EXPECT_TRUE(state.GetLastMetadataRecoveryEvidenceReport().evidence.metadataReady);
    EXPECT_TRUE(state.GetLastOwnershipRecoveryEvidenceReport().evidence.ownershipReady);
}

TEST(ObjectCacheRecoveryStateTest, ReadyCallbackDoesNotBlockNextGeneration)
{
    ObjectCacheRecoveryState state;
    std::promise<void> callbackEntered;
    std::promise<void> releaseCallback;
    auto releaseFuture = releaseCallback.get_future().share();
    worker::WorkerRecoveryGeneration callbackGeneration = 0;
    state.RegisterRecoveryEvidenceReadyHandler(
        [&](worker::WorkerRecoveryGeneration generation, const worker::WorkerRecoveryEvidenceReport &) {
            callbackGeneration = generation;
            callbackEntered.set_value();
            releaseFuture.wait();
        });
    const auto firstGeneration = state.BeginRecoveryEvidenceGeneration("first");
    auto completion = std::async(
        std::launch::async, [&] { return state.MarkOwnershipReconciliationReady(firstGeneration, "first complete"); });
    ASSERT_EQ(callbackEntered.get_future().wait_for(std::chrono::seconds(1)), std::future_status::ready);

    auto nextGeneration = std::async(std::launch::async, [&] { return state.BeginRecoveryEvidenceGeneration("next"); });
    const auto beginStatus = nextGeneration.wait_for(std::chrono::seconds(1));
    releaseCallback.set_value();

    EXPECT_EQ(beginStatus, std::future_status::ready);
    EXPECT_TRUE(completion.get());
    EXPECT_EQ(callbackGeneration, firstGeneration);
    EXPECT_NE(nextGeneration.get(), firstGeneration);
}

TEST(ObjectCacheRecoveryStateTest, ReadyCallbackCanBeginNextGeneration)
{
    struct ReentryContext {
        std::promise<bool> completion;
        worker::WorkerRecoveryGeneration callbackGeneration{ 0 };
        worker::WorkerRecoveryGeneration reentryGeneration{ 0 };
        bool callbackReportReady{ false };
    };

    auto state = std::make_shared<ObjectCacheRecoveryState>();
    auto context = std::make_shared<ReentryContext>();
    auto completion = context->completion.get_future();
    state->RegisterRecoveryEvidenceReadyHandler(
        [state = state.get(), context](worker::WorkerRecoveryGeneration generation,
                                       const worker::WorkerRecoveryEvidenceReport &report) {
            context->callbackGeneration = generation;
            context->callbackReportReady = report.evidence.ownershipReady;
            context->reentryGeneration = state->BeginRecoveryEvidenceGeneration("callback reentry");
        });
    const auto firstGeneration = state->BeginRecoveryEvidenceGeneration("first");
    std::thread completionThread([state, context, firstGeneration] {
        context->completion.set_value(state->MarkOwnershipReconciliationReady(firstGeneration, "first complete"));
    });

    if (completion.wait_for(std::chrono::seconds(1)) != std::future_status::ready) {
        completionThread.detach();
        ADD_FAILURE() << "ready callback deadlocked while beginning the next recovery generation";
        return;
    }
    completionThread.join();

    EXPECT_TRUE(completion.get());
    EXPECT_EQ(context->callbackGeneration, firstGeneration);
    EXPECT_TRUE(context->callbackReportReady);
    EXPECT_GT(context->reentryGeneration, firstGeneration);
    EXPECT_EQ(state->CurrentRecoveryEvidenceGeneration(), context->reentryGeneration);
    EXPECT_FALSE(state->GetLastOwnershipRecoveryEvidenceReport().evidence.ownershipReady);
}

TEST(ObjectCacheRecoveryStateTest, RestartStartupHookMarksReconciliationPending)
{
    ObjectCacheRecoveryState state;
    worker::WorkerRuntimeFacade runtime;

    MarkRestartReconciliationPending(&runtime, &state, true, true, true);

    EXPECT_FALSE(state.GetLastMetadataRecoveryEvidenceReport().evidence.metadataReady);
    EXPECT_NE(state.GetLastMetadataRecoveryEvidenceReport().detail.find("restart reconciliation pending"),
              std::string::npos);
    EXPECT_FALSE(state.GetLastOwnershipRecoveryEvidenceReport().evidence.ownershipReady);
    EXPECT_NE(state.GetLastOwnershipRecoveryEvidenceReport().detail.find("ownership reconciliation pending"),
              std::string::npos);
    auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, worker::WorkerServiceMode::RECOVERING);
    EXPECT_EQ(snapshot.reason, worker::WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE);
    EXPECT_EQ(snapshot.recoveryPhase, worker::WorkerRecoveryPhase::METADATA);
    (void)runtime.ObserveTopologyAvailability(cluster::TopologyAvailabilityLevel::NORMAL, [](bool) {});
    const auto generation = state.CurrentRecoveryEvidenceGeneration();
    auto report = CompleteObjectCacheReport();
    report.evidence.membershipReady = true;
    report.evidence.topologyReady = true;
    EXPECT_TRUE(
        runtime.CommitRecoveryEvidence(generation, cluster::TopologyAvailabilityLevel::NORMAL, report, [](bool) {}));
}

TEST(ObjectCacheRecoveryStateTest, RestartStartupHookSkipsWhenReconciliationIsDisabled)
{
    ObjectCacheRecoveryState state;
    worker::WorkerRuntimeFacade runtime;

    MarkRestartReconciliationPending(&runtime, &state, true, true, false);

    EXPECT_TRUE(state.GetLastMetadataRecoveryEvidenceReport().evidence.metadataReady);
    EXPECT_EQ(runtime.GetSnapshot().mode, worker::WorkerServiceMode::STARTING);
}

}  // namespace
}  // namespace object_cache
}  // namespace datasystem
