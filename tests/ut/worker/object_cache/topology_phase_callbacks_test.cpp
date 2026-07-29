/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Cluster topology business callback contract tests.
 */
#include <regex>
#include <type_traits>
#include <variant>
#include <vector>

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/control/topology_plan_builder.h"
#include "datasystem/cluster/control/topology_task_materializer.h"
#include "datasystem/cluster/executor/topology_task_executor.h"
#include "datasystem/cluster/executor/topology_phase_callbacks.h"
#include "datasystem/cluster/executor/storage_scan_plan.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/cluster/repository/topology_repository.h"
#include "datasystem/cluster/runtime/coordination_event_dispatcher.h"
#include "datasystem/cluster/runtime/topology_reader.h"
#include "datasystem/cluster/runtime/topology_snapshot_state.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#include "datasystem/master/object_cache/oc_migrate_metadata_manager.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/master/stream_cache/sc_metadata_manager.h"
#include "datasystem/master/stream_cache/sc_migrate_metadata_manager.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_clear_data_flow.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"
#include "datasystem/worker/runtime/worker_topology_phase_callbacks.h"
#include "ut/cluster/testing/fake_coordination_backend.h"

#include "gtest/gtest.h"

namespace datasystem::ut {
namespace {

using MetadataMigration = Status (master::OCMigrateMetadataManager::*)(
    const cluster::TopologyPhaseAction &, const cluster::IKeyFilter &, const std::string &,
    std::chrono::steady_clock::time_point, const cluster::CancellationToken &);
using StreamMigration = Status (master::SCMigrateMetadataManager::*)(
    const cluster::TopologyPhaseAction &, const cluster::IKeyFilter &, const std::string &,
    std::chrono::steady_clock::time_point, const cluster::CancellationToken &);
using ObjectFailureRecovery = Status (master::OCMetadataManager::*)(
    const cluster::TopologyPhaseAction &, const cluster::IKeyFilter &, const cluster::StorageScanPlan &,
    const std::string &, std::chrono::steady_clock::time_point, const cluster::CancellationToken &);
using StreamFailureRecovery = Status (master::SCMetadataManager::*)(
    const cluster::TopologyPhaseAction &, const cluster::IKeyFilter &, const std::string &,
    std::chrono::steady_clock::time_point, const cluster::CancellationToken &);
using ScaleInData = Status (object_cache::WorkerOCServiceImpl::*)(
    const cluster::TopologyPhaseAction &, const std::string &, std::chrono::steady_clock::time_point,
    const cluster::CancellationToken &);
using ObjectMetaScan = Status (master::ObjectMetaStore::*)(
    const cluster::StorageScanPlan &, const cluster::IKeyFilter &,
    const std::function<Status(const std::string &, const std::string &)> &);
using ObjectFailureCleanup = Status (master::OCMetadataManager::*)(
    const cluster::TopologyPhaseAction &, const std::string &, std::chrono::steady_clock::time_point,
    const cluster::CancellationToken &);
using StreamFailureCleanup = Status (master::SCMetadataManager::*)(
    const cluster::TopologyPhaseAction &, const std::string &, std::chrono::steady_clock::time_point,
    const cluster::CancellationToken &);
using ScaleInCleanup = Status (object_cache::WorkerOCServiceImpl::*)(
    const cluster::TopologyPhaseAction &, const cluster::IKeyFilter &, const std::string &,
    std::chrono::steady_clock::time_point, const cluster::CancellationToken &, std::function<Status()> &,
    cluster::TopologyCleanupEffect &);
using FailureDataCleanup = Status (object_cache::WorkerOcServiceClearDataFlow::*)(
    const cluster::TopologyPhaseAction &, const cluster::IKeyFilter &, const std::string &,
    std::chrono::steady_clock::time_point, const cluster::CancellationToken &);
using ScaleInWorkerDataDrain = Status (worker::WorkerTopologyPhaseCallbacks::*)(
    const cluster::TopologyCallbackContext &);

class RecordingMetadataActions final : public worker::IWorkerTopologyMetadataActions {
public:
    RecordingMetadataActions(std::vector<std::string> &calls, Status recovery, Status device)
        : calls_(calls), recoveryStatus_(recovery), deviceStatus_(device)
    {
    }
    Status MigrateMetadata(const cluster::TopologyCallbackContext &) override
    {
        return Status::OK();
    }
    Status RecoverFailureMetadata(const cluster::TopologyCallbackContext &) override
    {
        calls_.emplace_back("recover-metadata");
        return recoveryStatus_;
    }
    Status CleanupFailureMetadata(const cluster::TopologyCallbackContext &) override
    {
        calls_.emplace_back("cleanup-metadata");
        return Status::OK();
    }

    Status CleanupDeviceMetadata(const cluster::TopologyCallbackContext &) override
    {
        calls_.emplace_back("cleanup-device");
        return deviceStatus_;
    }

private:
    std::vector<std::string> &calls_;
    Status recoveryStatus_;
    Status deviceStatus_;
};

class RecordingObjectCacheActions final : public worker::IWorkerTopologyObjectCacheActions {
public:
    explicit RecordingObjectCacheActions(std::vector<std::string> &calls) : calls_(calls)
    {
    }

    Status DrainScaleInData(const cluster::TopologyCallbackContext &) override
    {
        return Status::OK();
    }

    Status PrepareScaleInCleanup(const cluster::TopologyCallbackContext &,
                                 std::unique_ptr<cluster::TopologyPreparedCleanup> &) override
    {
        return Status::OK();
    }

    Status CleanupLocalData(const cluster::TopologyCallbackContext &) override
    {
        calls_.emplace_back("cleanup-local-data");
        return Status::OK();
    }

private:
    std::vector<std::string> &calls_;
};

// clang-format off
#define FORWARD_CALLBACK(method)                                                   \
    Status method(const cluster::TopologyCallbackContext &context) override        \
    {                                                                              \
        return delegate_.method(context);                                          \
    }
// clang-format on

class FailureActionForwarder final : public cluster::ITopologyPhaseCallbacks {
public:
    explicit FailureActionForwarder(cluster::ITopologyPhaseCallbacks &delegate) : delegate_(delegate)
    {
    }

    FORWARD_CALLBACK(OnScaleOut)
    FORWARD_CALLBACK(OnScaleIn)
    FORWARD_CALLBACK(OnScaleInDataDrain)
    Status PrepareScaleInCleanup(const cluster::TopologyCallbackContext &context,
                                 std::unique_ptr<cluster::TopologyPreparedCleanup> &cleanup) override
    {
        return delegate_.PrepareScaleInCleanup(context, cleanup);
    }
    Status OnFailure(const cluster::TopologyCallbackContext &context) override
    {
        auto action = context.action;
        action.failed->address.clear();
        cluster::TopologyCallbackContext forwarded{ action,
                                                    context.businessOperationId,
                                                    context.deadline,
                                                    context.cancellation,
                                                    context.keyFilter,
                                                    context.storageScanPlan };
        return delegate_.OnFailure(forwarded);
    }

private:
    cluster::ITopologyPhaseCallbacks &delegate_;
};
#undef FORWARD_CALLBACK

TEST(TopologyBusinessContractTest, ExposesOnlyOpaqueTaskLevelBusinessEntryPoints)
{
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<MetadataMigration>(
                                    &master::OCMigrateMetadataManager::MigrateTopologyMetadata)),
                                MetadataMigration>));
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<StreamMigration>(
                                    &master::SCMigrateMetadataManager::MigrateTopologyMetadata)),
                                StreamMigration>));
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<ObjectFailureRecovery>(
                                    &master::OCMetadataManager::RecoverTopologyFailure)),
                                ObjectFailureRecovery>));
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<StreamFailureRecovery>(
                                    &master::SCMetadataManager::RecoverTopologyFailure)),
                                StreamFailureRecovery>));
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<ScaleInData>(
                                    &object_cache::WorkerOCServiceImpl::DrainTopologyScaleInData)),
                                ScaleInData>));
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<ObjectMetaScan>(
                                    &master::ObjectMetaStore::ScanTopologyScope)),
                                ObjectMetaScan>));
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<ObjectFailureCleanup>(
                                    &master::OCMetadataManager::CleanupTopologyFailedMember)),
                                ObjectFailureCleanup>));
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<StreamFailureCleanup>(
                                    &master::SCMetadataManager::CleanupTopologyFailedMember)),
                                StreamFailureCleanup>));
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<ScaleInCleanup>(
                                    &object_cache::WorkerOCServiceImpl::PrepareTopologyScaleInCleanup)),
                                ScaleInCleanup>));
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<FailureDataCleanup>(
                                    &object_cache::WorkerOcServiceClearDataFlow::SubmitTopologyFailureCleanup)),
                                FailureDataCleanup>));
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<ScaleInWorkerDataDrain>(
                                    &worker::WorkerTopologyPhaseCallbacks::OnScaleInDataDrain)),
                                ScaleInWorkerDataDrain>));
}

TEST(TopologyBusinessContractTest, FailureLocalActionsAreInjectedBehindHook)
{
    EXPECT_TRUE((std::is_class_v<worker::IWorkerTopologyObjectCacheActions>));
    EXPECT_TRUE((std::is_same_v<decltype(worker::WorkerTopologyPhaseCallbackDependencies::objectCacheActions),
                                std::shared_ptr<worker::IWorkerTopologyObjectCacheActions>>));
    EXPECT_TRUE((std::is_class_v<worker::IWorkerTopologyMetadataActions>));
    EXPECT_TRUE((std::is_same_v<decltype(worker::WorkerTopologyPhaseCallbackDependencies::metadataActions),
                                std::shared_ptr<worker::IWorkerTopologyMetadataActions>>));
}

class TopologyFailureCallbacksTest : public testing::TestWithParam<bool> {};

TEST_P(TopologyFailureCallbacksTest, BestEffortRunsEveryStepAndReturnsFirstError)
{
    cluster::FakeCoordinationBackend backend;
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    auto rc = cluster::TopologyKeyHelper::Create("topology-failure-callback", keys);
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
    cluster::TopologyRepository repository(backend, *keys);
    cluster::HashAlgorithm algorithm;
    cluster::TopologyState current;
    current.version = 1;
    current.clusterHasInit = true;
    current.members = {
        cluster::Member{ { std::string(16, 'a'), "127.0.0.1:1" }, cluster::MemberState::ACTIVE, { 1, 50 } },
        cluster::Member{ { std::string(16, 'b'), "127.0.0.1:2" }, cluster::MemberState::ACTIVE, { 100, 150 } }
    };
    cluster::TopologyPlan plan;
    cluster::TopologyPlanBuilder builder(algorithm);
    rc = builder.BuildFailureStartOrReplan(current, { current.members.front().identity }, plan);
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
    backend.PutRaw(keys->TopologyTable(), cluster::TopologyKeyHelper::TopologyKey(), plan.next);

    cluster::TopologyReader reader(repository);
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    rc = reader.Read(100, snapshot);
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
    cluster::TopologySnapshotState snapshots;
    cluster::SnapshotUpdateOutcome outcome;
    rc = snapshots.Publish(snapshot, outcome);
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
    cluster::ExpectedDerivedState expected;
    cluster::TopologyTaskMaterializer materializer;
    rc = materializer.BuildExpected(*snapshot, plan, expected);
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
    ASSERT_FALSE(expected.tasks.empty());
    const auto &task = std::get<cluster::TopologyDeleteTask>(expected.tasks.front());
    rc = repository.CreateTaskIfAbsent(expected.tasks.front());
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
    rc = repository.RewriteNotify(task.executorAddress, expected.notifiesByAddress.at(task.executorAddress));
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();

    std::vector<std::string> calls;
    const auto recoveryStatus = GetParam() ? Status(K_RUNTIME_ERROR, "first recovery failure") : Status::OK();
    const auto deviceStatus = GetParam() ? Status(K_TRY_AGAIN, "later device failure") : Status::OK();
    auto metadataActions = std::make_shared<RecordingMetadataActions>(calls, recoveryStatus, deviceStatus);
    auto objectCacheActions = std::make_shared<RecordingObjectCacheActions>(calls);
    worker::WorkerTopologyPhaseCallbacks callbacks({ nullptr, metadataActions, objectCacheActions });
    FailureActionForwarder forwarder(callbacks);
    cluster::CoordinationEventDispatcher dispatcher(8);
    rc = dispatcher.Start();
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
    cluster::TopologyTaskExecutor executor(task.executorAddress, repository, snapshots, forwarder, dispatcher, {});
    rc = executor.Start();
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
    const auto handleRc = executor.HandleNotify(expected.notifiesByAddress.at(task.executorAddress));
    cluster::RuntimeEvent event;
    const auto waitRc = dispatcher.WaitPop(std::chrono::steady_clock::now() + std::chrono::seconds(1), event);
    ASSERT_TRUE(handleRc.IsOk()) << handleRc.ToString();
    ASSERT_TRUE(waitRc.IsOk()) << waitRc.ToString();
    const auto status = std::get<cluster::TopologyCallbackCompletion>(event.payload).status;
    rc = executor.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1));
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();

    EXPECT_EQ(status.GetCode(), GetParam() ? K_RUNTIME_ERROR : K_INVALID);
    const auto message = status.GetMsg();
    if (GetParam()) {
        EXPECT_EQ(message, "first recovery failure");
    } else {
        const std::string primaryMessage = "failure cleanup lacks a failed member address";
        const std::string traceSuffix = primaryMessage + ", traceId: ";
        static const std::regex taskTraceIdPattern(
            R"(task;[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})");
        EXPECT_TRUE(message == primaryMessage
                    || (message.rfind(traceSuffix, 0) == 0
                        && std::regex_match(message.substr(traceSuffix.size()), taskTraceIdPattern)))
            << message;
    }
    EXPECT_EQ(calls, (std::vector<std::string>{ "recover-metadata", "cleanup-metadata", "cleanup-local-data",
                                                "cleanup-device" }));
}

INSTANTIATE_TEST_SUITE_P(WithAndWithoutEarlierFailure, TopologyFailureCallbacksTest, testing::Bool());

TEST(TopologyBusinessContractTest, RemoveMetaCarriesTopologyOperationIdentity)
{
    master::RemoveMetaReqPb request;
    request.set_topology_operation_id("scale-in-operation");
    std::string bytes;
    ASSERT_TRUE(request.SerializeToString(&bytes));

    master::RemoveMetaReqPb decoded;
    ASSERT_TRUE(decoded.ParseFromString(bytes));
    EXPECT_EQ(decoded.topology_operation_id(), "scale-in-operation");
    EXPECT_EQ(master::RemoveMetaReqPb::descriptor()->FindFieldByName("topology_operation_id")->number(), 8);
}

}  // namespace
}  // namespace datasystem::ut
