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
 * Description: Shared immutable metadata route for Object-cache unit fixtures.
 */
#ifndef DATASYSTEM_TESTS_UT_WORKER_OBJECT_CACHE_TEST_METADATA_ROUTE_H
#define DATASYSTEM_TESTS_UT_WORKER_OBJECT_CACHE_TEST_METADATA_ROUTE_H

#include <chrono>
#include <memory>
#include <string>
#include <utility>

#include "datasystem/cluster/executor/topology_phase_callbacks.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/cluster/runtime/topology_engine.h"
#include "datasystem/worker/metadata_route_resolver.h"
#include "datasystem/worker/runtime/worker_topology_runtime.h"
#include "ut/cluster/testing/fake_coordinator_service_proxy.h"

namespace datasystem::ut {

inline const worker::MetadataRouteResolver &GetTestMetadataRoute()
{
    static const worker::MetadataRouteResolver route(nullptr, [] {
        worker::MetadataRouteOptions options;
        options.centralizedMode = true;
        options.masterAddress = HostPort("127.0.0.1", 31500);
        return options;
    }());
    return route;
}

class TestTopologyPhaseCallbacks final : public cluster::ITopologyPhaseCallbacks {
public:
    ~TestTopologyPhaseCallbacks() override = default;

    Status OnScaleOut(const cluster::TopologyCallbackContext &) override
    {
        return Status::OK();
    }

    Status OnScaleIn(const cluster::TopologyCallbackContext &) override
    {
        return Status::OK();
    }

    Status OnScaleInDataDrain(const cluster::TopologyCallbackContext &) override
    {
        return Status::OK();
    }

    Status PrepareScaleInCleanup(const cluster::TopologyCallbackContext &,
                                 std::unique_ptr<cluster::TopologyPreparedCleanup> &cleanup) override
    {
        cleanup = std::make_unique<cluster::TopologyPreparedCleanup>(
            [] { return Status::OK(); },
            [](std::chrono::steady_clock::time_point, const cluster::CancellationToken &) { return Status::OK(); });
        return Status::OK();
    }

    Status OnFailure(const cluster::TopologyCallbackContext &) override
    {
        return Status::OK();
    }
};

class ObjectTopologyTestRuntime final {
public:
    ObjectTopologyTestRuntime() = default;
    ~ObjectTopologyTestRuntime()
    {
        (void)Shutdown();
    }

    Status StartReadySingleMember(const HostPort &localAddress)
    {
        CHECK_FAIL_RETURN_STATUS(engine_ != nullptr, K_NOT_READY, "Test topology engine is not initialized");
        cluster::TopologyState topology;
        topology.clusterHasInit = true;
        topology.version = 1;
        topology.members = { cluster::Member{
            { std::string(16, 'a'), localAddress.ToString() }, cluster::MemberState::ACTIVE, { 0, 1'000'000'000 } } };
        std::unique_ptr<cluster::TopologyKeyHelper> keys;
        RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create("", keys));
        std::string encoded;
        RETURN_IF_NOT_OK(cluster::TopologyRepositoryCodec::EncodeTopology(topology, encoded));
        RETURN_IF_NOT_OK(
            proxy_.PutRaw(keys->TopologyTable() + "/" + cluster::TopologyKeyHelper::TopologyKey(), encoded));
        RETURN_IF_NOT_OK(engine_->Start());
        started_ = true;

        std::shared_ptr<const cluster::TopologySnapshot> snapshot;
        RETURN_IF_NOT_OK(engine_->Membership().GetSnapshot(snapshot));
        CHECK_FAIL_RETURN_STATUS(snapshot->CommittedMembers().size() == 1, K_INVALID,
                                 "Expected exactly one committed member in test topology snapshot");
        const cluster::Member *member = nullptr;
        RETURN_IF_NOT_OK(snapshot->FindMemberByAddress(localAddress.ToString(), member));
        CHECK_FAIL_RETURN_STATUS(member != nullptr && member->state == cluster::MemberState::ACTIVE, K_INVALID,
                                 "Local member is not ACTIVE in test topology snapshot");
        return Status::OK();
    }

    Status Init(const HostPort &localAddress)
    {
        if (engine_ != nullptr) {
            return Status::OK();
        }
        cluster::CoordinatorWatchIngress ingress;
        ingress.bind = [](cluster::CoordinatorWatchIngress::Handler) { return Status::OK(); };
        ingress.unbindAndDrain = [](std::chrono::steady_clock::time_point) { return Status::OK(); };
        cluster::TopologyEngine::Builder builder;
        builder.SetClusterName("")
            .SetLocalAddress(localAddress.ToString())
            .UseCoordinator(proxy_, std::move(ingress))
            .SetPhaseCallbacks(callbacks_)
            .SetNodeDeadTimeout(std::chrono::seconds(30));
        RETURN_IF_NOT_OK(builder.Build(engine_));
        runtime_ = std::make_unique<worker::WorkerTopologyRuntimeAdapter>(engine_.get());
        return Status::OK();
    }

    cluster::TopologyEngine *Engine() const
    {
        return engine_.get();
    }

    worker::IWorkerTopologyRuntime *Runtime() const
    {
        return runtime_.get();
    }

    Status Shutdown()
    {
        if (!started_) {
            return Status::OK();
        }
        auto rc = engine_->Shutdown(std::chrono::steady_clock::now() + std::chrono::seconds(3));
        if (rc.IsOk()) {
            started_ = false;
        }
        return rc;
    }

private:
    cluster::testing::FakeCoordinatorServiceProxy proxy_;
    TestTopologyPhaseCallbacks callbacks_;
    std::unique_ptr<cluster::TopologyEngine> engine_;
    std::unique_ptr<worker::WorkerTopologyRuntimeAdapter> runtime_;
    bool started_{ false };
};

inline ObjectTopologyTestRuntime &GetObjectTopologyTestRuntime()
{
    static ObjectTopologyTestRuntime runtime;
    return runtime;
}

}  // namespace datasystem::ut

#endif  // DATASYSTEM_TESTS_UT_WORKER_OBJECT_CACHE_TEST_METADATA_ROUTE_H
