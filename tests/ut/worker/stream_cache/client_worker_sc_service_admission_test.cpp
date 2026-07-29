/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "datasystem/cluster/model/topology_snapshot.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/rpc/rpc_server_stream_base.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/worker/authenticate.h"
#include "datasystem/worker/runtime/worker_runtime_facade.h"
#include "datasystem/worker/worker_health_check.h"
#include "ut/common.h"

#define private public
#include "datasystem/worker/stream_cache/client_worker_sc_service_impl.h"
#include "datasystem/worker/stream_cache/stream_manager.h"
#undef private

namespace datasystem {
namespace ut {
namespace {
using Service = worker::stream_cache::ClientWorkerSCServiceImpl;
using ServiceMode = worker::WorkerServiceMode;
using WorkerMasterSCApi = worker::stream_cache::WorkerMasterSCApi;

constexpr std::chrono::milliseconds WAIT_TIMEOUT{ 3'000 };
constexpr char QUERY_PRODUCERS_PROBE[] = "query producers downstream probe";
constexpr char QUERY_CONSUMERS_PROBE[] = "query consumers downstream probe";
constexpr char CREATE_PRODUCER_PROBE[] = "create producer downstream probe";

class ProbeWorkerMasterSCApi final : public WorkerMasterSCApi {
public:
    ProbeWorkerMasterSCApi() : WorkerMasterSCApi(HostPort("probe-master", 1), nullptr)
    {
    }

    Status Init() override
    {
        ++initAttempts_;
        return Status::OK();
    }

    Status CreateProducer(master::CreateProducerReqPb &, master::CreateProducerRspPb &) override
    {
        ++createProducerAttempts_;
        return Status(StatusCode::K_INVALID, CREATE_PRODUCER_PROBE);
    }

    Status CloseProducer(master::CloseProducerReqPb &, master::CloseProducerRspPb &) override
    {
        return Unsupported();
    }

    Status Subscribe(master::SubscribeReqPb &, master::SubscribeRspPb &) override
    {
        return Unsupported();
    }

    Status CloseConsumer(master::CloseConsumerReqPb &, master::CloseConsumerRspPb &) override
    {
        return Unsupported();
    }

    Status DeleteStream(master::DeleteStreamReqPb &, master::DeleteStreamRspPb &) override
    {
        return Unsupported();
    }

    Status QueryGlobalProducersNum(master::QueryGlobalNumReqPb &, master::QueryGlobalNumRsqPb &) override
    {
        ++queryProducersAttempts_;
        return Status(StatusCode::K_INVALID, QUERY_PRODUCERS_PROBE);
    }

    Status QueryGlobalConsumersNum(master::QueryGlobalNumReqPb &, master::QueryGlobalNumRsqPb &) override
    {
        ++queryConsumersAttempts_;
        return Status(StatusCode::K_INVALID, QUERY_CONSUMERS_PROBE);
    }

    std::string LogPrefix() const override
    {
        return "ProbeWorkerMasterSCApi";
    }

    std::string Address() const override
    {
        return "probe-master:1";
    }

    int InitAttempts() const
    {
        return initAttempts_.load();
    }

    int CreateProducerAttempts() const
    {
        return createProducerAttempts_.load();
    }

    int QueryProducersAttempts() const
    {
        return queryProducersAttempts_.load();
    }

    int QueryConsumersAttempts() const
    {
        return queryConsumersAttempts_.load();
    }

private:
    static Status Unsupported()
    {
        return Status(StatusCode::K_RUNTIME_ERROR, "unexpected downstream call");
    }

    std::atomic<int> initAttempts_{ 0 };
    std::atomic<int> createProducerAttempts_{ 0 };
    std::atomic<int> queryProducersAttempts_{ 0 };
    std::atomic<int> queryConsumersAttempts_{ 0 };
};

class ProbeWorkerMasterSCApiManager final : public worker::WorkerMasterApiManagerBase<WorkerMasterSCApi> {
public:
    ProbeWorkerMasterSCApiManager(HostPort &workerAddr, const worker::MetadataRouteResolver &metadataRoute,
                                  std::shared_ptr<ProbeWorkerMasterSCApi> api)
        : WorkerMasterApiManagerBase<WorkerMasterSCApi>(workerAddr, nullptr, metadataRoute), api_(std::move(api))
    {
    }

    std::shared_ptr<WorkerMasterSCApi> CreateWorkerMasterApi(const HostPort &) override
    {
        return api_;
    }

    Status GetWorkerMasterApi(const HostPort &, std::shared_ptr<WorkerMasterSCApi> &api) override
    {
        ++resolutionAttempts_;
        api = api_;
        return Status::OK();
    }

    int ResolutionAttempts() const
    {
        return resolutionAttempts_.load();
    }

private:
    std::shared_ptr<ProbeWorkerMasterSCApi> api_;
    std::atomic<int> resolutionAttempts_{ 0 };
};

struct DownstreamProbe {
    std::shared_ptr<ProbeWorkerMasterSCApi> api;
    std::shared_ptr<ProbeWorkerMasterSCApiManager> manager;
};

bool WaitForInjectCount(const std::string &name, uint64_t expectedCount)
{
    const auto deadline = std::chrono::steady_clock::now() + WAIT_TIMEOUT;
    while (std::chrono::steady_clock::now() < deadline) {
        if (inject::GetExecuteCount(name) >= expectedCount) {
            return true;
        }
        std::this_thread::yield();
    }
    return inject::GetExecuteCount(name) >= expectedCount;
}

template <typename W, typename R>
class ProbeUnary : public ServerUnaryWriterReader<W, R> {
public:
    explicit ProbeUnary(R req, Status readStatus = Status::OK())
        : ServerUnaryWriterReader<W, R>(std::unique_ptr<ServerUnaryWriterReaderImpl<W, R>>(nullptr)),
          req_(std::move(req)),
          readStatus_(std::move(readStatus)),
          future_(promise_.get_future())
    {
    }

    Status Read(R &pb) override
    {
        ++readAttempts_;
        pb = req_;
        return readStatus_;
    }

    Status Write(const W &pb) override
    {
        Complete(pb, Status::OK());
        return Status::OK();
    }

    Status SendStatus(const Status &rc) override
    {
        Complete(W(), rc);
        return Status::OK();
    }

    int ReadAttempts() const
    {
        return readAttempts_.load();
    }

    int ResponseAttempts() const
    {
        return responseAttempts_.load();
    }

    std::future<std::pair<W, Status>> &Future()
    {
        return future_;
    }

private:
    void Complete(const W &rsp, const Status &rc)
    {
        if (++responseAttempts_ == 1) {
            promise_.set_value({ rsp, rc });
        }
    }

    R req_;
    Status readStatus_;
    std::promise<std::pair<W, Status>> promise_;
    std::future<std::pair<W, Status>> future_;
    std::atomic<int> readAttempts_{ 0 };
    std::atomic<int> responseAttempts_{ 0 };
};

void MarkClosed(worker::WorkerRuntimeFacade &runtime, ServiceMode mode)
{
    if (mode == ServiceMode::LOCAL_ISOLATED) {
        runtime.MarkLocalIsolated(worker::WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "local isolation");
        return;
    }
    runtime.MarkRecovering(worker::WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE, "recovering");
}

void MarkRunning(worker::WorkerRuntimeFacade &runtime)
{
    worker::WorkerRecoveryEvidenceBuilder builder;
    const auto report = builder.MarkMembershipReady("membership ready")
                            .MarkTopologyReady("topology ready")
                            .MarkMetadataReady("metadata ready")
                            .MarkSlotReady("slot ready")
                            .MarkOwnershipReady("ownership ready")
                            .MarkResourceReady("resource ready")
                            .BuildReport("stream admission recovered");
    ASSERT_TRUE(runtime.TryCompleteRecovery(report.evidence, report.detail));
}

CreateProducerReqPb ProducerReq(const std::string &streamName)
{
    CreateProducerReqPb req;
    req.set_client_id("client");
    req.set_stream_name(streamName);
    req.set_producer_id("producer");
    req.set_page_size(1'024);
    req.set_max_stream_size(4'096);
    req.set_stream_mode(StreamMode::MPSC);
    return req;
}

SubscribeReqPb SubscribeReq(const std::string &streamName)
{
    SubscribeReqPb req;
    req.set_client_id("client");
    req.set_stream_name(streamName);
    req.set_consumer_id("consumer");
    req.mutable_subscription_config()->set_subscription_name("subscription");
    req.mutable_subscription_config()->set_subscription_type(SubscriptionTypePb::STREAM_PB);
    return req;
}

CreateShmPageReqPb PageReq(const std::string &streamName)
{
    CreateShmPageReqPb req;
    req.set_client_id("client");
    req.set_stream_name(streamName);
    req.set_producer_id("producer");
    req.set_sub_timeout(30'000);
    return req;
}

CreateLobPageReqPb LobReq(const std::string &streamName)
{
    CreateLobPageReqPb req;
    req.set_client_id("client");
    req.set_stream_name(streamName);
    req.set_producer_id("producer");
    req.set_page_size(1'024);
    req.set_sub_timeout(30'000);
    return req;
}
}  // namespace

struct CreateShmPageInvoker {
    Status operator()(Service *service,
                      const std::shared_ptr<ServerUnaryWriterReader<CreateShmPageRspPb, CreateShmPageReqPb>> &rpc) const
    {
        return service->CreateShmPage(rpc);
    }
};

struct AllocBigShmMemoryInvoker {
    Status operator()(Service *service,
                      const std::shared_ptr<ServerUnaryWriterReader<CreateLobPageRspPb, CreateLobPageReqPb>> &rpc) const
    {
        return service->AllocBigShmMemory(rpc);
    }
};

template <typename BlockedRequest>
struct AllocationCallbackProbe {
    std::shared_ptr<std::atomic<uint64_t>> attempts;

    Status operator()(BlockedRequest *blockedReq) const
    {
        attempts->fetch_add(1, std::memory_order_relaxed);
        return blockedReq->Write();
    }
};

class ClientWorkerSCServiceAdmissionTest : public CommonTest {
public:
    void TearDown() override
    {
        SetTopologyServingAdmission(true);
        SetUnhealthy();
        CommonTest::TearDown();
    }

protected:
    std::shared_ptr<Service> MakeService()
    {
        return std::make_shared<Service>(HostPort("127.0.0.1", 31'500), HostPort("127.0.0.1", 31'501), nullptr,
                                         akSkManager_, nullptr, metadataRoute_, membership_);
    }

    std::shared_ptr<Service> MakeInitializedService()
    {
        auto service = MakeService();
        auto rc = SetHealthProbe();
        if (rc.IsError()) {
            ADD_FAILURE() << rc.ToString();
            return nullptr;
        }
        SetTopologyServingAdmission(true);
        rc = PublishTopology();
        if (rc.IsError()) {
            ADD_FAILURE() << rc.ToString();
            return nullptr;
        }
        rc = service->Init();
        if (rc.IsError()) {
            ADD_FAILURE() << rc.ToString();
            return nullptr;
        }
        return service;
    }

    DownstreamProbe InstallDownstreamProbe(Service &service)
    {
        DownstreamProbe probe;
        probe.api = std::make_shared<ProbeWorkerMasterSCApi>();
        probe.manager =
            std::make_shared<ProbeWorkerMasterSCApiManager>(service.localWorkerAddress_, metadataRoute_, probe.api);
        service.workerMasterApiManager_ = probe.manager;
        return probe;
    }

    void CreateLocalStream(Service &service, const std::string &streamName)
    {
        worker::stream_cache::StreamManagerMap::accessor accessor;
        Optional<StreamFields> fields(16 * 1'024, 1'024, false, 0, false, 1'024, StreamMode::MPSC);
        DS_ASSERT_OK(service.CreateStreamManagerImpl(streamName, fields, accessor));
    }

    template <typename W, typename R, typename BlockedRequest>
    void ExpectQueuedRequestRejected(
        Service &service, worker::WorkerRuntimeFacade &runtime, const std::string &streamName, R req,
        const std::function<Status(Service *, const std::shared_ptr<ServerUnaryWriterReader<W, R>> &)> &invoke,
        ServiceMode closedMode)
    {
        worker::stream_cache::StreamManagerMap::const_accessor accessor;
        DS_ASSERT_OK(service.GetStreamManager(streamName, accessor));
        auto streamManager = accessor->second;
        auto queue = streamManager->GetExclusivePageQueue();
        const auto pagesCreated = queue->GetNumPagesCreated();
        const auto pagesReleased = queue->GetNumPagesReleased();
        const auto bigPagesCreated = queue->GetNumBigPagesCreated();
        const auto bigPagesReleased = queue->GetNumBigPagesReleased();
        const auto sharedMemoryUsed = queue->GetSharedMemoryUsed();
        const auto ackCount = inject::GetExecuteCount("StreamManager.AckCursors.delay");

        const std::string barrier = "HandleBlockedRequestImpl.sleep";
        DS_ASSERT_OK(inject::Set(barrier, "pause()"));
        Raii clearBarrier([&barrier] { (void)inject::Clear(barrier); });
        const auto barrierCount = inject::GetExecuteCount(barrier);

        auto rpc = std::make_shared<ProbeUnary<W, R>>(std::move(req));
        DS_ASSERT_OK(invoke(&service, rpc));
        ASSERT_TRUE(WaitForInjectCount(barrier, barrierCount + 1));
        auto allocationAttempts = std::make_shared<std::atomic<uint64_t>>(0);
        if constexpr (std::is_same_v<W, CreateLobPageRspPb>) {
            auto request = streamManager->lobBlockedList_.blockedList_.at("producer");
            request->callBackFn_ = AllocationCallbackProbe<BlockedRequest>{ allocationAttempts };
        } else {
            auto request = streamManager->dataBlockedList_.blockedList_.at("producer");
            request->callBackFn_ = AllocationCallbackProbe<BlockedRequest>{ allocationAttempts };
        }
        MarkClosed(runtime, closedMode);
        DS_ASSERT_OK(inject::Clear(barrier));

        ASSERT_EQ(rpc->Future().wait_for(WAIT_TIMEOUT), std::future_status::ready);
        auto result = rpc->Future().get();
        EXPECT_EQ(result.second.GetCode(), StatusCode::K_NOT_READY);
        EXPECT_EQ(rpc->ReadAttempts(), 1);
        EXPECT_EQ(rpc->ResponseAttempts(), 1);
        EXPECT_EQ(queue->GetNumPagesCreated(), pagesCreated);
        EXPECT_EQ(queue->GetNumPagesReleased(), pagesReleased);
        EXPECT_EQ(queue->GetNumBigPagesCreated(), bigPagesCreated);
        EXPECT_EQ(queue->GetNumBigPagesReleased(), bigPagesReleased);
        EXPECT_EQ(queue->GetSharedMemoryUsed(), sharedMemoryUsed);
        EXPECT_EQ(inject::GetExecuteCount("StreamManager.AckCursors.delay"), ackCount);
        EXPECT_EQ(allocationAttempts->load(std::memory_order_relaxed), 0);

        std::shared_ptr<BlockedRequest> pending;
        EXPECT_EQ(streamManager->GetBlockedCreateRequest(pending).GetCode(), StatusCode::K_TRY_AGAIN);
        EXPECT_EQ(pending, nullptr);
        if constexpr (std::is_same_v<W, CreateLobPageRspPb>) {
            EXPECT_TRUE(streamManager->lobBlockedList_.blockedList_.empty());
            EXPECT_TRUE(streamManager->lobBlockedList_.processingBlockedList_.empty());
            EXPECT_TRUE(streamManager->lobBlockedList_.queue_.empty());
        } else {
            EXPECT_TRUE(streamManager->dataBlockedList_.blockedList_.empty());
            EXPECT_TRUE(streamManager->dataBlockedList_.processingBlockedList_.empty());
            EXPECT_TRUE(streamManager->dataBlockedList_.queue_.empty());
        }
    }

    template <typename W, typename R, typename BlockedRequest>
    void ExpectDirectUnblockRejected(
        Service &service, worker::WorkerRuntimeFacade &runtime, const std::string &streamName, R req,
        const std::function<Status(Service *, const std::shared_ptr<ServerUnaryWriterReader<W, R>> &)> &invoke,
        ServiceMode closedMode)
    {
        worker::stream_cache::StreamManagerMap::const_accessor accessor;
        DS_ASSERT_OK(service.GetStreamManager(streamName, accessor));
        auto streamManager = accessor->second;
        auto queue = streamManager->GetExclusivePageQueue();
        const auto pagesCreated = queue->GetNumPagesCreated();
        const auto pagesReleased = queue->GetNumPagesReleased();
        const auto bigPagesCreated = queue->GetNumBigPagesCreated();
        const auto bigPagesReleased = queue->GetNumBigPagesReleased();
        const auto sharedMemoryUsed = queue->GetSharedMemoryUsed();
        const auto ackCount = inject::GetExecuteCount("StreamManager.AckCursors.delay");

        const std::string barrier = "HandleBlockedRequestImpl.sleep";
        DS_ASSERT_OK(inject::Set(barrier, "pause()"));
        Raii clearBarrier([&barrier] { (void)inject::Clear(barrier); });
        const auto barrierCount = inject::GetExecuteCount(barrier);

        auto rpc = std::make_shared<ProbeUnary<W, R>>(std::move(req));
        DS_ASSERT_OK(invoke(&service, rpc));
        ASSERT_TRUE(WaitForInjectCount(barrier, barrierCount + 1));
        auto allocationAttempts = std::make_shared<std::atomic<uint64_t>>(0);
        if constexpr (std::is_same_v<W, CreateLobPageRspPb>) {
            auto request = streamManager->lobBlockedList_.blockedList_.at("producer");
            request->callBackFn_ = AllocationCallbackProbe<BlockedRequest>{ allocationAttempts };
        } else {
            auto request = streamManager->dataBlockedList_.blockedList_.at("producer");
            request->callBackFn_ = AllocationCallbackProbe<BlockedRequest>{ allocationAttempts };
        }
        MarkClosed(runtime, closedMode);
        DS_ASSERT_OK(streamManager->UnblockCreators());
        DS_ASSERT_OK(inject::Clear(barrier));

        ASSERT_EQ(rpc->Future().wait_for(WAIT_TIMEOUT), std::future_status::ready);
        auto result = rpc->Future().get();
        EXPECT_EQ(result.second.GetCode(), StatusCode::K_NOT_READY);
        EXPECT_EQ(rpc->ReadAttempts(), 1);
        EXPECT_EQ(rpc->ResponseAttempts(), 1);
        EXPECT_EQ(queue->GetNumPagesCreated(), pagesCreated);
        EXPECT_EQ(queue->GetNumPagesReleased(), pagesReleased);
        EXPECT_EQ(queue->GetNumBigPagesCreated(), bigPagesCreated);
        EXPECT_EQ(queue->GetNumBigPagesReleased(), bigPagesReleased);
        EXPECT_EQ(queue->GetSharedMemoryUsed(), sharedMemoryUsed);
        EXPECT_EQ(inject::GetExecuteCount("StreamManager.AckCursors.delay"), ackCount);
        EXPECT_EQ(allocationAttempts->load(std::memory_order_relaxed), 0);

        std::shared_ptr<BlockedRequest> pending;
        EXPECT_EQ(streamManager->GetBlockedCreateRequest(pending).GetCode(), StatusCode::K_TRY_AGAIN);
        EXPECT_EQ(pending, nullptr);
        if constexpr (std::is_same_v<W, CreateLobPageRspPb>) {
            EXPECT_TRUE(streamManager->lobBlockedList_.blockedList_.empty());
            EXPECT_TRUE(streamManager->lobBlockedList_.processingBlockedList_.empty());
            EXPECT_TRUE(streamManager->lobBlockedList_.queue_.empty());
        } else {
            EXPECT_TRUE(streamManager->dataBlockedList_.blockedList_.empty());
            EXPECT_TRUE(streamManager->dataBlockedList_.processingBlockedList_.empty());
            EXPECT_TRUE(streamManager->dataBlockedList_.queue_.empty());
        }
    }

private:
    static worker::MetadataRouteOptions MakeRouteOptions()
    {
        worker::MetadataRouteOptions options;
        options.centralizedMode = true;
        options.masterAddress = HostPort("127.0.0.1", 31'501);
        return options;
    }

    Status PublishTopology()
    {
        cluster::TopologyState topology;
        topology.version = 1;
        topology.members = { cluster::Member{
            { std::string(16, 'a'), "127.0.0.1:31501" }, cluster::MemberState::ACTIVE, { 1 } } };
        std::shared_ptr<const cluster::TopologySnapshot> snapshot;
        cluster::SnapshotUpdateOutcome outcome;
        RETURN_IF_NOT_OK(cluster::TopologySnapshot::Create(topology, 1, std::string(64, 'a'), snapshot));
        return snapshots_.Publish(std::move(snapshot), outcome);
    }

protected:
    std::shared_ptr<AkSkManager> akSkManager_{ std::make_shared<AkSkManager>(0) };
    cluster::TopologySnapshotState snapshots_;
    cluster::MembershipEndpointView membership_{ snapshots_ };
    worker::MetadataRouteOptions routeOptions_{ MakeRouteOptions() };
    worker::MetadataRouteResolver metadataRoute_{ nullptr, routeOptions_ };
};

TEST_F(ClientWorkerSCServiceAdmissionTest, RejectsWriteAndRegistrationEntriesBeforeRequestRead)
{
    for (auto mode : { ServiceMode::LOCAL_ISOLATED, ServiceMode::RECOVERING }) {
        worker::WorkerRuntimeFacade runtime;
        MarkClosed(runtime, mode);
        auto service = MakeService();
        service->SetRuntimeFacade(&runtime);
        DS_ASSERT_OK(SetHealthProbe());
        SetTopologyServingAdmission(true);

        auto producerRpc =
            std::make_shared<ProbeUnary<CreateProducerRspPb, CreateProducerReqPb>>(ProducerReq("producer-stream"));
        auto subscribeRpc =
            std::make_shared<ProbeUnary<SubscribeRspPb, SubscribeReqPb>>(SubscribeReq("subscribe-stream"));
        auto pageRpc = std::make_shared<ProbeUnary<CreateShmPageRspPb, CreateShmPageReqPb>>(PageReq("page-stream"));
        auto lobRpc = std::make_shared<ProbeUnary<CreateLobPageRspPb, CreateLobPageReqPb>>(LobReq("lob-stream"));

        EXPECT_EQ(service->CreateProducer(producerRpc).GetCode(), StatusCode::K_NOT_READY);
        EXPECT_EQ(service->Subscribe(subscribeRpc).GetCode(), StatusCode::K_NOT_READY);
        EXPECT_EQ(service->CreateShmPage(pageRpc).GetCode(), StatusCode::K_NOT_READY);
        EXPECT_EQ(service->AllocBigShmMemory(lobRpc).GetCode(), StatusCode::K_NOT_READY);
        EXPECT_EQ(producerRpc->ReadAttempts(), 0);
        EXPECT_EQ(subscribeRpc->ReadAttempts(), 0);
        EXPECT_EQ(pageRpc->ReadAttempts(), 0);
        EXPECT_EQ(lobRpc->ReadAttempts(), 0);
        EXPECT_EQ(producerRpc->ResponseAttempts(), 0);
        EXPECT_EQ(subscribeRpc->ResponseAttempts(), 0);
        EXPECT_EQ(pageRpc->ResponseAttempts(), 0);
        EXPECT_EQ(lobRpc->ResponseAttempts(), 0);

        std::vector<std::string> registrations;
        EXPECT_EQ(service->GetPubSubForClientStream("client", "producer-stream", registrations).GetCode(),
                  StatusCode::K_NOT_FOUND);
        EXPECT_EQ(service->GetPubSubForClientStream("client", "subscribe-stream", registrations).GetCode(),
                  StatusCode::K_NOT_FOUND);
        worker::stream_cache::StreamManagerMap::const_accessor accessor;
        EXPECT_EQ(service->GetStreamManager("page-stream", accessor).GetCode(), StatusCode::K_SC_STREAM_NOT_FOUND);
        EXPECT_EQ(service->GetStreamManager("lob-stream", accessor).GetCode(), StatusCode::K_SC_STREAM_NOT_FOUND);
    }
}

TEST_F(ClientWorkerSCServiceAdmissionTest, ScaleFaultOverlayRejectsStreamWritesDuringDrainingIsolation)
{
    worker::WorkerRuntimeFacade runtime;
    runtime.MarkDraining("scale-in drain started");
    runtime.MarkLocalIsolated(worker::WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION,
                              "scale-in drain overlapped local isolation");
    auto service = MakeService();
    service->SetRuntimeFacade(&runtime);
    DS_ASSERT_OK(SetHealthProbe());
    SetTopologyServingAdmission(true);

    auto rpc = std::make_shared<ProbeUnary<CreateProducerRspPb, CreateProducerReqPb>>(ProducerReq("draining-stream"));
    auto rc = service->CreateProducer(rpc);

    EXPECT_EQ(rc.GetCode(), StatusCode::K_NOT_READY);
    EXPECT_EQ(rpc->ReadAttempts(), 0);
    EXPECT_NE(rc.GetMsg().find("NORMAL_WRITE"), std::string::npos);
    EXPECT_NE(rc.GetMsg().find("DRAINING"), std::string::npos);
    EXPECT_EQ(runtime.GetSnapshot().mode, ServiceMode::DRAINING);
}

TEST_F(ClientWorkerSCServiceAdmissionTest, QueuedMemoryRequestsRecheckAdmissionBeforeAckReclaimOrAllocation)
{
    bool savedSkipAuthenticate = FLAGS_skip_authenticate;
    FLAGS_skip_authenticate = true;
    Raii restoreSkipAuthenticate([savedSkipAuthenticate] { FLAGS_skip_authenticate = savedSkipAuthenticate; });
    DS_ASSERT_OK(inject::Set("worker.CheckHadEnoughMem", "return(K_OUT_OF_MEMORY)"));
    DS_ASSERT_OK(inject::Set("StreamManager.AckCursors.delay", "call()"));
    Raii clearSideEffectProbes([] {
        (void)inject::Clear("worker.CheckHadEnoughMem");
        (void)inject::Clear("StreamManager.AckCursors.delay");
    });

    for (auto closedMode : { ServiceMode::LOCAL_ISOLATED, ServiceMode::RECOVERING }) {
        for (bool bigElement : { false, true }) {
            worker::WorkerRuntimeFacade runtime;
            MarkClosed(runtime, ServiceMode::RECOVERING);
            MarkRunning(runtime);
            auto service = MakeInitializedService();
            ASSERT_NE(service, nullptr);
            service->SetRuntimeFacade(&runtime);
            const std::string streamName = bigElement ? "queued-lob" : "queued-page";
            CreateLocalStream(*service, streamName);

            if (bigElement) {
                using BlockedRequest =
                    worker::stream_cache::BlockedCreateRequest<CreateLobPageRspPb, CreateLobPageReqPb>;
                ExpectQueuedRequestRejected<CreateLobPageRspPb, CreateLobPageReqPb, BlockedRequest>(
                    *service, runtime, streamName, LobReq(streamName), AllocBigShmMemoryInvoker(), closedMode);
            } else {
                using BlockedRequest =
                    worker::stream_cache::BlockedCreateRequest<CreateShmPageRspPb, CreateShmPageReqPb>;
                ExpectQueuedRequestRejected<CreateShmPageRspPb, CreateShmPageReqPb, BlockedRequest>(
                    *service, runtime, streamName, PageReq(streamName), CreateShmPageInvoker(), closedMode);
            }
        }
    }
}

TEST_F(ClientWorkerSCServiceAdmissionTest, DirectUnblockRechecksAdmissionBeforeAckReclaimOrAllocation)
{
    bool savedSkipAuthenticate = FLAGS_skip_authenticate;
    FLAGS_skip_authenticate = true;
    Raii restoreSkipAuthenticate([savedSkipAuthenticate] { FLAGS_skip_authenticate = savedSkipAuthenticate; });
    DS_ASSERT_OK(inject::Set("StreamManager.AckCursors.delay", "call()"));
    Raii clearSideEffectProbe([] { (void)inject::Clear("StreamManager.AckCursors.delay"); });

    for (auto closedMode : { ServiceMode::LOCAL_ISOLATED, ServiceMode::RECOVERING }) {
        for (bool bigElement : { false, true }) {
            worker::WorkerRuntimeFacade runtime;
            MarkClosed(runtime, ServiceMode::RECOVERING);
            MarkRunning(runtime);
            auto service = MakeInitializedService();
            ASSERT_NE(service, nullptr);
            service->SetRuntimeFacade(&runtime);
            const std::string streamName = bigElement ? "direct-lob" : "direct-page";
            CreateLocalStream(*service, streamName);

            if (bigElement) {
                using BlockedRequest =
                    worker::stream_cache::BlockedCreateRequest<CreateLobPageRspPb, CreateLobPageReqPb>;
                ExpectDirectUnblockRejected<CreateLobPageRspPb, CreateLobPageReqPb, BlockedRequest>(
                    *service, runtime, streamName, LobReq(streamName), AllocBigShmMemoryInvoker(), closedMode);
            } else {
                using BlockedRequest =
                    worker::stream_cache::BlockedCreateRequest<CreateShmPageRspPb, CreateShmPageReqPb>;
                ExpectDirectUnblockRejected<CreateShmPageRspPb, CreateShmPageReqPb, BlockedRequest>(
                    *service, runtime, streamName, PageReq(streamName), CreateShmPageInvoker(), closedMode);
            }
        }
    }
}

TEST_F(ClientWorkerSCServiceAdmissionTest, TransitionAfterTaskCheckWinsBeforeCreatePageSideEffectsCommit)
{
    bool savedSkipAuthenticate = FLAGS_skip_authenticate;
    FLAGS_skip_authenticate = true;
    Raii restoreSkipAuthenticate([savedSkipAuthenticate] { FLAGS_skip_authenticate = savedSkipAuthenticate; });
    DS_ASSERT_OK(inject::Set("worker.CheckHadEnoughMem", "pause()"));
    DS_ASSERT_OK(inject::Set("StreamManager.AckCursors.delay", "call()"));
    Raii clearProbes([] {
        (void)inject::Clear("worker.CheckHadEnoughMem");
        (void)inject::Clear("StreamManager.AckCursors.delay");
    });

    for (auto closedMode : { ServiceMode::LOCAL_ISOLATED, ServiceMode::RECOVERING }) {
        worker::WorkerRuntimeFacade runtime;
        MarkClosed(runtime, ServiceMode::RECOVERING);
        MarkRunning(runtime);
        auto service = MakeInitializedService();
        ASSERT_NE(service, nullptr);
        service->SetRuntimeFacade(&runtime);
        const std::string streamName = "checked-page";
        CreateLocalStream(*service, streamName);

        worker::stream_cache::StreamManagerMap::const_accessor accessor;
        DS_ASSERT_OK(service->GetStreamManager(streamName, accessor));
        auto streamManager = accessor->second;
        auto queue = streamManager->GetExclusivePageQueue();
        const auto pagesCreated = queue->GetNumPagesCreated();
        const auto pagesReleased = queue->GetNumPagesReleased();
        const auto sharedMemoryUsed = queue->GetSharedMemoryUsed();
        const auto ackCount = inject::GetExecuteCount("StreamManager.AckCursors.delay");
        const auto checkCount = inject::GetExecuteCount("worker.CheckHadEnoughMem");

        auto rpc = std::make_shared<ProbeUnary<CreateShmPageRspPb, CreateShmPageReqPb>>(PageReq(streamName));
        DS_ASSERT_OK(service->CreateShmPage(rpc));
        ASSERT_TRUE(WaitForInjectCount("worker.CheckHadEnoughMem", checkCount + 1));
        auto allocationAttempts = std::make_shared<std::atomic<uint64_t>>(0);
        auto request = streamManager->dataBlockedList_.processingBlockedList_.at("producer");
        using BlockedRequest = worker::stream_cache::BlockedCreateRequest<CreateShmPageRspPb, CreateShmPageReqPb>;
        request->callBackFn_ = AllocationCallbackProbe<BlockedRequest>{ allocationAttempts };
        MarkClosed(runtime, closedMode);
        DS_ASSERT_OK(inject::Clear("worker.CheckHadEnoughMem"));

        ASSERT_EQ(rpc->Future().wait_for(WAIT_TIMEOUT), std::future_status::ready);
        auto result = rpc->Future().get();
        EXPECT_EQ(result.second.GetCode(), StatusCode::K_NOT_READY);
        EXPECT_EQ(rpc->ReadAttempts(), 1);
        EXPECT_EQ(rpc->ResponseAttempts(), 1);
        EXPECT_EQ(queue->GetNumPagesCreated(), pagesCreated);
        EXPECT_EQ(queue->GetNumPagesReleased(), pagesReleased);
        EXPECT_EQ(queue->GetSharedMemoryUsed(), sharedMemoryUsed);
        EXPECT_EQ(inject::GetExecuteCount("StreamManager.AckCursors.delay"), ackCount);
        EXPECT_EQ(allocationAttempts->load(std::memory_order_relaxed), 0);
        EXPECT_TRUE(streamManager->dataBlockedList_.blockedList_.empty());
        EXPECT_TRUE(streamManager->dataBlockedList_.processingBlockedList_.empty());
        EXPECT_TRUE(streamManager->dataBlockedList_.queue_.empty());
        DS_ASSERT_OK(inject::Set("worker.CheckHadEnoughMem", "pause()"));
    }
}

TEST_F(ClientWorkerSCServiceAdmissionTest, ReadCursorAndQueriesContinueToDownstreamProbeWhenAdmissionIsClosed)
{
    bool savedSkipAuthenticate = FLAGS_skip_authenticate;
    FLAGS_skip_authenticate = true;
    Raii restoreSkipAuthenticate([savedSkipAuthenticate] { FLAGS_skip_authenticate = savedSkipAuthenticate; });

    for (auto mode : { ServiceMode::LOCAL_ISOLATED, ServiceMode::RECOVERING }) {
        worker::WorkerRuntimeFacade runtime;
        MarkClosed(runtime, mode);
        auto service = MakeService();
        service->SetRuntimeFacade(&runtime);
        auto downstream = InstallDownstreamProbe(*service);
        DS_ASSERT_OK(SetHealthProbe());
        SetTopologyServingAdmission(true);

        GetDataPageReqPb pageReq;
        pageReq.set_client_id("client");
        pageReq.set_stream_name("read-stream");
        pageReq.set_subscription_name("subscription");
        Status downstreamProbe(StatusCode::K_INVALID, "get data page downstream probe");
        auto pageRpc = std::make_shared<ProbeUnary<GetDataPageRspPb, GetDataPageReqPb>>(pageReq, downstreamProbe);
        auto pageRc = service->GetDataPage(pageRpc);
        EXPECT_EQ(pageRc.GetCode(), StatusCode::K_INVALID);
        EXPECT_EQ(pageRc.GetMsg(), downstreamProbe.GetMsg());
        EXPECT_EQ(pageRpc->ReadAttempts(), 1);
        EXPECT_EQ(pageRpc->ResponseAttempts(), 0);

        LastAppendCursorReqPb cursorReq;
        cursorReq.set_client_id("client");
        cursorReq.set_stream_name("cursor-stream");
        LastAppendCursorRspPb cursorRsp;
        auto cursorRc = service->GetLastAppendCursor(cursorReq, cursorRsp);
        EXPECT_EQ(cursorRc.GetCode(), StatusCode::K_SC_STREAM_NOT_FOUND);

        QueryGlobalNumReqPb queryReq;
        queryReq.set_client_id("client");
        queryReq.set_stream_name("query-stream");
        QueryGlobalNumRsqPb queryRsp;
        Status producersRc;
        {
            GetRequestContext()->scTimeoutDuration.Init(WAIT_TIMEOUT.count());
            Raii resetTimeout([] { GetRequestContext()->scTimeoutDuration.Reset(); });
            producersRc = service->QueryGlobalProducersNum(queryReq, queryRsp);
        }
        Status consumersRc;
        {
            GetRequestContext()->scTimeoutDuration.Init(WAIT_TIMEOUT.count());
            Raii resetTimeout([] { GetRequestContext()->scTimeoutDuration.Reset(); });
            consumersRc = service->QueryGlobalConsumersNum(queryReq, queryRsp);
        }
        EXPECT_EQ(producersRc.GetCode(), StatusCode::K_INVALID);
        EXPECT_EQ(producersRc.GetMsg(), QUERY_PRODUCERS_PROBE);
        EXPECT_EQ(consumersRc.GetCode(), StatusCode::K_INVALID);
        EXPECT_EQ(consumersRc.GetMsg(), QUERY_CONSUMERS_PROBE);
        EXPECT_EQ(downstream.manager->ResolutionAttempts(), 2);
        EXPECT_EQ(downstream.api->InitAttempts(), 0);
        EXPECT_EQ(downstream.api->QueryProducersAttempts(), 1);
        EXPECT_EQ(downstream.api->QueryConsumersAttempts(), 1);
    }
}

TEST_F(ClientWorkerSCServiceAdmissionTest, RecoveredWriteEntryReachesDownstreamAndRespondsExactlyOnce)
{
    bool savedSkipAuthenticate = FLAGS_skip_authenticate;
    FLAGS_skip_authenticate = true;
    Raii restoreSkipAuthenticate([savedSkipAuthenticate] { FLAGS_skip_authenticate = savedSkipAuthenticate; });

    worker::WorkerRuntimeFacade runtime;
    MarkClosed(runtime, ServiceMode::RECOVERING);
    auto service = MakeInitializedService();
    ASSERT_NE(service, nullptr);
    service->SetRuntimeFacade(&runtime);
    auto downstream = InstallDownstreamProbe(*service);
    MarkRunning(runtime);

    const std::string probe = "ClientWorkerSCServiceImpl.CreateProducerImpl.sleep";
    DS_ASSERT_OK(inject::Set(probe, "call()"));
    Raii clearProbe([&probe] { (void)inject::Clear(probe); });
    const auto probeCount = inject::GetExecuteCount(probe);

    auto rpc = std::make_shared<ProbeUnary<CreateProducerRspPb, CreateProducerReqPb>>(ProducerReq("recovered-stream"));
    DS_ASSERT_OK(service->CreateProducer(rpc));
    ASSERT_EQ(rpc->Future().wait_for(WAIT_TIMEOUT), std::future_status::ready);
    auto result = rpc->Future().get();

    EXPECT_EQ(result.second.GetCode(), StatusCode::K_INVALID);
    EXPECT_EQ(result.second.GetMsg(), CREATE_PRODUCER_PROBE);
    EXPECT_EQ(inject::GetExecuteCount(probe), probeCount + 1);
    EXPECT_EQ(rpc->ReadAttempts(), 1);
    EXPECT_EQ(rpc->ResponseAttempts(), 1);
    EXPECT_EQ(downstream.manager->ResolutionAttempts(), 1);
    EXPECT_EQ(downstream.api->InitAttempts(), 0);
    EXPECT_EQ(downstream.api->CreateProducerAttempts(), 1);
}
}  // namespace ut
}  // namespace datasystem
