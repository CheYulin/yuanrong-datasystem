/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Probe worker control-backend state through worker RPC.
 */
#include "datasystem/worker/runtime/worker_control_backend_probe.h"

#include <algorithm>
#include <exception>
#include <limits>
#include <thread>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::worker {
namespace {
struct PendingControlBackendProbe {
    cluster::MemberIdentity peer;
    std::unique_ptr<WorkerControlBackendProbe> client;
    int64_t tag{ -1 };

    PendingControlBackendProbe() = default;
    ~PendingControlBackendProbe()
    {
        Forget();
    }
    PendingControlBackendProbe(const PendingControlBackendProbe &) = delete;
    PendingControlBackendProbe &operator=(const PendingControlBackendProbe &) = delete;
    PendingControlBackendProbe(PendingControlBackendProbe &&other) noexcept
        : peer(std::move(other.peer)), client(std::move(other.client)), tag(std::exchange(other.tag, -1))
    {
    }
    PendingControlBackendProbe &operator=(PendingControlBackendProbe &&) = delete;

    void Forget() noexcept
    {
        if (tag < 0 || client == nullptr) {
            return;
        }
        const auto forgottenTag = std::exchange(tag, -1);
        try {
            auto rc = client->Forget(forgottenTag);
            if (rc.IsError()) {
                LOG(ERROR) << "CLUSTER_BACKEND_PROBE_CLEANUP_FAILED status=" << rc.ToString();
            }
        } catch (const std::exception &error) {
            LOG(ERROR) << "CLUSTER_BACKEND_PROBE_CLEANUP_FAILED reason=exception error=" << error.what();
        } catch (...) {
            LOG(ERROR) << "CLUSTER_BACKEND_PROBE_CLEANUP_FAILED reason=unknown_exception";
        }
    }
};

Status StartControlBackendProbe(const WorkerControlBackendProbeFactory &clientFactory,
                                const cluster::MemberIdentity &peer, std::chrono::steady_clock::time_point deadline,
                                PendingControlBackendProbe &pending)
{
    CHECK_FAIL_RETURN_STATUS(std::chrono::steady_clock::now() < deadline, K_RPC_DEADLINE_EXCEEDED,
                             "cluster-state probe deadline exceeded");
    std::unique_ptr<WorkerControlBackendProbe> client;
    RETURN_IF_NOT_OK(clientFactory(peer, deadline, client));
    CHECK_FAIL_RETURN_STATUS(client != nullptr, K_RUNTIME_ERROR, "cluster-state probe client is null");
    const auto now = std::chrono::steady_clock::now();
    CHECK_FAIL_RETURN_STATUS(now < deadline, K_RPC_DEADLINE_EXCEEDED, "cluster-state probe deadline exceeded");
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
    CHECK_FAIL_RETURN_STATUS(remaining > 0, K_RPC_DEADLINE_EXCEEDED, "cluster-state probe deadline exceeded");
    const auto timeout = static_cast<int32_t>(std::min<int64_t>(std::numeric_limits<int32_t>::max(), remaining));
    int64_t tag = -1;
    RETURN_IF_NOT_OK(client->Start(timeout, tag));
    pending.peer = peer;
    pending.client = std::move(client);
    pending.tag = tag;
    return Status::OK();
}

Status FinishControlBackendProbe(PendingControlBackendProbe &pending, cluster::ControlBackendObservation &observation)
{
    auto rc = pending.client->Finish(pending.peer, pending.tag, RpcRecvFlags::DONTWAIT, observation);
    if (rc.IsOk()) {
        pending.tag = -1;
    }
    return rc;
}
}  // namespace

WorkerControlBackendProbe::WorkerControlBackendProbe(StartFn start, FinishFn finish, ForgetFn forget)
    : start_(std::move(start)), finish_(std::move(finish)), forget_(std::move(forget))
{
}

Status WorkerControlBackendProbe::Start(int32_t timeoutMs, int64_t &tag)
{
    CHECK_FAIL_RETURN_STATUS(start_ != nullptr, K_RUNTIME_ERROR, "cluster-state probe start callback is null");
    return start_(timeoutMs, tag);
}

Status WorkerControlBackendProbe::Finish(const cluster::MemberIdentity &peer, int64_t tag, RpcRecvFlags flags,
                                         cluster::ControlBackendObservation &observation)
{
    CHECK_FAIL_RETURN_STATUS(finish_ != nullptr, K_RUNTIME_ERROR, "cluster-state probe finish callback is null");
    return finish_(peer, tag, flags, observation);
}

Status WorkerControlBackendProbe::Forget(int64_t tag)
{
    CHECK_FAIL_RETURN_STATUS(forget_ != nullptr, K_RUNTIME_ERROR, "cluster-state probe forget callback is null");
    forget_(tag);
    return Status::OK();
}

std::vector<cluster::ControlBackendObservation> ProbeControlBackendPeers(
    const std::vector<cluster::MemberIdentity> &peers, std::chrono::steady_clock::time_point deadline,
    const WorkerControlBackendProbeFactory &clientFactory)
{
    std::vector<PendingControlBackendProbe> pending;
    pending.reserve(peers.size());
    for (const auto &peer : peers) {
        PendingControlBackendProbe probe;
        auto rc = StartControlBackendProbe(clientFactory, peer, deadline, probe);
        if (rc.IsError()) {
            VLOG(1) << "Cluster-state probe start failed for " << peer.address << ": " << rc.ToString();
            continue;
        }
        pending.push_back(std::move(probe));
    }

    std::vector<cluster::ControlBackendObservation> observations;
    observations.reserve(pending.size());
    std::vector<bool> finished(pending.size(), false);
    size_t remaining = pending.size();
    while (remaining > 0 && std::chrono::steady_clock::now() < deadline) {
        bool madeProgress = false;
        for (size_t index = 0; index < pending.size(); ++index) {
            if (finished[index]) {
                continue;
            }
            auto &probe = pending[index];
            cluster::ControlBackendObservation observation;
            auto rc = FinishControlBackendProbe(probe, observation);
            if (rc.GetCode() == K_TRY_AGAIN) {
                continue;
            }
            finished[index] = true;
            --remaining;
            madeProgress = true;
            if (rc.IsError()) {
                VLOG(1) << "Cluster-state probe read failed for " << probe.peer.address << ": " << rc.ToString();
                continue;
            }
            observations.push_back(std::move(observation));
        }
        if (!madeProgress && remaining > 0) {
            const auto now = std::chrono::steady_clock::now();
            if (now < deadline) {
                std::this_thread::sleep_until(std::min(deadline, now + std::chrono::milliseconds(1)));
            }
        }
    }
    return observations;
}
}  // namespace datasystem::worker
