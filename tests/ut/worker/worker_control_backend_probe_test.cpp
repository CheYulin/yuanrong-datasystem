/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker control backend peer probe tests.
 */
#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "datasystem/worker/runtime/worker_control_backend_probe.h"

namespace datasystem::worker {
namespace {
cluster::MemberIdentity Peer(const std::string &address)
{
    cluster::MemberIdentity peer;
    peer.id = address;
    peer.address = address;
    return peer;
}

cluster::ControlBackendObservation AvailableObservation(const cluster::MemberIdentity &peer)
{
    cluster::ControlBackendObservation observation;
    observation.reporter = peer;
    observation.state = cluster::ControlBackendState::AVAILABLE;
    observation.topologyVersion = 1;
    observation.topologyRevision = 2;
    observation.topologyDigest = "digest";
    observation.observedAt = std::chrono::steady_clock::now();
    return observation;
}

void WaitUntil(std::chrono::steady_clock::time_point releaseAt)
{
    std::condition_variable condition;
    std::mutex mutex;
    std::unique_lock<std::mutex> lock(mutex);
    (void)condition.wait_until(lock, releaseAt, [] { return false; });
}
}  // namespace

TEST(WorkerControlBackendProbeTest, KeepsSuccessfulObservationsWhenOnePeerProbeFails)
{
    const auto availablePeer = Peer("worker1");
    const auto failedPeer = Peer("worker2");
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);

    auto observations = ProbeControlBackendPeers(
        { availablePeer, failedPeer }, deadline,
        [availablePeer](const cluster::MemberIdentity &, std::chrono::steady_clock::time_point,
                        std::unique_ptr<WorkerControlBackendProbe> &client) {
            client = std::make_unique<WorkerControlBackendProbe>(
                [](int32_t, int64_t &tag) {
                    tag = 1;
                    return Status::OK();
                },
                [availablePeer](const cluster::MemberIdentity &peer, int64_t, RpcRecvFlags flags,
                                cluster::ControlBackendObservation &observation) {
                    EXPECT_EQ(flags, RpcRecvFlags::DONTWAIT);
                    if (peer.address != availablePeer.address) {
                        return Status(K_RPC_UNAVAILABLE, "injected peer probe failure");
                    }
                    observation = AvailableObservation(peer);
                    return Status::OK();
                },
                [](int64_t) {});
            return Status::OK();
        });

    ASSERT_EQ(observations.size(), 1ul);
    EXPECT_EQ(observations[0].reporter, availablePeer);
    EXPECT_EQ(observations[0].state, cluster::ControlBackendState::AVAILABLE);
}

TEST(WorkerControlBackendProbeTest, CollectsReadyPeerBeforeSharedDeadlineAndCleansEveryTagOnce)
{
    const auto timeoutPeer = Peer("worker-timeout");
    const auto availablePeer = Peer("worker-available");
    const auto failedPeer = Peer("worker-failed");
    const std::map<std::string, int64_t> tags = { { timeoutPeer.address, 1 },
                                                  { availablePeer.address, 2 },
                                                  { failedPeer.address, 3 } };
    std::map<int64_t, int32_t> startTimeouts;
    std::map<int64_t, size_t> readAttempts;
    std::map<int64_t, size_t> forgetCalls;
    const auto start = std::chrono::steady_clock::now();
    const auto deadline = start + std::chrono::seconds(10);
    const auto releaseAt = start + std::chrono::seconds(1);
    std::promise<void> factoryEntered;
    std::promise<void> releaseFactory;
    std::promise<std::chrono::steady_clock::time_point> factoryReturned;
    auto factoryEnteredFuture = factoryEntered.get_future();
    auto releaseFactoryFuture = releaseFactory.get_future().share();
    auto factoryReturnedFuture = factoryReturned.get_future();

    auto probeFuture = std::async(std::launch::async, [&] {
        return ProbeControlBackendPeers(
            { timeoutPeer, availablePeer, failedPeer }, deadline,
            [&](const cluster::MemberIdentity &peer, std::chrono::steady_clock::time_point factoryDeadline,
                std::unique_ptr<WorkerControlBackendProbe> &client) {
                EXPECT_EQ(factoryDeadline, deadline);
                if (peer == timeoutPeer) {
                    factoryEntered.set_value();
                    releaseFactoryFuture.wait();
                }
                const auto tag = tags.at(peer.address);
                client = std::make_unique<WorkerControlBackendProbe>(
                    [&, tag](int32_t timeoutMs, int64_t &startedTag) {
                        startTimeouts[tag] = timeoutMs;
                        startedTag = tag;
                        return Status::OK();
                    },
                    [&, tag, timeoutPeer, availablePeer, failedPeer](const cluster::MemberIdentity &currentPeer,
                                                                     int64_t, RpcRecvFlags flags,
                                                                     cluster::ControlBackendObservation &observation) {
                        EXPECT_EQ(flags, RpcRecvFlags::DONTWAIT);
                        ++readAttempts[tag];
                        if (currentPeer == availablePeer) {
                            observation = AvailableObservation(currentPeer);
                            return Status::OK();
                        }
                        if (currentPeer == failedPeer || readAttempts[tag] > 1) {
                            return Status(K_RPC_UNAVAILABLE, "injected terminal peer probe failure");
                        }
                        EXPECT_EQ(currentPeer, timeoutPeer);
                        return Status(K_TRY_AGAIN, "injected unfinished peer probe");
                    },
                    [&, tag](int64_t forgottenTag) {
                        EXPECT_EQ(forgottenTag, tag);
                        ++forgetCalls[tag];
                    });
                if (peer == timeoutPeer) {
                    factoryReturned.set_value(std::chrono::steady_clock::now());
                }
                return Status::OK();
            });
    });
    factoryEnteredFuture.wait();
    WaitUntil(releaseAt);
    releaseFactory.set_value();
    const auto factoryReturnedAt = factoryReturnedFuture.get();
    auto observations = probeFuture.get();
    const auto remainderFloor =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - factoryReturnedAt).count();

    ASSERT_EQ(observations.size(), 1ul);
    EXPECT_EQ(observations[0].reporter, availablePeer);
    ASSERT_EQ(startTimeouts.size(), tags.size());
    EXPECT_GT(startTimeouts[tags.at(timeoutPeer.address)], 0);
    EXPECT_LE(startTimeouts[tags.at(timeoutPeer.address)], remainderFloor);
    EXPECT_LE(startTimeouts[tags.at(availablePeer.address)], startTimeouts[tags.at(timeoutPeer.address)]);
    EXPECT_EQ(readAttempts[tags.at(timeoutPeer.address)], 2ul);
    EXPECT_EQ(readAttempts[tags.at(availablePeer.address)], 1ul);
    EXPECT_EQ(readAttempts[tags.at(failedPeer.address)], 1ul);
    EXPECT_EQ(forgetCalls[tags.at(timeoutPeer.address)], 1ul);
    EXPECT_EQ(forgetCalls[tags.at(availablePeer.address)], 0ul);
    EXPECT_EQ(forgetCalls[tags.at(failedPeer.address)], 1ul);
}

TEST(WorkerControlBackendProbeTest, DoesNotStartOrCreateMorePeersAfterFactoryExceedsDeadline)
{
    const auto firstPeer = Peer("slow-cold-stub");
    const auto laterPeer = Peer("later-ready-peer");
    const auto anotherPeer = Peer("another-peer");
    size_t firstFactoryCalls = 0;
    size_t laterFactoryCalls = 0;
    size_t startCalls = 0;
    const auto start = std::chrono::steady_clock::now();
    const auto deadline = start + std::chrono::seconds(2);
    std::promise<void> factoryEntered;
    std::promise<void> releaseFactory;
    std::promise<std::chrono::steady_clock::time_point> factoryReturned;
    auto factoryEnteredFuture = factoryEntered.get_future();
    auto releaseFactoryFuture = releaseFactory.get_future().share();
    auto factoryReturnedFuture = factoryReturned.get_future();

    auto probeFuture = std::async(std::launch::async, [&] {
        return ProbeControlBackendPeers(
            { firstPeer, laterPeer, anotherPeer }, deadline,
            [&](const cluster::MemberIdentity &peer, std::chrono::steady_clock::time_point factoryDeadline,
                std::unique_ptr<WorkerControlBackendProbe> &client) {
                EXPECT_EQ(factoryDeadline, deadline);
                if (peer == firstPeer) {
                    ++firstFactoryCalls;
                    factoryEntered.set_value();
                    releaseFactoryFuture.wait();
                } else {
                    ++laterFactoryCalls;
                }
                client = std::make_unique<WorkerControlBackendProbe>(
                    [&](int32_t, int64_t &tag) {
                        ++startCalls;
                        tag = 1;
                        return Status::OK();
                    },
                    [](const cluster::MemberIdentity &, int64_t, RpcRecvFlags, cluster::ControlBackendObservation &) {
                        return Status(K_TRY_AGAIN, "unfinished");
                    },
                    [](int64_t) {});
                if (peer == firstPeer) {
                    factoryReturned.set_value(std::chrono::steady_clock::now());
                }
                return Status::OK();
            });
    });
    factoryEnteredFuture.wait();
    WaitUntil(deadline + std::chrono::seconds(1));
    releaseFactory.set_value();
    const auto factoryReturnedAt = factoryReturnedFuture.get();
    auto observations = probeFuture.get();
    const auto remainderFloor =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - factoryReturnedAt).count();

    EXPECT_TRUE(observations.empty());
    EXPECT_LE(remainderFloor, 0);
    EXPECT_EQ(firstFactoryCalls, 1ul);
    EXPECT_EQ(laterFactoryCalls, 0ul);
    EXPECT_EQ(startCalls, 0ul);
}
}  // namespace datasystem::worker
