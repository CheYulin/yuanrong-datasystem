/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Read-only hash ring snapshot for client-side route lookup.
 */
#include "datasystem/common/object_cache/read_only_hash_ring_view.h"

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/hash_ring.pb.h"
#include "datasystem/worker/hash_ring/hash_ring_tools.h"

DS_DECLARE_bool(enable_distributed_master);
DS_DECLARE_string(master_address);

namespace datasystem {
namespace object_cache {
namespace {
bool IsCentralizedMaster()
{
    return !FLAGS_enable_distributed_master;
}

bool HashInRange(const std::vector<std::pair<uint32_t, uint32_t>> &ranges, const std::string &objectKey)
{
    const uint32_t hash = MurmurHash3_32(reinterpret_cast<const uint8_t *>(objectKey.data()), objectKey.size());
    for (const auto &range : ranges) {
        if (range.first > range.second) {
            if (hash > range.first || hash <= range.second) {
                return true;
            }
        } else if (hash > range.first && hash <= range.second) {
            return true;
        }
    }
    return false;
}

Status ResolvePrimaryWorkerAddr(const std::map<uint32_t, std::string> &tokenMap, uint32_t keyHash,
                                std::string &workerAddr)
{
    if (tokenMap.empty()) {
        RETURN_STATUS(K_NOT_READY, "Hash ring token map is empty");
    }
    auto iter = tokenMap.upper_bound(keyHash);
    if (iter != tokenMap.end()) {
        workerAddr = iter->second;
        return Status::OK();
    }
    workerAddr = tokenMap.begin()->second;
    return Status::OK();
}

bool IsHealthyWorkerState(int state)
{
    return state == WorkerPb::ACTIVE || state == WorkerPb::LEAVING;
}

bool IsJoinableWorkerState(int state)
{
    return state == WorkerPb::ACTIVE || state == WorkerPb::LEAVING || state == WorkerPb::JOINING;
}

const WorkerPb *FindWorkerPbAtAddress(const HashRingPb &ringInfo,
                                      const std::map<std::string, HostPort> &workerUuid2AddrMap,
                                      const HostPort &workerAddress)
{
    const auto exact = ringInfo.workers().find(workerAddress.ToString());
    if (exact != ringInfo.workers().end()) {
        return &exact->second;
    }
    for (const auto &kv : ringInfo.workers()) {
        HostPort mappedAddress;
        if (!kv.second.worker_uuid().empty()) {
            const auto uuidIt = workerUuid2AddrMap.find(kv.second.worker_uuid());
            if (uuidIt != workerUuid2AddrMap.end()) {
                mappedAddress = uuidIt->second;
            } else {
                const auto semi = kv.first.find(';');
                const std::string hostPortStr = semi == std::string::npos ? kv.first : kv.first.substr(0, semi);
                if (!mappedAddress.ParseString(hostPortStr).IsOk()) {
                    continue;
                }
            }
        } else if (!mappedAddress.ParseString(kv.first).IsOk()) {
            continue;
        }
        if (mappedAddress == workerAddress) {
            return &kv.second;
        }
    }
    return nullptr;
}
}  // namespace

bool ReadOnlyHashRingView::HasSnapshot() const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return version_ >= 0;
}

int64_t ReadOnlyHashRingView::Version() const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return version_;
}

bool ReadOnlyHashRingView::HasScalingTask() const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto hasUnfinishedChange = [](const google::protobuf::Map<std::string, ChangeNodePb> &changeNodes) {
        for (const auto &entry : changeNodes) {
            for (const auto &range : entry.second.changed_ranges()) {
                if (!range.finished()) {
                    return true;
                }
            }
        }
        return false;
    };
    return hasUnfinishedChange(ringInfo_.add_node_info()) || hasUnfinishedChange(ringInfo_.del_node_info());
}

bool ReadOnlyHashRingView::IsWorkable() const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return ringInfo_.cluster_has_init() && !tokenMap_.empty();
}

Status ReadOnlyHashRingView::UpdateFromSerialized(const std::string &serializedRing, int64_t version,
                                                bool *versionChanged)
{
    HashRingPb ring;
    if (!ring.ParseFromString(serializedRing)) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Failed to parse hash ring snapshot");
    }
    return UpdateFromPb(ring, version, versionChanged);
}

Status ReadOnlyHashRingView::UpdateFromPb(const HashRingPb &ring, int64_t version, bool *versionChanged)
{
    std::unique_lock<std::shared_mutex> lock(mutex_);
    const int64_t previousVersion = version_;
    if (version_ >= 0 && version < 0) {
        if (versionChanged != nullptr) {
            *versionChanged = false;
        }
        return Status::OK();
    }
    if (version_ >= 0 && version >= 0 && version < version_) {
        if (versionChanged != nullptr) {
            *versionChanged = false;
        }
        return Status::OK();
    }
    if (version_ >= 0 && version >= 0 && version == version_ && ringInfo_.SerializeAsString() == ring.SerializeAsString()) {
        if (versionChanged != nullptr) {
            *versionChanged = false;
        }
        return Status::OK();
    }
    if (version < 0 && version_ < 0 && ringInfo_.SerializeAsString() == ring.SerializeAsString()) {
        if (versionChanged != nullptr) {
            *versionChanged = false;
        }
        return Status::OK();
    }
    ringInfo_.CopyFrom(ring);
    version_ = version;
    RebuildDerivedMapsLocked();
    if (versionChanged != nullptr) {
        *versionChanged = previousVersion != version_ || previousVersion < 0;
    }
    return Status::OK();
}

Status ReadOnlyHashRingView::GetMetaAddress(const std::string &objectKey, HostPort &metaAddress) const
{
    if (IsCentralizedMaster()) {
        RETURN_IF_NOT_OK(metaAddress.ParseString(FLAGS_master_address));
        return Status::OK();
    }

    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (!ringInfo_.cluster_has_init() || tokenMap_.empty()) {
        RETURN_STATUS(K_NOT_READY, "Hash ring snapshot is not ready");
    }

    for (const auto &entry : ringInfo_.add_node_info()) {
        for (const auto &range : entry.second.changed_ranges()) {
            if (HashInRange({ { range.from(), range.end() } }, objectKey)) {
                RETURN_IF_NOT_OK(metaAddress.ParseString(entry.first));
                return Status::OK();
            }
        }
    }

    const uint32_t keyHash = MurmurHash3_32(reinterpret_cast<const uint8_t *>(objectKey.data()), objectKey.size());
    std::string workerAddr;
    RETURN_IF_NOT_OK(ResolvePrimaryWorkerAddr(tokenMap_, keyHash, workerAddr));

    std::string workerUuid;
    auto uuidIt = ringInfo_.workers().find(workerAddr);
    if (uuidIt == ringInfo_.workers().end() || uuidIt->second.worker_uuid().empty()) {
        RETURN_IF_NOT_OK(metaAddress.ParseString(workerAddr));
        return Status::OK();
    }
    RETURN_IF_NOT_OK(
        worker::GetWorkerAddrByUuidForMetadata(workerUuid2AddrMap_, uuidIt->second.worker_uuid(), metaAddress));
    return Status::OK();
}

bool ReadOnlyHashRingView::HasHealthyWorkerAtAddress(const HostPort &workerAddress) const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    const WorkerPb *workerPb = FindWorkerPbAtAddress(ringInfo_, workerUuid2AddrMap_, workerAddress);
    return workerPb != nullptr && IsHealthyWorkerState(workerPb->state());
}

bool ReadOnlyHashRingView::HasJoinableWorkerAtAddress(const HostPort &workerAddress) const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    const WorkerPb *workerPb = FindWorkerPbAtAddress(ringInfo_, workerUuid2AddrMap_, workerAddress);
    return workerPb != nullptr && IsJoinableWorkerState(workerPb->state());
}

void ReadOnlyHashRingView::RebuildDerivedMapsLocked()
{
    tokenMap_.clear();
    workerUuid2AddrMap_.clear();
    for (const auto &kv : ringInfo_.workers()) {
        if (kv.second.state() != WorkerPb::ACTIVE && kv.second.state() != WorkerPb::LEAVING) {
            continue;
        }
        for (auto token : kv.second.hash_tokens()) {
            tokenMap_.insert({ token, kv.first });
        }
    }
    std::map<std::string, std::string> workerAddr2UuidMap;
    std::map<std::string, HostPort> relatedWorkerMap;
    worker::GenerateHashRingUuidMap(ringInfo_, workerUuid2AddrMap_, workerAddr2UuidMap, relatedWorkerMap);
}
}  // namespace object_cache
}  // namespace datasystem
