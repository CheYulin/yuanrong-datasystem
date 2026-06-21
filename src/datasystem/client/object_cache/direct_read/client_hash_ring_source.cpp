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
 * Description: Client-side hash ring refresh source (etcd first, worker next, etcd fallback).
 */
#include "datasystem/client/object_cache/direct_read/client_hash_ring_source.h"

#include "datasystem/client/object_cache/direct_read/direct_read_test_hook.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/kvstore/kv_store.h"
#include "datasystem/common/util/status_helper.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(metastore_address);
DS_DECLARE_bool(enable_distributed_master);

namespace datasystem {
namespace object_cache {
namespace {
std::string ResolveEtcdAddress()
{
    if (!FLAGS_metastore_address.empty()) {
        return FLAGS_metastore_address;
    }
    return FLAGS_etcd_address;
}
}  // namespace

ClientHashRingSource::ClientHashRingSource(std::shared_ptr<IClientWorkerApi> workerApi,
                                           DirectReadRpcAdapter *rpcAdapter)
    : workerApi_(std::move(workerApi)), rpcAdapter_(rpcAdapter)
{
}

Status ClientHashRingSource::GetMetaAddress(const std::string &objectKey, HostPort &metaAddress)
{
    RETURN_IF_NOT_OK(RefreshForRouteLookup());
    return view_.GetMetaAddress(objectKey, metaAddress);
}

Status ClientHashRingSource::RefreshForRouteLookup()
{
    if (!FLAGS_enable_distributed_master) {
        return Status::OK();
    }
    if (!view_.HasSnapshot()) {
        return BootstrapRing();
    }
    if (view_.HasScalingTask() || DirectReadTestHook::ForceHashRingRefresh()) {
        return RefreshRing();
    }
    return Status::OK();
}

ReadOnlyHashRingView &ClientHashRingSource::ViewForTest()
{
    return view_;
}

HashRingRefreshSource ClientHashRingSource::LastRefreshSource() const
{
    return lastRefreshSource_;
}

Status ClientHashRingSource::EnsureInitialized()
{
    if (etcdStore_ != nullptr) {
        return Status::OK();
    }
    const auto backendAddress = ResolveEtcdAddress();
    if (backendAddress.empty()) {
        RETURN_STATUS(K_INVALID, "etcd_address is empty for client hash ring bootstrap");
    }
    etcdStore_ = std::make_shared<EtcdStore>(backendAddress);
    RETURN_IF_NOT_OK(etcdStore_->Init());
    RETURN_IF_NOT_OK(etcdStore_->CreateTable(ETCD_RING_PREFIX, ETCD_RING_PREFIX));
    return Status::OK();
}

Status ClientHashRingSource::LoadFromEtcd()
{
    DirectReadTestHook::RecordHashRingEtcdRefresh();
    RETURN_IF_NOT_OK(EnsureInitialized());
    RangeSearchResult res;
    Status rc = etcdStore_->Get(ETCD_RING_PREFIX, "", res);
    if (rc.GetCode() == K_NOT_FOUND) {
        RETURN_STATUS(K_NOT_READY, "Hash ring not found in etcd");
    }
    RETURN_IF_NOT_OK(rc);
    RETURN_IF_NOT_OK(view_.UpdateFromSerialized(res.value, res.modRevision));
    lastRefreshSource_ = HashRingRefreshSource::ETCD;
    return Status::OK();
}

Status ClientHashRingSource::LoadFromWorker()
{
    RETURN_RUNTIME_ERROR_IF_NULL(workerApi_);
    RETURN_RUNTIME_ERROR_IF_NULL(rpcAdapter_);
    DirectReadTestHook::RecordHashRingWorkerRefresh();
    HashRingPb ring;
    int64_t version = -1;
    RETURN_IF_NOT_OK(rpcAdapter_->GetClusterState(workerApi_->hostPort_, ring, version));
    RETURN_IF_NOT_OK(view_.UpdateFromPb(ring, version));
    lastRefreshSource_ = HashRingRefreshSource::WORKER;
    return Status::OK();
}

Status ClientHashRingSource::BootstrapRing()
{
    Status etcdRc = LoadFromEtcd();
    if (etcdRc.IsOk()) {
        return Status::OK();
    }
    Status workerRc = LoadFromWorker();
    if (workerRc.IsOk()) {
        return Status::OK();
    }
    return etcdRc.IsError() ? etcdRc : workerRc;
}

Status ClientHashRingSource::RefreshRing()
{
    Status workerRc = LoadFromWorker();
    if (workerRc.IsOk()) {
        return Status::OK();
    }
    return LoadFromEtcd();
}
}  // namespace object_cache
}  // namespace datasystem
