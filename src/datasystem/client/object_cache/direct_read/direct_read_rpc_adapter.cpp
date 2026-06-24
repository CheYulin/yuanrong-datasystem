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
 * Description: Minimal RPC adapter for client direct read.
 */
#include "datasystem/client/object_cache/direct_read/direct_read_rpc_adapter.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "datasystem/client/object_cache/direct_read/direct_read_flow.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/protos/worker_object.stub.rpc.pb.h"

namespace datasystem {
namespace object_cache {
DirectReadRpcAdapter::DirectReadRpcAdapter(RpcCredential cred, Signature *signature, int32_t requestTimeoutMs)
    : cred_(std::move(cred)), signature_(signature), requestTimeoutMs_(requestTimeoutMs)
{
}

Status DirectReadRpcAdapter::GetClusterState(const HostPort &workerAddress, HashRingPb &ring, int64_t &version) const
{
    RETURN_RUNTIME_ERROR_IF_NULL(signature_);
    GetClusterStateReqPb req;
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));

    auto channel = std::make_shared<RpcChannel>(workerAddress, cred_);
    WorkerWorkerOCService_Stub stub(channel, requestTimeoutMs_);
    RpcOptions opts;
    opts.SetTimeout(requestTimeoutMs_);
    GetClusterStateRspPb rsp;
    RETURN_IF_NOT_OK(stub.GetClusterState(opts, req, rsp));
    ring.CopyFrom(rsp.hash_ring());
    version = rsp.ring_etcd_mod_revision() > 0 ? rsp.ring_etcd_mod_revision() : -1;
    return Status::OK();
}

Status DirectReadRpcAdapter::GetObjectRemoteTcp(const HostPort &dataAddress, const master::QueryMetaInfoPb &queryMeta,
                                                const GetParam &getParam, size_t objectIndex, int64_t subTimeoutMs,
                                                GetObjectRemoteRspPb &rsp, std::vector<RpcMessage> &payloads) const
{
    RETURN_RUNTIME_ERROR_IF_NULL(signature_);
    const auto &meta = queryMeta.meta();
    GetObjectRemoteReqPb req;
    req.set_object_key(meta.object_key());
    req.set_request_id(GetStringUuid());
    req.set_version(meta.version());
    uint64_t readOffset = 0;
    uint64_t readSize = meta.data_size();
    if (!getParam.readParams.empty() && objectIndex < getParam.readParams.size()) {
        readOffset = getParam.readParams[objectIndex].offset;
        readSize = getParam.readParams[objectIndex].size;
    }
    req.set_read_offset(readOffset);
    req.set_read_size(readSize);
    req.set_data_size(meta.data_size());
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));

    int32_t rpcTimeoutMs = requestTimeoutMs_;
    if (subTimeoutMs > 0) {
        rpcTimeoutMs = static_cast<int32_t>(std::min<int64_t>(subTimeoutMs, requestTimeoutMs_));
    }

    auto channel = std::make_shared<RpcChannel>(dataAddress, cred_);
    WorkerWorkerOCService_Stub stub(channel, rpcTimeoutMs);
    RpcOptions opts;
    opts.SetTimeout(rpcTimeoutMs);
    RETURN_IF_NOT_OK(stub.GetObjectRemote(opts, req, rsp, payloads));
    if (rsp.has_error() && rsp.error().error_code() != static_cast<int32_t>(StatusCode::K_OK)) {
        return Status(static_cast<StatusCode>(rsp.error().error_code()), rsp.error().error_msg());
    }
    if (rsp.data_source() != DataTransferSource::DATA_IN_PAYLOAD) {
        return Status(K_NOT_SUPPORTED, "direct_read_non_tcp_data_source");
    }
    if (payloads.empty()) {
        return Status(K_RUNTIME_ERROR, DirectReadFlow::kDataWorkerUnavailableFallbackReason);
    }
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
