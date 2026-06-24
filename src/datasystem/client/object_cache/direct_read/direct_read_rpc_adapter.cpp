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

#include <chrono>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "datasystem/client/object_cache/direct_read/direct_read_flow.h"
#include "datasystem/client/object_cache/direct_read/direct_read_test_hook.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/protos/master_object.stub.rpc.pb.h"
#include "datasystem/protos/worker_object.stub.rpc.pb.h"

DS_DECLARE_int32(client_direct_read_retry_count);

namespace datasystem {
namespace object_cache {
namespace {
Status QueryMetaOnce(Signature *signature, RpcCredential cred, int32_t requestTimeoutMs, const HostPort &metaAddress,
                     const HostPort &clientWorkerAddress, const std::vector<std::string> &objectKeys,
                     int64_t subTimeoutMs, bool enableRedirect, master::QueryMetaRspPb &rsp,
                     std::vector<RpcMessage> &payloads)
{
    RETURN_RUNTIME_ERROR_IF_NULL(signature);
    DirectReadTestHook::RecordMetaQuery();
    master::QueryMetaReqPb req;
    *req.mutable_ids() = { objectKeys.begin(), objectKeys.end() };
    req.set_address(clientWorkerAddress.ToString());
    req.set_sub_timeout(std::min<int64_t>(subTimeoutMs, requestTimeoutMs));
    req.set_request_id(GetStringUuid());
    req.set_timeout(requestTimeoutMs);
    req.set_redirect(enableRedirect);
    RETURN_IF_NOT_OK(signature->GenerateSignature(req));

    auto channel = std::make_shared<RpcChannel>(metaAddress, cred);
    master::MasterOCService_Stub stub(channel, requestTimeoutMs);
    RpcOptions opts;
    opts.SetTimeout(requestTimeoutMs);
    Status rc = stub.QueryMeta(opts, req, rsp, payloads);
    if (rc.IsError()) {
        payloads.clear();
        if (rc.GetCode() == K_RPC_DEADLINE_EXCEEDED || rc.GetCode() == K_WORKER_TIMEOUT
            || rc.GetCode() == K_RPC_UNAVAILABLE) {
            return Status(rc.GetCode(), DirectReadFlow::kMetaTimeoutFallbackReason);
        }
        return rc;
    }

    if (DirectReadTestHook::ConsumeSimulateMetaMovingResponse()) {
        rsp.set_meta_is_moving(true);
        auto *info = rsp.add_info();
        info->set_redirect_meta_address(metaAddress.ToString());
        for (const auto &objectKey : objectKeys) {
            *info->add_change_meta_ids() = objectKey;
        }
        return Status::OK();
    }

    if (DirectReadTestHook::SimulateRedirectLoop()) {
        DirectReadTestHook::RecordRedirectRetry();
        auto *info = rsp.add_info();
        info->set_redirect_meta_address(metaAddress.ToString());
        for (const auto &objectKey : objectKeys) {
            *info->add_change_meta_ids() = objectKey;
        }
    }

    return Status::OK();
}

int32_t MaxControlPlaneRetries()
{
    return std::max(0, FLAGS_client_direct_read_retry_count);
}

Status AppendQueryMetaPayloads(std::vector<RpcMessage> &basePayloads, master::QueryMetaRspPb &rsp,
                               std::vector<RpcMessage> &newPayloads)
{
    if (newPayloads.empty()) {
        return Status::OK();
    }
    const auto payloadOffset = static_cast<uint32_t>(basePayloads.size());
    for (auto &queryMeta : *rsp.mutable_query_metas()) {
        for (int i = 0; i < queryMeta.payload_indexs_size(); ++i) {
            queryMeta.set_payload_indexs(i, queryMeta.payload_indexs(i) + static_cast<int32_t>(payloadOffset));
        }
    }
    basePayloads.insert(basePayloads.end(), std::make_move_iterator(newPayloads.begin()),
                        std::make_move_iterator(newPayloads.end()));
    return Status::OK();
}

void MergeQueryMetaResponse(master::QueryMetaRspPb &dest, master::QueryMetaRspPb &src)
{
    for (auto &queryMeta : *src.mutable_query_metas()) {
        dest.add_query_metas()->Swap(&queryMeta);
    }
    for (auto &missingKey : *src.mutable_not_exist_ids()) {
        *dest.add_not_exist_ids() = std::move(missingKey);
    }
    for (auto version : src.deleting_versions()) {
        dest.add_deleting_versions(version);
    }
    for (auto &missingKey : *src.mutable_not_exist_ids_is_deleting()) {
        *dest.add_not_exist_ids_is_deleting() = std::move(missingKey);
    }
    if (src.meta_is_moving()) {
        dest.set_meta_is_moving(true);
    }
}

Status QueryMetadataFromRedirectMasters(Signature *signature, RpcCredential cred, int32_t requestTimeoutMs,
                                        const HostPort &clientWorkerAddress, int64_t subTimeoutMs,
                                        master::QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads)
{
    const google::protobuf::RepeatedPtrField<RedirectMetaInfo> redirectInfos = rsp.info();
    rsp.clear_info();

    for (const auto &redirectInfo : redirectInfos) {
        if (redirectInfo.redirect_meta_address().empty()) {
            return Status(K_RUNTIME_ERROR, DirectReadFlow::kRedirectLoopFallbackReason);
        }
        std::vector<std::string> redirectIds = { redirectInfo.change_meta_ids().begin(),
                                                 redirectInfo.change_meta_ids().end() };
        if (redirectIds.empty()) {
            continue;
        }

        HostPort redirectMetaAddress;
        RETURN_IF_NOT_OK(redirectMetaAddress.ParseString(redirectInfo.redirect_meta_address()));

        master::QueryMetaRspPb redirectRsp;
        std::vector<RpcMessage> redirectPayloads;
        RETURN_IF_NOT_OK(QueryMetaOnce(signature, cred, requestTimeoutMs, redirectMetaAddress, clientWorkerAddress,
                                       redirectIds, subTimeoutMs, false, redirectRsp, redirectPayloads));
        DirectReadTestHook::RecordRedirectRetry();

        if (redirectRsp.meta_is_moving()) {
            return Status(K_TRY_AGAIN, DirectReadFlow::kMetaMovingFallbackReason);
        }
        if (redirectRsp.info_size() > 0) {
            return Status(K_RUNTIME_ERROR, DirectReadFlow::kRedirectLoopFallbackReason);
        }

        MergeQueryMetaResponse(rsp, redirectRsp);
        RETURN_IF_NOT_OK(AppendQueryMetaPayloads(payloads, redirectRsp, redirectPayloads));
    }
    return Status::OK();
}

Status WaitForMetaMigrationToFinish(Signature *signature, RpcCredential cred, int32_t requestTimeoutMs,
                                    const HostPort &metaAddress, const HostPort &clientWorkerAddress,
                                    const std::vector<std::string> &objectKeys, int64_t subTimeoutMs,
                                    master::QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads,
                                    const DirectReadRpcAdapter::RouteRefreshFn &refreshRoute)
{
    int32_t movingAttempts = 0;
    const int32_t maxRetries = MaxControlPlaneRetries();
    int64_t sleepTimeMs = 1;
    const int64_t maxSleepTimeMs = 128;

    while (rsp.info_size() > 0 && rsp.meta_is_moving()) {
        DirectReadTestHook::RecordMovingRetry();
        if (++movingAttempts > maxRetries) {
            return Status(K_TRY_AGAIN, DirectReadFlow::kMetaMovingFallbackReason);
        }
        if (refreshRoute) {
            RETURN_IF_NOT_OK(refreshRoute());
        }
        rsp.Clear();
        payloads.clear();
        sleepTimeMs = std::min(sleepTimeMs, subTimeoutMs > 0 ? subTimeoutMs : sleepTimeMs);
        if (sleepTimeMs > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepTimeMs));
            sleepTimeMs = std::min(sleepTimeMs * 2, maxSleepTimeMs);
        }
        RETURN_IF_NOT_OK(QueryMetaOnce(signature, cred, requestTimeoutMs, metaAddress, clientWorkerAddress, objectKeys,
                                       subTimeoutMs, true, rsp, payloads));
    }
    return Status::OK();
}
}  // namespace

DirectReadRpcAdapter::DirectReadRpcAdapter(RpcCredential cred, Signature *signature, int32_t requestTimeoutMs)
    : cred_(std::move(cred)), signature_(signature), requestTimeoutMs_(requestTimeoutMs)
{
}

Status DirectReadRpcAdapter::QueryMeta(const HostPort &metaAddress, const HostPort &clientWorkerAddress,
                                       const GetParam &getParam, master::QueryMetaRspPb &rsp,
                                       std::vector<RpcMessage> &payloads,
                                       const RouteRefreshFn &refreshRoute) const
{
    std::vector<std::string> objectKeys(getParam.objectKeys.begin(), getParam.objectKeys.end());
    master::QueryMetaRspPb primaryRsp;
    std::vector<RpcMessage> primaryPayloads;
    RETURN_IF_NOT_OK(QueryMetaOnce(signature_, cred_, requestTimeoutMs_, metaAddress, clientWorkerAddress, objectKeys,
                                   getParam.subTimeoutMs, true, primaryRsp, primaryPayloads));
    RETURN_IF_NOT_OK(WaitForMetaMigrationToFinish(signature_, cred_, requestTimeoutMs_, metaAddress,
                                                  clientWorkerAddress, objectKeys, getParam.subTimeoutMs, primaryRsp,
                                                  primaryPayloads, refreshRoute));

    if (primaryRsp.meta_is_moving()) {
        return Status(K_TRY_AGAIN, DirectReadFlow::kMetaMovingFallbackReason);
    }

    rsp = std::move(primaryRsp);
    payloads.clear();
    RETURN_IF_NOT_OK(AppendQueryMetaPayloads(payloads, rsp, primaryPayloads));
    if (rsp.info_size() > 0) {
        RETURN_IF_NOT_OK(QueryMetadataFromRedirectMasters(signature_, cred_, requestTimeoutMs_, clientWorkerAddress,
                                                          getParam.subTimeoutMs, rsp, payloads));
    }
    return Status::OK();
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
    version = -1;
    return Status::OK();
}

Status DirectReadRpcAdapter::GetObjectRemoteTcp(const HostPort &dataAddress, const master::QueryMetaInfoPb &queryMeta,
                                                const GetParam &getParam, size_t objectIndex,
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

    auto channel = std::make_shared<RpcChannel>(dataAddress, cred_);
    WorkerWorkerOCService_Stub stub(channel, requestTimeoutMs_);
    RpcOptions opts;
    opts.SetTimeout(requestTimeoutMs_);
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
