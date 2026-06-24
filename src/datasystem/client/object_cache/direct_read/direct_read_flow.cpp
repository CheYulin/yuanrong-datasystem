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
 * Description: Client direct read flow skeleton.
 */
#include "datasystem/client/object_cache/direct_read/direct_read_flow.h"

#include <algorithm>
#include <utility>

#include "datasystem/client/object_cache/direct_read/client_direct_read_meta_options.h"
#include "datasystem/client/object_cache/direct_read/client_query_meta_transport.h"
#include "datasystem/client/object_cache/direct_read/direct_read_fallback.h"
#include "datasystem/client/object_cache/direct_read/direct_read_test_hook.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/object_cache/object_bitmap.h"
#include "datasystem/common/object_cache/read_access/query_meta_orchestrating_meta_client.h"
#include "datasystem/common/object_cache/read_access/object_read_data_access.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/object_posix.pb.h"

DS_DECLARE_int32(client_direct_read_retry_count);

namespace datasystem {
namespace object_cache {
namespace {
uint64_t ResolveReadSize(const master::QueryMetaInfoPb &queryMeta, const GetParam &getParam, size_t objectIndex)
{
    if (!getParam.readParams.empty() && objectIndex < getParam.readParams.size()) {
        return getParam.readParams[objectIndex].size;
    }
    return queryMeta.meta().data_size();
}

void FillPayloadInfoFromMeta(const master::QueryMetaInfoPb &queryMeta, const GetParam &getParam, size_t objectIndex,
                             GetRspPb::PayloadInfoPb &info)
{
    const auto &meta = queryMeta.meta();
    info.set_object_key(meta.object_key());
    info.set_data_size(static_cast<int64_t>(ResolveReadSize(queryMeta, getParam, objectIndex)));
    info.set_version(static_cast<int64_t>(meta.version()));
    info.set_is_seal(meta.life_state() == static_cast<uint32_t>(ObjectLifeState::OBJECT_SEALED));
    info.set_write_mode(meta.config().write_mode());
    info.set_consistency_type(meta.config().consistency_type());
    info.set_cache_type(meta.config().cache_type());
}

Status AppendInlinePayloadsToGetRsp(const master::QueryMetaInfoPb &queryMeta, std::vector<RpcMessage> &fetchedPayloads,
                                    std::vector<RpcMessage> &outPayloads, GetRspPb::PayloadInfoPb &info)
{
    const auto startIndex = outPayloads.size();
    for (auto &payload : fetchedPayloads) {
        outPayloads.emplace_back(std::move(payload));
    }
    fetchedPayloads.clear();
    for (size_t index = startIndex; index < outPayloads.size(); ++index) {
        info.add_part_index(static_cast<uint32_t>(index));
    }
    CHECK_FAIL_RETURN_STATUS(!info.part_index().empty(), K_RUNTIME_ERROR, "Inline meta payload is empty");
    (void)queryMeta;
    return Status::OK();
}

Status AppendRemotePayloads(const GetObjectRemoteRspPb &remoteRsp, std::vector<RpcMessage> &remotePayloads,
                            std::vector<RpcMessage> &outPayloads, GetRspPb::PayloadInfoPb &info)
{
    const auto startIndex = outPayloads.size();
    for (auto &payload : remotePayloads) {
        outPayloads.emplace_back(std::move(payload));
    }
    for (size_t index = startIndex; index < outPayloads.size(); ++index) {
        info.add_part_index(static_cast<uint32_t>(index));
    }
    if (remoteRsp.data_size() > 0) {
        info.set_data_size(remoteRsp.data_size());
    }
    if (remoteRsp.create_time() > 0) {
        info.set_version(remoteRsp.create_time());
    }
    if (remoteRsp.life_state() > 0) {
        info.set_is_seal(remoteRsp.life_state() == static_cast<uint32_t>(ObjectLifeState::OBJECT_SEALED));
    }
    CHECK_FAIL_RETURN_STATUS(!info.part_index().empty(), K_RUNTIME_ERROR, "Remote TCP payload is empty");
    return Status::OK();
}

const master::QueryMetaInfoPb *FindQueryMeta(const master::QueryMetaRspPb &metaRsp, const std::string &objectKey)
{
    for (const auto &queryMeta : metaRsp.query_metas()) {
        if (queryMeta.meta().object_key() == objectKey) {
            return &queryMeta;
        }
    }
    return nullptr;
}

std::shared_ptr<IObjectReadMetaClient> CreateDirectReadMetaClient(RpcCredential cred, Signature *signature,
                                                                  int32_t requestTimeoutMs,
                                                                  const HostPort &clientWorkerAddress,
                                                                  DirectReadRouteProvider *routeProvider)
{
    auto transport = std::make_shared<ClientQueryMetaTransport>(std::move(cred), signature, requestTimeoutMs,
                                                                clientWorkerAddress);
    std::function<Status()> refreshRoute = [routeProvider]() { return routeProvider->RefreshRouteOnClusterEvent(); };
    return std::make_shared<QueryMetaOrchestratingMetaClient>(
        transport, BuildClientDirectReadMetaOptions(routeProvider, std::move(refreshRoute)));
}
}  // namespace

DirectReadFlow::DirectReadFlow(std::shared_ptr<IClientWorkerApi> workerApi, RpcCredential cred, Signature *signature,
                               int32_t requestTimeoutMs, std::shared_ptr<ClientHashRingSource> sharedRingSource,
                               std::shared_ptr<DirectReadRpcAdapter> sharedRpcAdapter)
    : workerApi_(std::move(workerApi)),
      rpcAdapter_(std::move(sharedRpcAdapter)),
      ringSource_(std::move(sharedRingSource)),
      routeProvider_(ringSource_ != nullptr ? std::make_shared<DirectReadRouteProvider>(ringSource_)
                                            : std::make_shared<DirectReadRouteProvider>(workerApi_, rpcAdapter_)),
      metaClient_(CreateDirectReadMetaClient(cred, signature, requestTimeoutMs, workerApi_->hostPort_,
                                             routeProvider_.get())),
      dataAdapter_(std::make_shared<DirectReadDataClientAdapter>(rpcAdapter_.get())),
      metaAccessFlow_(routeProvider_, metaClient_)
{
}

Status DirectReadFlow::ExecuteMetaPhaseWithRetry(const ObjectReadAccessRequest &request,
                                                 ObjectReadAccessMetaResult &result)
{
    Status lastRc = Status::OK();
    const int32_t maxRetries = std::max(0, FLAGS_client_direct_read_retry_count);
    for (int32_t attempt = 0; attempt <= maxRetries; ++attempt) {
        lastRc = metaAccessFlow_.ExecuteMetaPhase(request, result);
        if (lastRc.IsOk()) {
            return Status::OK();
        }
        if (!DirectReadFallback::IsOuterMetaPhaseRetriable(lastRc) || attempt == maxRetries) {
            return DirectReadFallback::ToPathFallbackStatus(lastRc);
        }
        DirectReadTestHook::RecordStaleRouteRetry();
        RETURN_IF_NOT_OK(routeProvider_->RefreshRouteOnClusterEvent());
    }
    return DirectReadFallback::ToPathFallbackStatus(lastRc);
}

Status DirectReadFlow::ExecuteDataPhase(const GetParam &getParam, ObjectReadAccessMetaResult &metaResult,
                                       GetRspPb &getRsp, std::vector<RpcMessage> &outPayloads)
{
    for (size_t objectIndex = 0; objectIndex < getParam.objectKeys.size(); ++objectIndex) {
        const auto &objectKey = getParam.objectKeys[objectIndex];
        bool notExist = false;
        for (const auto &missingKey : metaResult.metaRsp.not_exist_ids()) {
            if (missingKey == objectKey) {
                notExist = true;
                break;
            }
        }
        if (notExist) {
            getRsp.mutable_last_rc()->set_error_code(K_NOT_FOUND);
            getRsp.mutable_last_rc()->set_error_msg("Object not found in direct meta query");
            auto *payloadInfo = getRsp.add_payload_info();
            payloadInfo->set_object_key(objectKey);
            payloadInfo->set_data_size(-1);
            continue;
        }

        const master::QueryMetaInfoPb *queryMeta = FindQueryMeta(metaResult.metaRsp, objectKey);
        if (queryMeta == nullptr) {
            return Status(K_RUNTIME_ERROR, FormatString("Direct meta query missing object %s", objectKey));
        }

        auto *payloadInfo = getRsp.add_payload_info();
        FillPayloadInfoFromMeta(*queryMeta, getParam, objectIndex, *payloadInfo);

        ObjectReadSpec spec;
        if (!getParam.readParams.empty() && objectIndex < getParam.readParams.size()) {
            spec.readOffset = getParam.readParams[objectIndex].offset;
            spec.readSize = getParam.readParams[objectIndex].size;
        } else {
            spec.readSize = queryMeta->meta().data_size();
        }

        dataAdapter_->SetObjectIndex(objectIndex);
        std::vector<RpcMessage> fetchedPayloads;
        GetObjectRemoteRspPb remoteRsp;
        ObjectReadL0Outcome l0Outcome = ObjectReadL0Outcome::kRemote;

        if (DirectReadTestHook::PreferRemoteDataGet()) {
            DirectReadTestHook::RecordDataQuery();
            RETURN_IF_NOT_OK(dataAdapter_->FetchRemote(*queryMeta, spec, remoteRsp, fetchedPayloads));
            l0Outcome = ObjectReadL0Outcome::kRemote;
        } else {
            RETURN_IF_NOT_OK(FetchObjectReadData(*queryMeta, spec, metaResult.metaPayloads, *dataAdapter_,
                                                 fetchedPayloads, &remoteRsp, &l0Outcome));
            if (l0Outcome == ObjectReadL0Outcome::kRemote) {
                DirectReadTestHook::RecordDataQuery();
            } else if (l0Outcome == ObjectReadL0Outcome::kInlineHit) {
                DirectReadTestHook::RecordInlineDataHit();
            }
        }

        if (l0Outcome == ObjectReadL0Outcome::kRemote) {
            RETURN_IF_NOT_OK(AppendRemotePayloads(remoteRsp, fetchedPayloads, outPayloads, *payloadInfo));
        } else {
            RETURN_IF_NOT_OK(
                AppendInlinePayloadsToGetRsp(*queryMeta, fetchedPayloads, outPayloads, *payloadInfo));
        }
    }
    return Status::OK();
}

Status DirectReadFlow::Get(const GetParam &getParam, std::vector<std::shared_ptr<Buffer>> &buffers,
                           const DirectReadFinishGetFn &finishGet)
{
    CHECK_FAIL_RETURN_STATUS(finishGet != nullptr, K_INVALID, "Direct read finish handler is null");
    CHECK_FAIL_RETURN_STATUS(buffers.size() == getParam.objectKeys.size(), K_INVALID,
                             "Direct read buffer size does not match object key count");

    dataAdapter_->SetGetParam(&getParam);

    ObjectReadAccessRequest request;
    request.objectKeys.assign(getParam.objectKeys.begin(), getParam.objectKeys.end());
    request.subTimeoutMs = getParam.subTimeoutMs;
    request.clientWorkerAddress = workerApi_->hostPort_;

    ObjectReadAccessMetaResult metaResult;
    RETURN_IF_NOT_OK(ExecuteMetaPhaseWithRetry(request, metaResult));

    GetRspPb getRsp;
    std::vector<RpcMessage> outPayloads;
    outPayloads.reserve(metaResult.metaPayloads.size());
    RETURN_IF_NOT_OK(ExecuteDataPhase(getParam, metaResult, getRsp, outPayloads));

    getRsp.mutable_last_rc()->set_error_code(K_OK);
    return finishGet(getParam, getRsp, outPayloads, buffers);
}
}  // namespace object_cache
}  // namespace datasystem
