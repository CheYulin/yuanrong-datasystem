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

#include "datasystem/client/object_cache/direct_read/direct_read_fallback.h"
#include "datasystem/client/object_cache/direct_read/direct_read_test_hook.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/object_cache/object_bitmap.h"
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

Status AppendInlinePayloads(const master::QueryMetaInfoPb &queryMeta, std::vector<RpcMessage> &metaPayloads,
                            std::vector<RpcMessage> &outPayloads, GetRspPb::PayloadInfoPb &info)
{
    const auto startIndex = outPayloads.size();
    for (const auto idx : queryMeta.payload_indexs()) {
        CHECK_FAIL_RETURN_STATUS(idx < metaPayloads.size(), K_RUNTIME_ERROR, "Invalid inline payload index from meta");
        outPayloads.emplace_back(std::move(metaPayloads[idx]));
    }
    for (size_t index = startIndex; index < outPayloads.size(); ++index) {
        info.add_part_index(static_cast<uint32_t>(index));
    }
    CHECK_FAIL_RETURN_STATUS(!info.part_index().empty(), K_RUNTIME_ERROR, "Inline meta payload is empty");
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

int32_t MaxControlPlaneRetries()
{
    return std::max(0, FLAGS_client_direct_read_retry_count);
}
}  // namespace

DirectReadFlow::DirectReadFlow(std::shared_ptr<IClientWorkerApi> workerApi, RpcCredential cred, Signature *signature,
                               int32_t requestTimeoutMs)
    : workerApi_(std::move(workerApi)),
      rpcAdapter_(std::move(cred), signature, requestTimeoutMs),
      routeProvider_(workerApi_, &rpcAdapter_),
      routeAdapter_(std::make_shared<DirectReadRouteProviderAdapter>(&routeProvider_)),
      metaAdapter_(std::make_shared<DirectReadMetaClientAdapter>(&rpcAdapter_, workerApi_->hostPort_)),
      dataAdapter_(std::make_shared<DirectReadDataClientAdapter>(&rpcAdapter_)),
      accessFlow_(routeAdapter_, metaAdapter_, dataAdapter_)
{
}

Status DirectReadFlow::ExecuteMetaPhaseWithRetry(const ObjectReadAccessRequest &request,
                                                 ObjectReadAccessMetaResult &result)
{
    Status lastRc = Status::OK();
    const int32_t maxRetries = MaxControlPlaneRetries();
    for (int32_t attempt = 0; attempt <= maxRetries; ++attempt) {
        lastRc = accessFlow_.ExecuteMetaPhase(request, result);
        if (lastRc.IsOk()) {
            return Status::OK();
        }
        if (!DirectReadFallback::IsRetriableControlPlaneFailure(lastRc) || attempt == maxRetries) {
            return DirectReadFallback::ToPathFallbackStatus(lastRc);
        }
        if (lastRc.GetCode() == K_NOT_READY) {
            DirectReadTestHook::RecordStaleRouteRetry();
        } else if (lastRc.GetCode() == K_TRY_AGAIN) {
            DirectReadTestHook::RecordMovingRetry();
        }
        RETURN_IF_NOT_OK(routeProvider_.RefreshRouteIfNeeded());
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
        DirectReadTestHook::RecordDataQuery();

        const bool useInlinePayload =
            queryMeta->payload_indexs_size() > 0 && !DirectReadTestHook::PreferRemoteDataGet();
        if (useInlinePayload) {
            RETURN_IF_NOT_OK(AppendInlinePayloads(*queryMeta, metaResult.metaPayloads, outPayloads, *payloadInfo));
            continue;
        }

        GetObjectRemoteRspPb remoteRsp;
        std::vector<RpcMessage> remotePayloads;
        RETURN_IF_NOT_OK(
            dataAdapter_->ReadData(*queryMeta, getParam.subTimeoutMs, objectIndex, remoteRsp, remotePayloads));
        RETURN_IF_NOT_OK(AppendRemotePayloads(remoteRsp, remotePayloads, outPayloads, *payloadInfo));
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
