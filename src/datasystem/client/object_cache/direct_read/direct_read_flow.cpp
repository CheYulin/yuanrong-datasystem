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

#include <utility>

#include "datasystem/client/object_cache/direct_read/direct_read_test_hook.h"
#include "datasystem/common/object_cache/object_bitmap.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace object_cache {
namespace {
uint64_t ResolveReadOffset(const GetParam &getParam, size_t objectIndex)
{
    if (getParam.readParams.empty() || objectIndex >= getParam.readParams.size()) {
        return 0;
    }
    return getParam.readParams[objectIndex].offset;
}

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

Status AppendInlinePayloads(const master::QueryMetaInfoPb &queryMeta, const std::vector<RpcMessage> &metaPayloads,
                            std::vector<RpcMessage> &outPayloads, GetRspPb::PayloadInfoPb &info)
{
    const auto startIndex = outPayloads.size();
    for (const auto idx : queryMeta.payload_indexs()) {
        CHECK_FAIL_RETURN_STATUS(idx < metaPayloads.size(), K_RUNTIME_ERROR, "Invalid inline payload index from meta");
        outPayloads.emplace_back(metaPayloads[idx]);
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
}  // namespace

DirectReadFlow::DirectReadFlow(std::shared_ptr<IClientWorkerApi> workerApi, RpcCredential cred, Signature *signature,
                               int32_t requestTimeoutMs)
    : workerApi_(std::move(workerApi)),
      rpcAdapter_(std::move(cred), signature, requestTimeoutMs),
      routeProvider_(workerApi_)
{
}

Status DirectReadFlow::Get(const GetParam &getParam, std::vector<std::shared_ptr<Buffer>> &buffers,
                           const DirectReadFinishGetFn &finishGet)
{
    CHECK_FAIL_RETURN_STATUS(finishGet != nullptr, K_INVALID, "Direct read finish handler is null");
    CHECK_FAIL_RETURN_STATUS(buffers.size() == getParam.objectKeys.size(), K_INVALID,
                             "Direct read buffer size does not match object key count");

    HostPort metaAddress;
    RETURN_IF_NOT_OK(routeProvider_.GetMetaAddress(getParam, metaAddress));

    master::QueryMetaRspPb metaRsp;
    std::vector<RpcMessage> metaPayloads;
    RETURN_IF_NOT_OK(rpcAdapter_.QueryMeta(metaAddress, workerApi_->hostPort_, getParam, metaRsp, metaPayloads));
    if (metaRsp.meta_is_moving()) {
        return Status(K_TRY_AGAIN, "meta_is_moving");
    }

    GetRspPb getRsp;
    std::vector<RpcMessage> outPayloads;
    outPayloads.reserve(metaPayloads.size());

    for (size_t objectIndex = 0; objectIndex < getParam.objectKeys.size(); ++objectIndex) {
        const auto &objectKey = getParam.objectKeys[objectIndex];
        bool notExist = false;
        for (const auto &missingKey : metaRsp.not_exist_ids()) {
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

        const master::QueryMetaInfoPb *queryMeta = FindQueryMeta(metaRsp, objectKey);
        if (queryMeta == nullptr) {
            return Status(K_NOT_FOUND, FormatString("Direct meta query missing object %s", objectKey));
        }

        auto *payloadInfo = getRsp.add_payload_info();
        FillPayloadInfoFromMeta(*queryMeta, getParam, objectIndex, *payloadInfo);
        DirectReadTestHook::RecordDataQuery();

        const bool useInlinePayload =
            queryMeta->payload_indexs_size() > 0 && !DirectReadTestHook::PreferRemoteDataGet();
        if (useInlinePayload) {
            RETURN_IF_NOT_OK(AppendInlinePayloads(*queryMeta, metaPayloads, outPayloads, *payloadInfo));
            continue;
        }

        if (queryMeta->address().empty()) {
            return Status(K_RUNTIME_ERROR, kDataWorkerUnavailableFallbackReason);
        }

        HostPort dataAddress;
        RETURN_IF_NOT_OK(dataAddress.ParseString(queryMeta->address()));
        GetObjectRemoteRspPb remoteRsp;
        std::vector<RpcMessage> remotePayloads;
        Status remoteRc =
            rpcAdapter_.GetObjectRemoteTcp(dataAddress, *queryMeta, getParam, objectIndex, remoteRsp, remotePayloads);
        if (remoteRc.IsError()) {
            return Status(remoteRc.GetCode(), kDataWorkerUnavailableFallbackReason);
        }
        RETURN_IF_NOT_OK(AppendRemotePayloads(remoteRsp, remotePayloads, outPayloads, *payloadInfo));
        (void)ResolveReadOffset(getParam, objectIndex);
    }

    getRsp.mutable_last_rc()->set_error_code(K_OK);
    return finishGet(getParam, getRsp, outPayloads, buffers);
}
}  // namespace object_cache
}  // namespace datasystem
