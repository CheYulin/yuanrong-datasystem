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
 * Description: Shared object read data path (inline colocate vs remote) for client and worker.
 */
#include "datasystem/common/object_cache/read_access/object_read_data_access.h"

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace object_cache {
namespace {
void RecordOutcome(ObjectReadL0Outcome outcome, ObjectReadL0Outcome *out)
{
    if (out != nullptr) {
        *out = outcome;
    }
}
}  // namespace

ObjectReadDataPath PlanObjectReadDataPath(const master::QueryMetaInfoPb &queryMeta)
{
    return queryMeta.payload_indexs_size() > 0 ? ObjectReadDataPath::kInlineFromMeta : ObjectReadDataPath::kRemote;
}

Status ExtractInlinePayloads(const master::QueryMetaInfoPb &queryMeta, std::vector<RpcMessage> &metaPayloadPool,
                             std::vector<RpcMessage> &outPayloads, ObjectReadL0Outcome *outcome)
{
    if (PlanObjectReadDataPath(queryMeta) != ObjectReadDataPath::kInlineFromMeta) {
        RecordOutcome(ObjectReadL0Outcome::kInlineNotOffered, outcome);
        return Status(K_INVALID, "inline payload not offered by meta query");
    }
    for (const auto idx : queryMeta.payload_indexs()) {
        CHECK_FAIL_RETURN_STATUS(idx < metaPayloadPool.size(), K_RUNTIME_ERROR,
                                 "Invalid inline payload index from meta query");
        outPayloads.emplace_back(std::move(metaPayloadPool[idx]));
    }
    CHECK_FAIL_RETURN_STATUS(!outPayloads.empty(), K_RUNTIME_ERROR, "Inline meta payload is empty");
    RecordOutcome(ObjectReadL0Outcome::kInlineHit, outcome);
    return Status::OK();
}

Status FetchObjectReadData(const master::QueryMetaInfoPb &queryMeta, const ObjectReadSpec &spec,
                           std::vector<RpcMessage> &metaPayloadPool, IObjectReadRemoteDataClient &remoteClient,
                           std::vector<RpcMessage> &outPayloads, GetObjectRemoteRspPb *remoteRspOut,
                           ObjectReadL0Outcome *l0Outcome)
{
    if (PlanObjectReadDataPath(queryMeta) == ObjectReadDataPath::kInlineFromMeta) {
        auto rc = ExtractInlinePayloads(queryMeta, metaPayloadPool, outPayloads, l0Outcome);
        if (rc.IsOk()) {
            return Status::OK();
        }
        RecordOutcome(ObjectReadL0Outcome::kInlineExtractFailed, l0Outcome);
    } else {
        RecordOutcome(ObjectReadL0Outcome::kInlineNotOffered, l0Outcome);
    }

    GetObjectRemoteRspPb remoteRsp;
    RETURN_IF_NOT_OK(remoteClient.FetchRemote(queryMeta, spec, remoteRsp, outPayloads));
    if (remoteRspOut != nullptr) {
        *remoteRspOut = std::move(remoteRsp);
    }
    RecordOutcome(ObjectReadL0Outcome::kRemote, l0Outcome);
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
