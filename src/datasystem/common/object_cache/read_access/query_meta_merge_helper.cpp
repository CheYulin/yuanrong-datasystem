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
 * Description: Shared helpers for merging QueryMeta responses and inline payloads.
 */
#include "datasystem/common/object_cache/read_access/query_meta_merge_helper.h"

#include <algorithm>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace object_cache {
void MergeQueryMetaResponses(master::QueryMetaRspPb &dest, master::QueryMetaRspPb &src)
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

Status AppendQueryMetaPayloads(std::vector<RpcMessage> &basePayloads, master::QueryMetaRspPb &rsp,
                               std::vector<RpcMessage> &newPayloads)
{
    if (newPayloads.empty()) {
        return Status::OK();
    }
    const auto payloadSize = basePayloads.size();
    const auto realPayloadSize = static_cast<uint32_t>(payloadSize);
    if (payloadSize != realPayloadSize) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "overflow happen");
    }
    bool overflow = false;
    for (auto iter = rsp.mutable_query_metas()->begin(); iter != rsp.mutable_query_metas()->end(); ++iter) {
        auto &queryMeta = *iter;
        std::for_each(queryMeta.mutable_payload_indexs()->begin(), queryMeta.mutable_payload_indexs()->end(),
                      [realPayloadSize, &overflow](uint32_t &idx) {
                          overflow |= (idx > UINT32_MAX - realPayloadSize);
                          idx += realPayloadSize;
                      });
    }
    basePayloads.insert(basePayloads.end(), std::make_move_iterator(newPayloads.begin()),
                        std::make_move_iterator(newPayloads.end()));
    if (overflow) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "overflow happen");
    }
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
