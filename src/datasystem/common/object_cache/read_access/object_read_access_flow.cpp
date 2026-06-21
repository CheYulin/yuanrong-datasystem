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
 * Description: Shared object read access flow for client direct read and worker gateway read.
 */
#include "datasystem/common/object_cache/read_access/object_read_access_flow.h"

#include <map>
#include <mutex>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace object_cache {
namespace {
std::mutex g_flowTestMutex;
uint64_t g_metaPhaseCount = 0;
}  // namespace

ObjectReadAccessFlow::ObjectReadAccessFlow(std::shared_ptr<IObjectReadRouteProvider> routeProvider,
                                           std::shared_ptr<IObjectReadMetaClient> metaClient,
                                           std::shared_ptr<IObjectReadDataClient> dataClient)
    : routeProvider_(std::move(routeProvider)),
      metaClient_(std::move(metaClient)),
      dataClient_(std::move(dataClient))
{
}

void ObjectReadAccessFlow::RecordMetaPhaseForTest()
{
    std::lock_guard<std::mutex> lock(g_flowTestMutex);
    ++g_metaPhaseCount;
}

uint64_t ObjectReadAccessFlow::MetaPhaseCountForTest()
{
    std::lock_guard<std::mutex> lock(g_flowTestMutex);
    return g_metaPhaseCount;
}

void ObjectReadAccessFlow::ResetTestCounters()
{
    std::lock_guard<std::mutex> lock(g_flowTestMutex);
    g_metaPhaseCount = 0;
}

Status ObjectReadAccessFlow::QueryMetaGroup(const HostPort &metaAddress, const std::vector<std::string> &objectKeys,
                                            int64_t subTimeoutMs, master::QueryMetaRspPb &rsp,
                                            std::vector<RpcMessage> &payloads)
{
    RETURN_RUNTIME_ERROR_IF_NULL(metaClient_);
    return metaClient_->QueryMeta(metaAddress, objectKeys, subTimeoutMs, rsp, payloads);
}

Status ObjectReadAccessFlow::ExecuteMetaPhase(const ObjectReadAccessRequest &request,
                                              ObjectReadAccessMetaResult &result)
{
    RecordMetaPhaseForTest();
    CHECK_FAIL_RETURN_STATUS(routeProvider_ != nullptr && metaClient_ != nullptr, K_RUNTIME_ERROR,
                             "Object read access flow ports are not configured");
    CHECK_FAIL_RETURN_STATUS(!request.objectKeys.empty(), K_INVALID, "Object read access request has no object keys");

    RETURN_IF_NOT_OK(routeProvider_->RefreshRouteIfNeeded());

    std::map<std::string, std::vector<std::string>> keysByMeta;
    for (const auto &objectKey : request.objectKeys) {
        HostPort metaAddress;
        RETURN_IF_NOT_OK(routeProvider_->GetMetaAddress(objectKey, metaAddress));
        keysByMeta[metaAddress.ToString()].push_back(objectKey);
    }

    result.metaRsp.Clear();
    result.metaPayloads.clear();
    for (auto &[metaAddrStr, groupedKeys] : keysByMeta) {
        HostPort metaAddress;
        RETURN_IF_NOT_OK(metaAddress.ParseString(metaAddrStr));

        master::QueryMetaRspPb groupRsp;
        std::vector<RpcMessage> groupPayloads;
        RETURN_IF_NOT_OK(QueryMetaGroup(metaAddress, groupedKeys, request.subTimeoutMs, groupRsp, groupPayloads));

        if (groupRsp.meta_is_moving()) {
            return Status(K_TRY_AGAIN, "meta_is_moving");
        }

        const auto payloadOffset = result.metaPayloads.size();
        for (auto &payload : groupPayloads) {
            result.metaPayloads.emplace_back(std::move(payload));
        }
        for (const auto &missingKey : groupRsp.not_exist_ids()) {
            *result.metaRsp.add_not_exist_ids() = missingKey;
        }
        for (auto &queryMeta : *groupRsp.mutable_query_metas()) {
            auto *merged = result.metaRsp.add_query_metas();
            merged->Swap(&queryMeta);
            for (int i = 0; i < merged->payload_indexs_size(); ++i) {
                merged->set_payload_indexs(i, merged->payload_indexs(i) + static_cast<int32_t>(payloadOffset));
            }
        }
        result.metaRsp.set_meta_is_moving(groupRsp.meta_is_moving());
    }
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
