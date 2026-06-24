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
 * Description: Shared route/meta phase for client direct read and worker gateway read.
 */
#ifndef DATASYSTEM_COMMON_OBJECT_CACHE_READ_ACCESS_OBJECT_READ_META_ACCESS_FLOW_H
#define DATASYSTEM_COMMON_OBJECT_CACHE_READ_ACCESS_OBJECT_READ_META_ACCESS_FLOW_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
struct ObjectReadAccessRequest {
    std::vector<std::string> objectKeys;
    int64_t subTimeoutMs = 0;
    HostPort clientWorkerAddress;
};

struct ObjectReadAccessMetaResult {
    master::QueryMetaRspPb metaRsp;
    std::vector<RpcMessage> metaPayloads;
};

class IObjectReadRouteProvider {
public:
    virtual ~IObjectReadRouteProvider() = default;
    virtual Status GetMetaAddress(const std::string &objectKey, HostPort &metaAddress) = 0;
    virtual Status RefreshRouteIfNeeded() = 0;
};

class IObjectReadMetaClient {
public:
    virtual ~IObjectReadMetaClient() = default;
    virtual Status QueryMeta(const HostPort &metaAddress, const std::vector<std::string> &objectKeys,
                             int64_t subTimeoutMs, master::QueryMetaRspPb &rsp,
                             std::vector<RpcMessage> &payloads) = 0;
};

class ObjectReadMetaAccessFlow {
public:
    ObjectReadMetaAccessFlow(std::shared_ptr<IObjectReadRouteProvider> routeProvider,
                             std::shared_ptr<IObjectReadMetaClient> metaClient);

    Status ExecuteMetaPhase(const ObjectReadAccessRequest &request, ObjectReadAccessMetaResult &result);

    static void RecordMetaPhaseForTest();
    static uint64_t MetaPhaseCountForTest();
    static void ResetTestCounters();

private:
    Status QueryMetaGroup(const HostPort &metaAddress, const std::vector<std::string> &objectKeys,
                          int64_t subTimeoutMs, master::QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads);

    std::shared_ptr<IObjectReadRouteProvider> routeProvider_;
    std::shared_ptr<IObjectReadMetaClient> metaClient_;
};

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_OBJECT_CACHE_READ_ACCESS_OBJECT_READ_META_ACCESS_FLOW_H
