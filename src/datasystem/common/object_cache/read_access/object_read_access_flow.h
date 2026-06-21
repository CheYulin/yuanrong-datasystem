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
 * Description: Shared object read access ports for client and worker reuse.
 */
#ifndef DATASYSTEM_COMMON_OBJECT_CACHE_READ_ACCESS_OBJECT_READ_ACCESS_FLOW_H
#define DATASYSTEM_COMMON_OBJECT_CACHE_READ_ACCESS_OBJECT_READ_ACCESS_FLOW_H

#include <memory>
#include <string>
#include <vector>

#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
class IObjectReadRouteProvider {
public:
    virtual ~IObjectReadRouteProvider() = default;
    virtual Status GetMetaAddress(const std::string &objectKey, HostPort &metaAddress) = 0;
    virtual Status RefreshRouteIfNeeded() = 0;
};

class IObjectReadMetaClient {
public:
    virtual ~IObjectReadMetaClient() = default;
    virtual Status QueryMeta(const HostPort &metaAddress, const GetParam &getParam, master::QueryMetaRspPb &rsp,
                             std::vector<RpcMessage> &payloads) = 0;
};

class IObjectReadDataClient {
public:
    virtual ~IObjectReadDataClient() = default;
    virtual Status ReadData(const master::QueryMetaInfoPb &queryMeta, const GetParam &getParam, size_t objectIndex,
                            GetObjectRemoteRspPb &rsp, std::vector<RpcMessage> &payloads) = 0;
};

// Task 3.6 will move worker/client read orchestration into ObjectReadAccessFlow using the ports above.
class ObjectReadAccessFlow {
public:
    ObjectReadAccessFlow(std::shared_ptr<IObjectReadRouteProvider> routeProvider,
                         std::shared_ptr<IObjectReadMetaClient> metaClient,
                         std::shared_ptr<IObjectReadDataClient> dataClient);
};

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_OBJECT_CACHE_READ_ACCESS_OBJECT_READ_ACCESS_FLOW_H
