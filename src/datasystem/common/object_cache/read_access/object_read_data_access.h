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
#ifndef DATASYSTEM_COMMON_OBJECT_CACHE_READ_ACCESS_OBJECT_READ_DATA_ACCESS_H
#define DATASYSTEM_COMMON_OBJECT_CACHE_READ_ACCESS_OBJECT_READ_DATA_ACCESS_H

#include <cstdint>
#include <vector>

#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
enum class ObjectReadDataPath {
    kInlineFromMeta,
    kRemote,
};

enum class ObjectReadL0Outcome {
    kInlineHit,
    kInlineNotOffered,
    kInlineExtractFailed,
    kRemote,
};

struct ObjectReadSpec {
    uint64_t readOffset = 0;
    uint64_t readSize = 0;
};

ObjectReadDataPath PlanObjectReadDataPath(const master::QueryMetaInfoPb &queryMeta);

Status ExtractInlinePayloads(const master::QueryMetaInfoPb &queryMeta, std::vector<RpcMessage> &metaPayloadPool,
                             std::vector<RpcMessage> &outPayloads, ObjectReadL0Outcome *outcome = nullptr);

class IObjectReadRemoteDataClient {
public:
    virtual ~IObjectReadRemoteDataClient() = default;
    virtual Status FetchRemote(const master::QueryMetaInfoPb &queryMeta, const ObjectReadSpec &spec,
                               GetObjectRemoteRspPb &rsp, std::vector<RpcMessage> &payloads) = 0;
};

Status FetchObjectReadData(const master::QueryMetaInfoPb &queryMeta, const ObjectReadSpec &spec,
                           std::vector<RpcMessage> &metaPayloadPool, IObjectReadRemoteDataClient &remoteClient,
                           std::vector<RpcMessage> &outPayloads, GetObjectRemoteRspPb *remoteRspOut = nullptr,
                           ObjectReadL0Outcome *l0Outcome = nullptr);

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_OBJECT_CACHE_READ_ACCESS_OBJECT_READ_DATA_ACCESS_H
