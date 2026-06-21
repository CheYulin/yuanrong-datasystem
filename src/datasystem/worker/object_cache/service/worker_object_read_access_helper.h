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
 * Description: Worker helper for ObjectReadAccessFlow meta phase.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_SERVICE_WORKER_OBJECT_READ_ACCESS_HELPER_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_SERVICE_WORKER_OBJECT_READ_ACCESS_HELPER_H

#include <cstdint>
#include <string>
#include <vector>

#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
class WorkerOcServiceGetImpl;

Status QueryMetaGroupUsingSharedFlow(WorkerOcServiceGetImpl &getImpl, const HostPort &masterAddress,
                                     const std::vector<std::string> &objectKeys, bool isFromOtherAz,
                                     uint64_t subTimeout, master::QueryMetaRspPb &rsp,
                                     std::vector<RpcMessage> &payloads);
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_SERVICE_WORKER_OBJECT_READ_ACCESS_HELPER_H
