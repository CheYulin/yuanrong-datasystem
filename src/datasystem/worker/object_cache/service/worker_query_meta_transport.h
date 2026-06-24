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
 * Description: Worker single-shot QueryMeta transport.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_SERVICE_WORKER_QUERY_META_TRANSPORT_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_SERVICE_WORKER_QUERY_META_TRANSPORT_H

#include <cstdint>
#include <vector>

#include "datasystem/common/object_cache/read_access/query_meta_transport.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
class WorkerOcServiceGetImpl;

class WorkerQueryMetaTransport : public IQueryMetaTransport {
public:
    WorkerQueryMetaTransport(WorkerOcServiceGetImpl *getImpl, bool isFromOtherAz);

    Status QueryMetaOnce(const HostPort &metaAddress, const std::vector<std::string> &objectKeys, int64_t subTimeoutMs,
                         bool enableRedirect, master::QueryMetaRspPb &rsp,
                         std::vector<RpcMessage> &payloads) override;

private:
    WorkerOcServiceGetImpl *getImpl_;
    bool isFromOtherAz_;
};
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_SERVICE_WORKER_QUERY_META_TRANSPORT_H
