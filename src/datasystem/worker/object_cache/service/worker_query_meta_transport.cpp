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
#include "datasystem/worker/object_cache/service/worker_query_meta_transport.h"

#include "datasystem/common/util/status_helper.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_get_impl.h"

namespace datasystem {
namespace object_cache {
WorkerQueryMetaTransport::WorkerQueryMetaTransport(WorkerOcServiceGetImpl *getImpl, bool isFromOtherAz)
    : getImpl_(getImpl), isFromOtherAz_(isFromOtherAz)
{
}

Status WorkerQueryMetaTransport::QueryMetaOnce(const HostPort &metaAddress, const std::vector<std::string> &objectKeys,
                                               int64_t subTimeoutMs, bool enableRedirect, master::QueryMetaRspPb &rsp,
                                               std::vector<RpcMessage> &payloads)
{
    RETURN_RUNTIME_ERROR_IF_NULL(getImpl_);
    return getImpl_->QueryMetaOnceAtMaster(metaAddress, static_cast<uint64_t>(subTimeoutMs), objectKeys,
                                           isFromOtherAz_, enableRedirect, rsp, payloads);
}
}  // namespace object_cache
}  // namespace datasystem
