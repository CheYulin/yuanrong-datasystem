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

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace object_cache {
DirectReadFlow::DirectReadFlow(std::shared_ptr<IClientWorkerApi> workerApi, RpcCredential cred, Signature *signature,
                               int32_t requestTimeoutMs)
    : workerApi_(std::move(workerApi)),
      rpcAdapter_(std::move(cred), signature, requestTimeoutMs),
      routeProvider_(workerApi_, &rpcAdapter_),
      routeAdapter_(std::make_shared<DirectReadRouteProviderAdapter>(&routeProvider_)),
      metaAdapter_(std::make_shared<DirectReadMetaClientAdapter>(&rpcAdapter_, workerApi_->hostPort_)),
      dataAdapter_(std::make_shared<DirectReadDataClientAdapter>()),
      accessFlow_(routeAdapter_, metaAdapter_, dataAdapter_)
{
}

Status DirectReadFlow::Get(const GetParam &getParam, std::vector<std::shared_ptr<Buffer>> &buffers)
{
    (void)buffers;
    ObjectReadAccessRequest request;
    request.objectKeys.assign(getParam.objectKeys.begin(), getParam.objectKeys.end());
    request.subTimeoutMs = getParam.subTimeoutMs;
    request.clientWorkerAddress = workerApi_->hostPort_;

    ObjectReadAccessMetaResult metaResult;
    RETURN_IF_NOT_OK(accessFlow_.ExecuteMetaPhase(request, metaResult));
    return Status(K_NOT_SUPPORTED, kNotImplementedFallbackReason);
}
}  // namespace object_cache
}  // namespace datasystem
