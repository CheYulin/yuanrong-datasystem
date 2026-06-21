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
 * Description: Route provider for client direct read.
 */
#include "datasystem/client/object_cache/direct_read/direct_read_route_provider.h"

#include <utility>

#include "datasystem/client/object_cache/direct_read/direct_read_test_hook.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace object_cache {
DirectReadRouteProvider::DirectReadRouteProvider(std::shared_ptr<IClientWorkerApi> workerApi)
    : workerApi_(std::move(workerApi))
{
}

Status DirectReadRouteProvider::GetMetaAddress(const GetParam &getParam, HostPort &metaAddress) const
{
    (void)getParam;
    DirectReadTestHook::RecordRouteQuery();
    metaAddress = workerApi_->hostPort_;
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
