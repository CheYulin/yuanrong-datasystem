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
 * Description: Minimal route provider for client direct read.
 */
#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_DIRECT_READ_ROUTE_PROVIDER_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_DIRECT_READ_ROUTE_PROVIDER_H

#include <memory>

#include "datasystem/client/object_cache/client_worker_api/iclient_worker_api.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
class DirectReadRouteProvider {
public:
    explicit DirectReadRouteProvider(std::shared_ptr<IClientWorkerApi> workerApi);

    Status GetMetaAddress(const GetParam &getParam, HostPort &metaAddress) const;

private:
    std::shared_ptr<IClientWorkerApi> workerApi_;
};
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_DIRECT_READ_ROUTE_PROVIDER_H
