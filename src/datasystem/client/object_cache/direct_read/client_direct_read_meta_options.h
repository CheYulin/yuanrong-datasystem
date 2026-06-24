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
 * Description: Client QueryMeta orchestration options for direct read.
 */
#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_CLIENT_DIRECT_READ_META_OPTIONS_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_CLIENT_DIRECT_READ_META_OPTIONS_H

#include <functional>

#include "datasystem/common/object_cache/read_access/query_meta_orchestrating_meta_client.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
class DirectReadRouteProvider;

QueryMetaOrchestratingMetaClient::Options BuildClientDirectReadMetaOptions(
    DirectReadRouteProvider *routeProvider, std::function<Status()> refreshRoute);
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_CLIENT_DIRECT_READ_META_OPTIONS_H
