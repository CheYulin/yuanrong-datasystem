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
 * Description: Route provider for client direct read backed by ClientHashRingSource.
 */
#include "datasystem/client/object_cache/direct_read/direct_read_route_provider.h"

#include <utility>

#include "datasystem/client/object_cache/direct_read/direct_read_test_hook.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace object_cache {
DirectReadRouteProvider::DirectReadRouteProvider(std::shared_ptr<IClientWorkerApi> workerApi,
                                                 DirectReadRpcAdapter *rpcAdapter)
    : hashRingSourcePtr_(nullptr),
      ownedRingSource_(std::make_unique<ClientHashRingSource>(std::move(workerApi), rpcAdapter))
{
    hashRingSourcePtr_ = ownedRingSource_.get();
}

DirectReadRouteProvider::DirectReadRouteProvider(ClientHashRingSource &sharedRingSource)
    : hashRingSourcePtr_(&sharedRingSource), ownedRingSource_(nullptr)
{
}

ClientHashRingSource &DirectReadRouteProvider::RingSource()
{
    return *hashRingSourcePtr_;
}

Status DirectReadRouteProvider::GetMetaAddress(const GetParam &getParam, HostPort &metaAddress)
{
    CHECK_FAIL_RETURN_STATUS(!getParam.objectKeys.empty(), K_INVALID, "Direct read requires at least one object key");
    return GetMetaAddress(getParam.objectKeys.front(), metaAddress);
}

Status DirectReadRouteProvider::GetMetaAddress(const std::string &objectKey, HostPort &metaAddress)
{
    DirectReadTestHook::RecordRouteQuery();
    return RingSource().GetMetaAddress(objectKey, metaAddress);
}

Status DirectReadRouteProvider::RefreshRouteIfNeeded()
{
    return RingSource().RefreshForRouteLookup();
}

Status DirectReadRouteProvider::RefreshRouteOnClusterEvent()
{
    return RingSource().RefreshOnClusterEvent();
}

ClientHashRingSource &DirectReadRouteProvider::HashRingSourceForTest()
{
    return RingSource();
}
}  // namespace object_cache
}  // namespace datasystem
