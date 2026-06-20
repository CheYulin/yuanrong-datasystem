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

namespace datasystem {
namespace object_cache {
Status DirectReadFlow::Get(const GetParam &getParam, std::vector<std::shared_ptr<Buffer>> &buffers)
{
    (void)getParam;
    (void)buffers;
    return Status(K_NOT_SUPPORTED, kNotImplementedFallbackReason);
}
}  // namespace object_cache
}  // namespace datasystem
