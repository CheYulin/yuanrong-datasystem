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

#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_META_AFFINITY_REPLICATE_EXECUTOR_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_META_AFFINITY_REPLICATE_EXECUTOR_H

#include <memory>
#include <string>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/object_cache/meta_affinity_replicate_param.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "datasystem/worker/worker_master_api_manager_base.h"

namespace datasystem {
namespace object_cache {

struct MetaAffinityReplicateContext {
    EtcdClusterManager *etcdCM{ nullptr };
    HostPort localAddress;
    std::shared_ptr<AkSkManager> akSkManager;
    std::shared_ptr<ObjectTable> objectTable;
    std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> workerMasterApiManager;
};

bool ShouldScheduleMetaAffinityReplicate(const std::string &localAddress, const std::string &metaWorkerAddress);

Status ExecuteMetaAffinityReplicate(const MetaAffinityReplicateParam &param, const MetaAffinityReplicateContext &ctx);

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_META_AFFINITY_REPLICATE_EXECUTOR_H
