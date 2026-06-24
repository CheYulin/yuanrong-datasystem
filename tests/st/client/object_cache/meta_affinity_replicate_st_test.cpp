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

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/object_client.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "oc_client_common.h"

namespace datasystem {
namespace st {

class MetaAffinityReplicateStTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 2;
        opts.numOBS = 0;
        opts.enableSpill = false;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams += " -enable_meta_affinity_replicate=true -enable_data_replication=true ";
    }

protected:
    void SetUp() override
    {
        OCClientCommon::SetUp();
        StartWorkerAndWaitReady({ 0, 1 });
        db_ = InitTestEtcdInstance();
        InitMasterApis();
    }

    void InitMasterApis()
    {
        hostPort_.ParseString("127.0.0.1:" + std::to_string(GetFreePort()));
        akSkManager_ = std::make_shared<AkSkManager>();
        akSkManager_->SetClientAkSk(accessKey_, secretKey_);
        RpcStubCacheMgr::Instance().Init(100);
        DS_ASSERT_OK(cluster_->GetWorkerAddr(1, worker1Address_));
        worker1MasterApi_ =
            std::make_unique<worker::WorkerRemoteMasterOCApi>(worker1Address_, hostPort_, akSkManager_);
        DS_ASSERT_OK(worker1MasterApi_->Init());
    }

    Status QueryPrimaryAddress(const std::string &objectKey, std::string &primaryAddress)
    {
        master::QueryMetaReqPb queryReq;
        master::QueryMetaRspPb queryRsp;
        queryReq.add_ids(objectKey);
        queryReq.set_address(hostPort_.ToString());
        std::vector<RpcMessage> payloads;
        RETURN_IF_NOT_OK(worker1MasterApi_->QueryMeta(queryReq, 0, queryRsp, payloads));
        if (queryRsp.query_metas_size() == 0) {
            return Status(K_NOT_FOUND, "meta not found");
        }
        primaryAddress = queryRsp.query_metas(0).meta().primary_address();
        return Status::OK();
    }

    void WaitUntilPrimaryIs(const std::string &objectKey, const std::string &expectedPrimary)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        std::string primary;
        do {
            if (QueryPrimaryAddress(objectKey, primary).IsOk() && primary == expectedPrimary) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        } while (std::chrono::steady_clock::now() < deadline);
        FAIL() << "primary did not become " << expectedPrimary << ", last=" << primary;
    }

    std::unique_ptr<EtcdStore> db_;
    HostPort hostPort_;
    HostPort worker1Address_;
    std::shared_ptr<AkSkManager> akSkManager_;
    std::unique_ptr<worker::WorkerRemoteMasterOCApi> worker1MasterApi_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(MetaAffinityReplicateStTest, SameNodePutSwapsPrimaryToMetaOwner)
{
    std::shared_ptr<ObjectClient> client0;
    InitTestClient(0, client0);

    const std::string objectKey = GetObjectKeyHashToWorker(db_.get(), 1);
    const std::string payload = RandomData().GetRandomString(1024);

    CreateParam param;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client0->Create(objectKey, payload.size(), param, buffer));
    DS_ASSERT_OK(buffer->WLatch());
    DS_ASSERT_OK(buffer->MemoryCopy(payload.data(), payload.size()));
    DS_ASSERT_OK(buffer->Seal({}));
    DS_ASSERT_OK(buffer->UnWLatch());

    WaitUntilPrimaryIs(objectKey, worker1Address_.ToString());

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client0->Get({ objectKey }, 0, buffers));
    ASSERT_EQ(buffers.size(), 1u);
    ASSERT_TRUE(buffers[0].has_value());
    AssertBufferEqual(*buffers[0], payload);
}

}  // namespace st
}  // namespace datasystem
