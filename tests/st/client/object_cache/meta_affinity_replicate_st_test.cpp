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

#include <algorithm>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/object_client.h"
#include "datasystem/protos/hash_ring.pb.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/utils/service_discovery.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "oc_client_common.h"

DS_DECLARE_bool(enable_meta_affinity_replicate);
DS_DECLARE_bool(enable_distributed_master);

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
        opts.workerGflagParams += " -enable_meta_affinity_replicate=true -enable_data_replication=false ";

        std::string hostIp = "127.0.0.1";
        for (size_t i = 0; i < opts.numWorkers; i++) {
            HostPort hostPort(hostIp, GetFreePort());
            opts.workerConfigs.emplace_back(hostPort);
            std::string envName = "meta_affinity_host_id_env" + std::to_string(i);
            std::string envVal = "meta_affinity_host_id" + std::to_string(i);
            ASSERT_EQ(setenv(envName.c_str(), envVal.c_str(), 1), 0);
            opts.workerSpecifyGflagParams[i] = FormatString("-host_id_env_name=%s", envName);
        }
    }

protected:
    void SetUp() override
    {
        OCClientCommon::SetUp();
        FLAGS_enable_meta_affinity_replicate = true;
        FLAGS_enable_distributed_master = true;
        db_ = InitTestEtcdInstance();
        SetWorkerHashInjection();
        std::this_thread::sleep_for(std::chrono::seconds(2));
        GetWorkerUuids(db_.get(), uuidMap_);
        InitMasterApis();
    }

    void InitMasterApis()
    {
        hostPort_.ParseString("127.0.0.1:" + std::to_string(GetFreePort()));
        akSkManager_ = std::make_shared<AkSkManager>();
        akSkManager_->SetClientAkSk(accessKey_, secretKey_);
        RpcStubCacheMgr::Instance().Init(100);
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, worker0Address_));
        DS_ASSERT_OK(cluster_->GetWorkerAddr(1, worker1Address_));
        worker0MasterApi_ =
            std::make_unique<worker::WorkerRemoteMasterOCApi>(worker0Address_, hostPort_, akSkManager_);
        DS_ASSERT_OK(worker0MasterApi_->Init());
        worker1MasterApi_ =
            std::make_unique<worker::WorkerRemoteMasterOCApi>(worker1Address_, hostPort_, akSkManager_);
        DS_ASSERT_OK(worker1MasterApi_->Init());
    }

    Status QueryMetaInfo(const std::string &objectKey, master::QueryMetaInfoPb &queryMeta)
    {
        master::QueryMetaReqPb queryReq;
        queryReq.add_ids(objectKey);
        queryReq.set_address(hostPort_.ToString());
        std::vector<RpcMessage> payloads;
        for (auto *api : { worker0MasterApi_.get(), worker1MasterApi_.get() }) {
            master::QueryMetaRspPb queryRsp;
            payloads.clear();
            Status status = api->QueryMeta(queryReq, 0, queryRsp, payloads);
            if (status.IsOk() && queryRsp.query_metas_size() > 0) {
                queryMeta = queryRsp.query_metas(0);
                return Status::OK();
            }
        }
        return Status(K_NOT_FOUND, "meta not found");
    }

    void WaitUntilPrimaryIs(const std::string &objectKey, const std::string &expectedPrimary)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        std::string primary;
        do {
            master::QueryMetaInfoPb queryMeta;
            if (QueryMetaInfo(objectKey, queryMeta).IsOk()
                && queryMeta.meta().primary_address() == expectedPrimary) {
                return;
            }
            if (QueryMetaInfo(objectKey, queryMeta).IsOk()) {
                primary = queryMeta.meta().primary_address();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        } while (std::chrono::steady_clock::now() < deadline);
        FAIL() << "primary did not become " << expectedPrimary << ", last=" << primary;
    }

    void GetHashKeyForMetaOwner(uint32_t workerIndex, std::string &objectKey)
    {
        objectKey = GetObjectKeyHashToWorker(db_.get(), workerIndex);
    }

    void InitRemoteOnlyAffinityClient(std::shared_ptr<ObjectClient> &client)
    {
        ASSERT_EQ(setenv("meta_affinity_remote_client_env", "remote_only_client_host", 1), 0);
        ServiceDiscoveryOptions sdOpts;
        sdOpts.etcdAddress = cluster_->GetEtcdAddrs();
        sdOpts.hostIdEnvName = "meta_affinity_remote_client_env";
        sdOpts.affinityPolicy = ServiceAffinityPolicy::PREFERRED_SAME_NODE;
        auto serviceDiscovery = std::make_shared<ServiceDiscovery>(sdOpts);
        DS_ASSERT_OK(serviceDiscovery->Init());

        HostPort workerAddr;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddr));
        ConnectOptions connectOptions;
        connectOptions.host = workerAddr.Host();
        connectOptions.port = workerAddr.Port();
        connectOptions.connectTimeoutMs = 60000;
        connectOptions.accessKey = accessKey_;
        connectOptions.secretKey = secretKey_;
        connectOptions.enableCrossNodeConnection = true;
        connectOptions.serviceDiscovery = serviceDiscovery;

        client = std::make_shared<ObjectClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    void AssertPrimaryIsImmediately(const std::string &objectKey, const std::string &expectedPrimary)
    {
        master::QueryMetaInfoPb queryMeta;
        DS_ASSERT_OK(QueryMetaInfo(objectKey, queryMeta));
        EXPECT_EQ(queryMeta.meta().primary_address(), expectedPrimary);
    }

    void AssertLocationsContainBothWorkers(const std::vector<std::string> &locations)
    {
        const std::string worker0Uuid = GetWorkerUuid(0, uuidMap_);
        const std::string worker1Uuid = GetWorkerUuid(1, uuidMap_);
        EXPECT_NE(std::find(locations.begin(), locations.end(), worker0Uuid), locations.end())
            << "origin worker should keep a local copy location";
        EXPECT_NE(std::find(locations.begin(), locations.end(), worker1Uuid), locations.end())
            << "meta owner should hold primary location";
    }

    std::unique_ptr<EtcdStore> db_;
    HostPort hostPort_;
    HostPort worker0Address_;
    HostPort worker1Address_;
    std::unordered_map<HostPort, std::string> uuidMap_;
    std::shared_ptr<AkSkManager> akSkManager_;
    std::unique_ptr<worker::WorkerRemoteMasterOCApi> worker0MasterApi_;
    std::unique_ptr<worker::WorkerRemoteMasterOCApi> worker1MasterApi_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(MetaAffinityReplicateStTest, ColocatePrimaryWithMetaOwnerAndReadLocalCopy)
{
    std::shared_ptr<ObjectClient> client0;
    InitTestClient(0, client0);
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(1, client1);

    std::string objectKey;
    GetHashKeyForMetaOwner(1, objectKey);
    const std::string payload = RandomData().GetRandomString(1024);

    CreateParam param;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client0->Create(objectKey, payload.size(), param, buffer));
    DS_ASSERT_OK(buffer->WLatch());
    DS_ASSERT_OK(buffer->MemoryCopy(payload.data(), payload.size()));
    DS_ASSERT_OK(buffer->Seal({}));
    DS_ASSERT_OK(buffer->UnWLatch());

    // Meta owner is worker1 (hash injection); after async replicate primary must colocate with meta owner.
    WaitUntilPrimaryIs(objectKey, worker1Address_.ToString());

    master::QueryMetaInfoPb queryMeta;
    DS_ASSERT_OK(QueryMetaInfo(objectKey, queryMeta));
    EXPECT_EQ(queryMeta.meta().primary_address(), worker1Address_.ToString());
    // Remote fetch hint should prefer primary (meta owner), not the origin local copy.
    EXPECT_EQ(queryMeta.address(), worker1Address_.ToString());

    std::vector<ObjMetaInfo> objMetas;
    DS_ASSERT_OK(client0->GetObjMetaInfo("", { objectKey }, objMetas));
    ASSERT_EQ(objMetas.size(), 1u);
    EXPECT_EQ(objMetas[0].objSize, payload.size());
    AssertLocationsContainBothWorkers(objMetas[0].locations);

    // Worker0 still serves from its local copy when data is present locally.
    std::vector<Optional<Buffer>> worker0Buffers;
    DS_ASSERT_OK(client0->Get({ objectKey }, 0, worker0Buffers));
    ASSERT_EQ(worker0Buffers.size(), 1u);
    ASSERT_TRUE(worker0Buffers[0]);
    AssertBufferEqual(*worker0Buffers[0], payload);

    // Worker1 reads primary copy locally after colocation.
    std::vector<Optional<Buffer>> worker1Buffers;
    DS_ASSERT_OK(client1->Get({ objectKey }, 0, worker1Buffers));
    ASSERT_EQ(worker1Buffers.size(), 1u);
    ASSERT_TRUE(worker1Buffers[0]);
    AssertBufferEqual(*worker1Buffers[0], payload);

    // Invalidate local cache on worker0; next Get should pull from primary on worker1.
    DS_ASSERT_OK(worker0Buffers[0]->InvalidateBuffer());
    worker0Buffers.clear();
    DS_ASSERT_OK(client0->Get({ objectKey }, 0, worker0Buffers));
    ASSERT_EQ(worker0Buffers.size(), 1u);
    ASSERT_TRUE(worker0Buffers[0]);
    AssertBufferEqual(*worker0Buffers[0], payload);
}

TEST_F(MetaAffinityReplicateStTest, RemoteOnlyClientPutDirectlyOnMetaOwner)
{
    std::string objectKey;
    GetHashKeyForMetaOwner(1, objectKey);
    const std::string payload = RandomData().GetRandomString(1024);

    std::shared_ptr<ObjectClient> remoteClient;
    InitRemoteOnlyAffinityClient(remoteClient);
    CreateParam param;
    DS_ASSERT_OK(remoteClient->Put(objectKey, reinterpret_cast<const uint8_t *>(payload.data()), payload.size(), param));

    // Remote-only client writes to hash-routed meta owner directly; no async replicate from gateway worker.
    AssertPrimaryIsImmediately(objectKey, worker1Address_.ToString());

    std::vector<ObjMetaInfo> objMetas;
    DS_ASSERT_OK(remoteClient->GetObjMetaInfo("", { objectKey }, objMetas));
    ASSERT_EQ(objMetas.size(), 1u);
    EXPECT_EQ(objMetas[0].objSize, payload.size());
    ASSERT_EQ(objMetas[0].locations.size(), 1u);
    EXPECT_EQ(objMetas[0].locations[0], GetWorkerUuid(1, uuidMap_));

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(remoteClient->Get({ objectKey }, 0, buffers));
    ASSERT_EQ(buffers.size(), 1u);
    ASSERT_TRUE(buffers[0]);
    AssertBufferEqual(*buffers[0], payload);
}

}  // namespace st
}  // namespace datasystem
