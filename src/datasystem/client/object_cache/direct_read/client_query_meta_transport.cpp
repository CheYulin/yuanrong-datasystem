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
 * Description: Client single-shot QueryMeta RPC transport.
 */
#include "datasystem/client/object_cache/direct_read/client_query_meta_transport.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "datasystem/client/object_cache/direct_read/direct_read_flow.h"
#include "datasystem/client/object_cache/direct_read/direct_read_test_hook.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/protos/master_object.stub.rpc.pb.h"

namespace datasystem {
namespace object_cache {
ClientQueryMetaTransport::ClientQueryMetaTransport(RpcCredential cred, Signature *signature, int32_t requestTimeoutMs,
                                                   HostPort clientWorkerAddress)
    : cred_(std::move(cred)),
      signature_(signature),
      requestTimeoutMs_(requestTimeoutMs),
      clientWorkerAddress_(std::move(clientWorkerAddress))
{
}

Status ClientQueryMetaTransport::QueryMetaOnce(const HostPort &metaAddress, const std::vector<std::string> &objectKeys,
                                               int64_t subTimeoutMs, bool enableRedirect, master::QueryMetaRspPb &rsp,
                                               std::vector<RpcMessage> &payloads)
{
    RETURN_RUNTIME_ERROR_IF_NULL(signature_);
    DirectReadTestHook::RecordMetaQuery();
    master::QueryMetaReqPb req;
    *req.mutable_ids() = { objectKeys.begin(), objectKeys.end() };
    req.set_address(clientWorkerAddress_.ToString());
    req.set_sub_timeout(std::min<int64_t>(subTimeoutMs, requestTimeoutMs_));
    req.set_request_id(GetStringUuid());
    req.set_timeout(requestTimeoutMs_);
    req.set_redirect(enableRedirect);
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));

    auto channel = std::make_shared<RpcChannel>(metaAddress, cred_);
    master::MasterOCService_Stub stub(channel, requestTimeoutMs_);
    RpcOptions opts;
    opts.SetTimeout(requestTimeoutMs_);
    Status rc = stub.QueryMeta(opts, req, rsp, payloads);
    if (rc.IsError()) {
        payloads.clear();
        if (rc.GetCode() == K_RPC_DEADLINE_EXCEEDED || rc.GetCode() == K_WORKER_TIMEOUT
            || rc.GetCode() == K_RPC_UNAVAILABLE) {
            return Status(rc.GetCode(), DirectReadFlow::kMetaTimeoutFallbackReason);
        }
        return rc;
    }

    if (DirectReadTestHook::ConsumeSimulateMetaMovingResponse()) {
        rsp.set_meta_is_moving(true);
        auto *info = rsp.add_info();
        info->set_redirect_meta_address(metaAddress.ToString());
        for (const auto &objectKey : objectKeys) {
            *info->add_change_meta_ids() = objectKey;
        }
        return Status::OK();
    }

    if (DirectReadTestHook::SimulateRedirectLoop()) {
        DirectReadTestHook::RecordRedirectRetry();
        auto *info = rsp.add_info();
        info->set_redirect_meta_address(metaAddress.ToString());
        for (const auto &objectKey : objectKeys) {
            *info->add_change_meta_ids() = objectKey;
        }
    }

    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
