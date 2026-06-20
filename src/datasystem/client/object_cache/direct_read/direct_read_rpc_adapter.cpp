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
 * Description: Minimal RPC adapter for client direct read.
 */
#include "datasystem/client/object_cache/direct_read/direct_read_rpc_adapter.h"

#include <algorithm>
#include <memory>

#include "datasystem/client/object_cache/direct_read/direct_read_test_hook.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/protos/master_object.stub.rpc.pb.h"

namespace datasystem {
namespace object_cache {
DirectReadRpcAdapter::DirectReadRpcAdapter(RpcCredential cred, Signature *signature, int32_t requestTimeoutMs)
    : cred_(std::move(cred)), signature_(signature), requestTimeoutMs_(requestTimeoutMs)
{
}

Status DirectReadRpcAdapter::QueryMeta(const HostPort &metaAddress, const HostPort &clientWorkerAddress,
                                       const GetParam &getParam, master::QueryMetaRspPb &rsp,
                                       std::vector<RpcMessage> &payloads) const
{
    RETURN_RUNTIME_ERROR_IF_NULL(signature_);
    DirectReadTestHook::RecordMetaQuery();
    master::QueryMetaReqPb req;
    *req.mutable_ids() = { getParam.objectKeys.begin(), getParam.objectKeys.end() };
    req.set_address(clientWorkerAddress.ToString());
    req.set_sub_timeout(std::min<int64_t>(getParam.subTimeoutMs, requestTimeoutMs_));
    req.set_request_id(GetStringUuid());
    req.set_timeout(requestTimeoutMs_);
    req.set_redirect(true);
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));

    auto channel = std::make_shared<RpcChannel>(metaAddress, cred_);
    master::MasterOCService_Stub stub(channel, requestTimeoutMs_);
    RpcOptions opts;
    opts.SetTimeout(requestTimeoutMs_);
    return stub.QueryMeta(opts, req, rsp, payloads);
}
}  // namespace object_cache
}  // namespace datasystem
