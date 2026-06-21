#include "datasystem/client/object_cache/direct_read/direct_read_route_provider.h"
#include <utility>
#include "datasystem/client/object_cache/direct_read/direct_read_test_hook.h"
#include "datasystem/common/util/status_helper.h"
namespace datasystem { namespace object_cache {
DirectReadRouteProvider::DirectReadRouteProvider(std::shared_ptr<IClientWorkerApi> workerApi) : workerApi_(std::move(workerApi)) {}
Status DirectReadRouteProvider::GetMetaAddress(const GetParam &getParam, HostPort &metaAddress) const {
    (void)getParam; DirectReadTestHook::RecordRouteQuery(); metaAddress = workerApi_->hostPort_; return Status::OK();
}
}} 
