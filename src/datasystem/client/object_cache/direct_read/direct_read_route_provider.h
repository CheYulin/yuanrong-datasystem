#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_DIRECT_READ_ROUTE_PROVIDER_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_READ_DIRECT_READ_ROUTE_PROVIDER_H
#include <memory>
#include "datasystem/client/object_cache/client_worker_api/iclient_worker_api.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"
namespace datasystem { namespace object_cache {
class DirectReadRouteProvider {
public:
    explicit DirectReadRouteProvider(std::shared_ptr<IClientWorkerApi> workerApi);
    Status GetMetaAddress(const GetParam &getParam, HostPort &metaAddress) const;
private:
    std::shared_ptr<IClientWorkerApi> workerApi_;
};
}} 
#endif
