#pragma once

#include <string>
#include "include/common_fwd.h"

struct lua_State;
class req_state;
class RGWREST;
class OpsLogSink;
namespace rgw::sal {
  class Store;
}
namespace rgw::lua {
  class Background;
}

namespace rgw::lua::request {

// create the request metatable
void create_top_metatable(lua_State* L, req_state* s, const char* op_name);

// execute a lua script in the Request context
int execute(
    rgw::sal::Store* store,
    RGWREST* rest,
    OpsLogSink* olog,
    req_state *s, 
    const char* op_name,
    const std::string& script,
    rgw::lua::Background* background = nullptr);

}

