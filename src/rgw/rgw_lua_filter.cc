#include "rgw_lua_filter.h"
#include "rgw_lua_utils.h"
#include <lua.hpp>

namespace rgw::lua {

void push_bufferlist_byte(lua_State* L, bufferlist::iterator& it) {
    char byte[1];
    it.copy(1, byte);
    lua_pushlstring(L, byte, 1);
}

struct BufferlistMetaTable : public EmptyMetaTable {

  static std::string TableName() {return "Data";}
  static std::string Name() {return TableName() + "Meta";}
  
  static int IndexClosure(lua_State* L) {
    auto bl = reinterpret_cast<bufferlist*>(lua_touserdata(L, lua_upvalueindex(1)));
    auto dpp = reinterpret_cast<DoutPrefixProvider*>(lua_touserdata(L, lua_upvalueindex(2)));
    const auto index = luaL_checkinteger(L, 2);
    ldpp_dout(dpp, 20) << "Lua: BufferlistMetaTable::IndexClosure() called with index " << index << dendl;
    if (index <= 0 || index > bl->length()) {
      // lua arrays start from 1
      lua_pushnil(L);
      return ONE_RETURNVAL;
    }
    auto it = bl->begin(index-1);
    if (it != bl->end()) {
      push_bufferlist_byte(L, it);
    } else {
      lua_pushnil(L);
    }
    
    return ONE_RETURNVAL;
  }

  static int PairsClosure(lua_State* L) {
    auto bl = reinterpret_cast<bufferlist*>(lua_touserdata(L, lua_upvalueindex(1)));
    auto dpp = reinterpret_cast<DoutPrefixProvider*>(lua_touserdata(L, lua_upvalueindex(2)));
    ceph_assert(bl);
    lua_pushlightuserdata(L, bl);
    lua_pushlightuserdata(L, dpp);
    lua_pushcclosure(L, stateless_iter, TWO_UPVALS); // push the stateless iterator function
    lua_pushnil(L);                                 // indicate this is the first call
    // return stateless_iter, nil
    ldpp_dout(dpp, 20) << "Lua: BufferlistMetaTable::PairsClosure called" << dendl;

    return TWO_RETURNVALS;
  }
  
  static int stateless_iter(lua_State* L) {
    // based on: http://lua-users.org/wiki/GeneralizedPairsAndIpairs
    auto bl = reinterpret_cast<bufferlist*>(lua_touserdata(L, lua_upvalueindex(1)));
    auto dpp = reinterpret_cast<DoutPrefixProvider*>(lua_touserdata(L, lua_upvalueindex(2)));
    lua_Integer index;
    if (lua_isnil(L, -1)) {
      index = 1;
      ldpp_dout(dpp, 20) << "Lua: BufferlistMetaTable::stateless_iter() first call index =" << index << dendl;
    } else {
      index = luaL_checkinteger(L, -1) + 1;
    }
    ldpp_dout(dpp, 20) << "Lua: BufferlistMetaTable::stateless_iter() index = " << index << dendl;

    // lua arrays start from 1
    auto it = bl->begin(index-1);

    if (index > bl->length()) {
      // index of the last element was provided
      ldpp_dout(dpp, 20) << "Lua: BufferlistMetaTable::stateless_iter() index of last element was provided" << dendl;
      lua_pushnil(L);
      lua_pushnil(L);
      // return nil, nil
    } else {
      ldpp_dout(dpp, 20) << "Lua: BufferlistMetaTable::stateless_iter() returning index " << index << " and value" << dendl;
      lua_pushinteger(L, index);
      push_bufferlist_byte(L, it);
      // return key, value
    }

    return TWO_RETURNVALS;
  }
  
  static int LenClosure(lua_State* L) {
    const auto bl = reinterpret_cast<bufferlist*>(lua_touserdata(L, lua_upvalueindex(1)));
    auto dpp = reinterpret_cast<DoutPrefixProvider*>(lua_touserdata(L, lua_upvalueindex(2)));

    ldpp_dout(dpp, 20) << "Lua: BufferlistMetaTable::LenClosure() returning length " << bl->length() << " and value" << dendl;
    lua_pushinteger(L, bl->length());

    return ONE_RETURNVAL;
  }
};

int RGWObjFilter::execute(bufferlist& bl) const {
  auto L = luaL_newstate();
  lua_state_guard lguard(L);

  open_standard_libs(L);

  create_debug_action(L, cct);  

  create_metatable<BufferlistMetaTable>(L, true, &bl, const_cast<DoutPrefixProvider*>(dpp));

  try {
    // execute the lua script
    if (luaL_dostring(L, script.c_str()) != LUA_OK) {
      const std::string err(lua_tostring(L, -1));
      ldpp_dout(dpp, 1) << "Lua ERROR: " << err << dendl;
    }
  } catch (const std::runtime_error& e) {
    ldpp_dout(dpp, 1) << "Lua ERROR: " << e.what() << dendl;
  }
  ldpp_dout(dpp, 20) << "Successfully executed Lua script in 'data' context" << dendl;

  return 0;
}

int RGWGetObjFilter::handle_data(bufferlist& bl,
                  off_t bl_ofs,
                  off_t bl_len) {
  const auto rc = filter.execute(bl);
  if (rc < 0) {
    return rc;
  }
  return RGWGetObj_Filter::handle_data(bl, bl_ofs, bl_len);
}

int RGWPutObjFilter::process(bufferlist&& data, uint64_t logical_offset) {
  const auto rc = filter.execute(data);
  if (rc < 0) {
    return rc;
  }
  return rgw::putobj::Pipe::process(std::move(data), logical_offset); 
}

}

