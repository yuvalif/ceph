#pragma once

#include "rgw_op.h"

class DoutPrefixProvider;

namespace rgw::lua {

class RGWObjFilter {
  const DoutPrefixProvider* const dpp;
  CephContext* const cct;
  const std::string script;

public:
  RGWObjFilter(const DoutPrefixProvider *dpp,
                         CephContext* cct,
                         const std::string& script) : 
    dpp(dpp), cct(cct), script(script) {}

  int execute(bufferlist& bl) const;
};

class RGWGetObjFilter : public RGWGetObj_Filter {
  const RGWObjFilter filter;

public:
  RGWGetObjFilter(const DoutPrefixProvider *dpp,
      CephContext* cct,
      const std::string& script,
      RGWGetObj_Filter* next) : RGWGetObj_Filter(next), filter(dpp, cct, script) 
  {}

  ~RGWGetObjFilter() override = default;

  int handle_data(bufferlist& bl,
                  off_t bl_ofs,
                  off_t bl_len) override;

};

class RGWPutObjFilter : public rgw::putobj::Pipe {
  const RGWObjFilter filter;

public:
  RGWPutObjFilter(const DoutPrefixProvider *dpp,
      CephContext* cct,
      const std::string& script,
      rgw::sal::DataProcessor* next) : rgw::putobj::Pipe(next), filter(dpp, cct, script) 
  {}

  ~RGWPutObjFilter() override = default;

  int process(bufferlist&& data, uint64_t logical_offset) override;
};
}

