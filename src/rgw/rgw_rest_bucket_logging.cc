// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/dout.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_arn.h"
#include "rgw_auth_s3.h"
#include "rgw_url.h"
#include "rgw_bucket_logging.h"
#include "rgw_rest_bucket_logging.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

namespace {
  int verify_bucket_logging_params(const DoutPrefixProvider* dpp,  const req_state* s) {
    bool exists;
    const auto no_value = s->info.args.get("logging", &exists);
    if (!exists) {
      ldpp_dout(dpp, 1) << "missing required param 'logging'" << dendl;
      return -EINVAL;
    } 
    if (no_value.length() > 0) {
      ldpp_dout(dpp, 1) << "param 'logging' should not have any value" << dendl;
      return -EINVAL;
    }
    if (s->bucket_name.empty()) {
      ldpp_dout(dpp, 1) << "request must be on a bucket" << dendl;
      return -EINVAL;
    }
    return 0;
  }
}

// GET /<bucket name>/?logging
// reply is XML encoded
class RGWGetBucketLoggingOp : public RGWOp {
  rgw_bucket_logging configurations;

public:
  int verify_permission(optional_yield y) override {
    auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
    if (has_s3_resource_tag)
      rgw_iam_add_buckettags(this, s);

    return verify_bucket_owner_or_policy(s, rgw::IAM::s3GetBucketLogging);
  }

  void execute(optional_yield y) override {
    op_ret = verify_bucket_logging_params(this, s);
    if (op_ret < 0) {
      return;
    }

    std::unique_ptr<rgw::sal::Bucket> bucket;
    op_ret = driver->load_bucket(this, rgw_bucket(s->bucket_tenant, s->bucket_name),
                                 &bucket, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "failed to get bucket '" << 
        (s->bucket_tenant.empty() ? s->bucket_name : s->bucket_tenant + ":" + s->bucket_name) << 
        "' info, ret = " << op_ret << dendl;
      return;
    }
    if (auto iter = bucket->get_attrs().find(RGW_ATTR_BUCKET_LOGGING); iter != bucket->get_attrs().end()) {
      try {
        configurations.enabled = true;
        decode(configurations, iter->second);
      } catch (buffer::error& err) {
        ldpp_dout(this, 1) << "failed to decode attribute '" << RGW_ATTR_BUCKET_LOGGING 
          << "'. error: " << err.what() << dendl;
        op_ret = -EIO;
        return;
      }
    } else {
      ldpp_dout(this, 10) << "no logging configuration to bucket '" << bucket->get_name() << "'" << dendl;
      return;
    }
    ldpp_dout(this, 10) << "found logging configuration to bucket '" << bucket->get_name() << "'" << dendl;
  }

  void send_response() override {
    dump_errno(s);
    end_header(s, this, to_mime_type(s->format));
    dump_start(s);

    s->formatter->open_object_section_in_ns("BucketLoggingStatus", XMLNS_AWS_S3);
    configurations.dump_xml(s->formatter);
    s->formatter->close_section();
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
  const char* name() const override { return "get_bucket_logging"; }
  RGWOpType get_type() override { return RGW_OP_GET_BUCKET_LOGGING; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

// PUT /<bucket name>/?logging
// actual configuration is XML encoded in the body of the message
class RGWPutBucketLoggingOp : public RGWDefaultResponseOp {
  int verify_permission(optional_yield y) override {
    auto [has_s3_existing_tag, has_s3_resource_tag] = rgw_check_policy_condition(this, s, false);
    if (has_s3_resource_tag)
      rgw_iam_add_buckettags(this, s);

    return verify_bucket_owner_or_policy(s, rgw::IAM::s3PutBucketLogging);
  }

  const char* name() const override { return "put_bucket_logging"; }
  RGWOpType get_type() override { return RGW_OP_PUT_BUCKET_LOGGING; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

  void execute(optional_yield y) override { 
    op_ret = verify_bucket_logging_params(this, s);
    if (op_ret < 0) {
      return;
    }

    const auto max_size = s->cct->_conf->rgw_max_put_param_size;
    bufferlist data;
    std::tie(op_ret, data) = read_all_input(s, max_size, false);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "failed to read XML payload, ret = " << op_ret << dendl;
      return;
    }
    if (data.length() == 0) {
      ldpp_dout(this, 1) << "XML payload missing" << dendl;
      op_ret = -EINVAL;
      return;
    }

    RGWXMLDecoder::XMLParser parser;
    if (!parser.init()){
      ldpp_dout(this, 1) << "failed to initialize XML parser" << dendl;
      op_ret = -EINVAL;
      return;
    }
    if (!parser.parse(data.c_str(), data.length(), 1)) {
      ldpp_dout(this, 1) << "failed to parse XML payload" << dendl;
      op_ret = -ERR_MALFORMED_XML;
      return;
    }
    rgw_bucket_logging configurations;
    try {
      RGWXMLDecoder::decode_xml("BucketLoggingStatus", configurations, &parser, true);
    } catch (RGWXMLDecoder::err& err) {
      ldpp_dout(this, 1) << "failed to parse XML payload. error: " << err << dendl;
      op_ret = -ERR_MALFORMED_XML;
      return;
    }

    std::unique_ptr<rgw::sal::Bucket> bucket;
    op_ret = driver->load_bucket(this, rgw_bucket(s->bucket_tenant, s->bucket_name),
                                 &bucket, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "failed to get bucket '" << s->bucket_name << "', ret = " << op_ret << dendl;
      return;
    }

    // TODO: should we delay this check to the actual writing of the logs?
    std::unique_ptr<rgw::sal::Bucket> target_bucket;
    op_ret = driver->load_bucket(this, rgw_bucket(s->bucket_tenant, configurations.target_bucket),
                                 &target_bucket, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "failed to get target bucket '" << configurations.target_bucket << "', ret = " << op_ret << dendl;
      return;
    }

    auto& attrs = bucket->get_attrs();
    if (!configurations.enabled) {
      if (auto iter = attrs.find(RGW_ATTR_BUCKET_LOGGING); iter != attrs.end()) {
        attrs.erase(iter);
      }
    } else {
      bufferlist conf_bl;
      encode(configurations, conf_bl);
      attrs[RGW_ATTR_BUCKET_LOGGING] = conf_bl;
    }
    // TODO: use retry_raced_bucket_write from rgw_op.cc
    op_ret = bucket->merge_and_store_attrs(this, attrs, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "failed to set attribute '" << RGW_ATTR_BUCKET_LOGGING << "' to bucket '" << 
        bucket->get_name() << "', ret = " << op_ret << dendl;
      return;
    }

    if (configurations.enabled) {
      ldpp_dout(this, 20) << "wrote logging configuration to bucket '" << bucket->get_name() << "' configuration: " <<
        configurations.to_json_str() << dendl;
    } else {
      ldpp_dout(this, 20) << "removed logging configuration from bucket '" << bucket->get_name() << "'" << dendl;
    }
  }
};

RGWOp* RGWHandler_REST_BucketLogging_S3::create_put_op() {
  return new RGWPutBucketLoggingOp();
}

RGWOp* RGWHandler_REST_BucketLogging_S3::create_get_op() {
  return new RGWGetBucketLoggingOp();
}

