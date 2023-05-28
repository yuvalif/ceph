// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "acconfig.h"
#include "include/encoding.h"

#ifdef HAVE_JAEGER
#include "opentelemetry/trace/provider.h"

using jspan = opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>;
using jspan_context = opentelemetry::trace::SpanContext;
using jspan_attribute = opentelemetry::common::AttributeValue;

namespace tracing {


static constexpr int TraceIdkSize = 16;
static constexpr int SpanIdkSize = 8;
static_assert(TraceIdkSize == opentelemetry::trace::TraceId::kSize);
static_assert(SpanIdkSize == opentelemetry::trace::SpanId::kSize);

class Tracer {
 private:
  const static opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> noop_tracer;
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer;

 public:
  const static jspan noop_span;

  Tracer() = default;
  Tracer(opentelemetry::nostd::string_view service_name);

  void init(opentelemetry::nostd::string_view service_name);

  bool is_enabled() const;
  // creates and returns a new span with `trace_name`
  // this span represents a trace, since it has no parent.
  jspan start_trace(opentelemetry::nostd::string_view trace_name);

  // creates and returns a new span with `trace_name`
  // if false is given to `trace_is_enabled` param, noop span will be returned
  jspan start_trace(opentelemetry::nostd::string_view trace_name, bool trace_is_enabled);

  // creates and returns a new span with `span_name` which parent span is `parent_span'
  jspan add_span(opentelemetry::nostd::string_view span_name, const jspan& parent_span);
  // creates and return a new span with `span_name`
  // the span is added to the trace which it's context is `parent_ctx`.
  // parent_ctx contains the required information of the trace.
  jspan add_span(opentelemetry::nostd::string_view span_name, const jspan_context& parent_ctx);

};

// encoding the span
// shoudl be called only if valid, and between ENCODE_START, ENCODE_FINISH pair
inline void encode_nohead(const jspan_context& span_ctx, bufferlist& bl, uint64_t f = 0) {
  using namespace opentelemetry;
  using namespace trace;
  ceph::encode_nohead(std::string_view(reinterpret_cast<const char*>(span_ctx.trace_id().Id().data()), TraceIdkSize), bl);
  ceph::encode_nohead(std::string_view(reinterpret_cast<const char*>(span_ctx.span_id().Id().data()), SpanIdkSize), bl);
  encode(span_ctx.trace_flags().flags(), bl);
}

// decoding the span
// shoudl be called only if valid, and between DECODE_START, DECODE_FINISH pair
inline void decode_nohead(jspan_context& span_ctx, bufferlist::const_iterator& bl) {
  using namespace opentelemetry;
  using namespace trace;
  std::array<uint8_t, TraceIdkSize> trace_id;
  std::array<uint8_t, SpanIdkSize> span_id;
  uint8_t flags;
  decode(trace_id, bl);
  decode(span_id, bl);
  decode(flags, bl);
  span_ctx = SpanContext(
    TraceId(nostd::span<uint8_t, TraceIdkSize>(trace_id)),
    SpanId(nostd::span<uint8_t, SpanIdkSize>(span_id)),
    TraceFlags(flags),
    true);
}

// encoding the span
inline void encode(const jspan_context& span_ctx, bufferlist& bl, uint64_t f = 0) {
  ENCODE_START(1, 1, bl);
  auto is_valid = span_ctx.IsValid();
  encode(is_valid, bl);
  if (is_valid) {
    encode_nohead(span_ctx, bl, f);
  }
  ENCODE_FINISH(bl);
}

// decoding the span
inline void decode(jspan_context& span_ctx, bufferlist::const_iterator& bl) {
  DECODE_START(1, bl);
  bool is_valid;
  decode(is_valid, bl);
  if (is_valid) {
    decode_nohead(span_ctx, bl);
  }
  DECODE_FINISH(bl);
}

// encoding the span
// shoudl be called between ENCODE_START, ENCODE_FINISH pair
inline void nested_encode(const jspan_context& span_ctx, bufferlist& bl, uint64_t f = 0) {
  auto is_valid = span_ctx.IsValid();
  ceph::encode(is_valid, bl);
  if (is_valid) {
    ENCODE_START(1, 1, bl);
    encode_nohead(span_ctx, bl, f);
    ENCODE_FINISH(bl);
  }
}

// decoding the span
// shoudl be called between DECODE_START, DECODE_FINISH pair
inline void nested_decode(jspan_context& span_ctx, bufferlist::const_iterator& bl) {
  bool is_valid;
  ceph::decode(is_valid, bl);
  if (is_valid) {
    DECODE_START(1, bl);
    decode_nohead(span_ctx, bl);
    DECODE_FINISH(bl);
  }
}

} // namespace tracing


#else  // !HAVE_JAEGER

#include <string_view>

class Value {
 public:
  template <typename T> Value(T val) {}
};

using jspan_attribute = Value;

namespace opentelemetry {
inline namespace v1 {
namespace trace {
class SpanContext {
public:
  SpanContext() = default;
  SpanContext(bool sampled_flag, bool is_remote) {}
  bool IsValid() const { return false;}
};
} // namespace trace
} // namespace v1
} // namespace opentelemetry

using jspan_context = opentelemetry::v1::trace::SpanContext;

struct span_stub {
  jspan_context _ctx;
  template <typename T>
  void SetAttribute(std::string_view key, const T& value) const noexcept {}
  void AddEvent(std::string_view) {}
  void AddEvent(std::string_view, std::initializer_list<std::pair<std::string_view, jspan_attribute>> fields) {}
  template <typename T> void AddEvent(std::string_view name, const T& fields = {}) {}
  jspan_context GetContext() const { return _ctx; }
  void UpdateName(std::string_view) {}
  bool IsRecording() { return false; }
};

class jspan {
  span_stub span;
 public:
  span_stub& operator*() { return span; }
  const span_stub& operator*() const { return span; }

  span_stub* operator->() { return &span; }
  const span_stub* operator->() const { return &span; }

  operator bool() const { return false; }
};

namespace tracing {

struct Tracer {
  bool is_enabled() const { return false; }
  jspan start_trace(std::string_view, bool enabled = true) { return {}; }
  jspan add_span(std::string_view, const jspan&) { return {}; }
  jspan add_span(std::string_view span_name, const jspan_context& parent_ctx) { return {}; }
  void init(std::string_view service_name) {}
};
  inline void encode(const jspan_context& span, bufferlist& bl, uint64_t f=0) {}
  inline void decode(jspan_context& span_ctx, ceph::buffer::list::const_iterator& bl) {}
  inline void nested_encode(const jspan_context& span, bufferlist& bl, uint64_t f=0) {}
  inline void nested_decode(jspan_context& span_ctx, ceph::buffer::list::const_iterator& bl) {}
}

#endif // !HAVE_JAEGER
