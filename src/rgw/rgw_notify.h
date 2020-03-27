// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include "common/ceph_time.h"
#include "include/common_fwd.h"
#include "rgw_notify_event_type.h"
#include "common/async/yield_context.h"
#include "cls/2pc_queue/cls_2pc_queue_types.h"
#include "rgw_pubsub.h"

// forward declarations
namespace rgw::sal {
    class RGWRadosStore;
}

class RGWRados;
struct rgw_obj_key;

namespace rgw::notify {

// initialize the notification manager
// notification manager is dequeing the 2-phase-commit queues
// and send the notifications to the endpoints
bool init(CephContext* cct, rgw::sal::RGWRadosStore* store);

// shutdown the notification manager
void shutdown();

// create persistent delivery queue for a topic (endpoint)
// this operation also:
// * create a (timed) lock to be owned by the RGW that created the topic
// * add a topic name to the common (to all RGWs) list of all topics
int add_persistent_topic(const std::string& topic_name, optional_yield y);

// struct holding reservation information
// populated in the publish_reserve call
// then used to commit or abort the reservation
struct reservation_t {
  struct topic_t {
    topic_t(const std::string& _configurationId, const rgw_pubsub_topic& _cfg, cls_2pc_reservation::id_t _res_id) :
        configurationId(_configurationId), cfg(_cfg), res_id(_res_id) {}

    const std::string configurationId;
    const rgw_pubsub_topic cfg;
    // res_id is reset after topic is committed/aborted
    cls_2pc_reservation::id_t res_id;
  };

  std::vector<topic_t> topics;
  rgw::sal::RGWRadosStore* const store;
  const req_state* const s;

  reservation_t(rgw::sal::RGWRadosStore* _store, const req_state* _s) : 
      store(_store), s(_s) {}

  // dtor doing resource leak guarding
  // aborting the reservation if not already committed or aborted
  ~reservation_t();
};

// create a reservation on the 2-phase-commit queue
int publish_reserve(EventType event_type,
        reservation_t& reservation);

// commit the reservation to the queue
int publish_commit(const rgw_obj_key& key,
        uint64_t size,
        const ceph::real_time& mtime, 
        const std::string& etag, 
        EventType event_type,
        reservation_t& reservation);

// cancel the reservation
int publish_abort(reservation_t& reservation);

}

