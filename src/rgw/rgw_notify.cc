// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "rgw_notify.h"
#include "cls/2pc_queue/cls_2pc_queue_client.h"
#include "cls/lock/cls_lock_client.h"
#include <memory>
#include <boost/algorithm/hex.hpp>
#include "rgw_pubsub.h"
#include "rgw_pubsub_push.h"
#include "rgw_perf_counters.h"
#include "common/dout.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::notify {

struct record_with_endpoint_t {
  rgw_pubsub_s3_record record;
  std::string push_endpoint;
  std::string push_endpoint_args;
  std::string arn_topic;
  
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(record, bl);
    encode(push_endpoint, bl);
    encode(push_endpoint_args, bl);
    encode(arn_topic, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(record, bl);
    decode(push_endpoint, bl);
    decode(push_endpoint_args, bl);
    decode(arn_topic, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(record_with_endpoint_t)

// TODO: why decode/encode can't resolve that?
//using queues_t = std::unordered_set<std::string>;

struct queues_t {
  std::unordered_set<std::string> list;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(list, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(list, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(queues_t)

class Manager {
  bool stopped;
  const size_t max_queue_size;
  const std::chrono::milliseconds queues_update_period;
  const std::chrono::milliseconds queues_update_retry;
  const std::chrono::microseconds idle_sleep;
  const utime_t failover_time;
  CephContext* const cct;
  librados::IoCtx& ioctx;
  RGWUserPubSub ps_user;
  std::thread runner;
  std::unordered_set<std::string> owned_queues;
  std::atomic_bool list_of_queues_object_created;
  std::string lock_cookie;
 
  const std::string Q_LIST_OBJECT_NAME = "queues_list_object";

  int populate_queues() {
    bufferlist bl;
    constexpr auto chunk_size = 1024U;
    auto start_offset = 0U;
    int ret;
    do {
      bufferlist chunk_bl;
      // TODO: add yield ?
      ret = ioctx.read(Q_LIST_OBJECT_NAME, chunk_bl, chunk_size, start_offset);
      if (ret < 0) {
        return ret;
      }
      start_offset += chunk_size;
      bl.claim_append(chunk_bl);
    } while (ret > 0);
    auto iter = bl.cbegin();
    queues_t queues;
    try {
      decode(queues, iter);
    } catch (buffer::error& err) {
      ldout(cct, 1) << "ERROR: failed to decode queue list. error: " << err.what() << dendl;
      return -EINVAL;
    }
    for (const auto& queue_name : queues.list) {
      // try to lock the queue (lock must be created beforehand)
      // to check if it is owned by this rgw
      ret = rados::cls::lock::lock(&ioctx, queue_name, queue_name+"_lock", 
            LOCK_EXCLUSIVE,
            lock_cookie, 
            "" /*no tag*/,
            "" /*no description*/,
            failover_time,
            LOCK_FLAG_MUST_RENEW);

      if (ret == -EBUSY) {
        // lock is already taken by another RGW
       continue;
      }
      if (ret < 0) {
        // failed to lock for another reason
        return ret;
      }
      // add queue to list of owned queues
      owned_queues.insert(queue_name);
      // queue may already be in the list, no need to check return value
    }
    return 0;
  }

  void run() {
    // populate queue list
    auto next_queues_update = ceph::real_clock::now();
    while (!stopped) {
      auto idle = true;
      // periodically update the list of queues
      // this also renew the locks of the owned queues
	  if (next_queues_update <= ceph::real_clock::now()) {
        idle = false;
        const auto ret = populate_queues();
        if (ret < 0) {
          ldout(cct, 1) << "ERROR: failed to populate queue list. error: " << ret << dendl;
          next_queues_update += queues_update_retry;
        } else {
          next_queues_update += queues_update_period;
        }
      }
      // go through all owned queues and try to empty them
      for (const auto& queue_name : owned_queues) {
        // TODO: make this multithreaded loop
        const auto max_elements = 1024;
        // TODO limit the max number of messages read from the queue
        const std::string marker;
        bool truncated = false;
        std::string end_marker;
        std::vector<cls_queue_entry> entries;
        const auto ret = cls_2pc_queue_list_entries(ioctx, queue_name, marker, max_elements, entries, &truncated, end_marker);
        if (ret < 0) {
          ldout(cct, 5) << "WARNING: failed to get list of entries in queue: " 
            << queue_name << ". error: " << ret << " (will retry)" << dendl;
          continue;
        }
        idle = entries.empty();
        ldout(cct, 20) << "INFO: publishing: " << entries.size() << " entries from: " << queue_name << dendl;
        for (auto& entry : entries) {
          record_with_endpoint_t record_with_endpoint;
          auto iter = entry.data.cbegin();
          try {
            decode(record_with_endpoint, iter);
          } catch (buffer::error& err) {
            ldout(cct, 5) << "WARNING: failed to decode entry. error: " << err.what() << dendl;
            continue;
          }
          try {
            // TODO add endpoint LRU cache
            const auto push_endpoint = RGWPubSubEndpoint::create(record_with_endpoint.push_endpoint, record_with_endpoint.arn_topic,
                RGWHTTPArgs(record_with_endpoint.push_endpoint_args), 
                cct);
            ldout(cct, 20) << "INFO: push endpoint created: " << record_with_endpoint.push_endpoint << dendl;
            // TODO: create optional yield
            const auto ret = push_endpoint->send_to_completion_async(cct, record_with_endpoint.record, null_yield);
            if (ret < 0) {
              ldout(cct, 5) << "WARNING: push entry: " << entry.marker << " to endpoint: " << record_with_endpoint.push_endpoint 
                << " failed. error: " << ret << " (will retry)" << dendl;
              end_marker = entry.marker;
              break;
            } else {
              ldout(cct, 20) << "INFO: push entry: " << entry.marker << " to endpoint: " << record_with_endpoint.push_endpoint 
                << " OK" <<  dendl;
              if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_ok);
            }
          } catch (const RGWPubSubEndpoint::configuration_error& e) {
            ldout(cct, 5) << "WARNING: failed to create push endpoint: " 
                << record_with_endpoint.push_endpoint << ". error: " << e.what() << " (will retry) " << dendl;
            end_marker = entry.marker;
            break;
          }
        }

        // delete all published entries from queue
        if (!end_marker.empty()) {
          librados::ObjectWriteOperation op;
          cls_2pc_queue_remove_entries(op, end_marker); 
          // TODO call operate async with yield
          const auto ret = ioctx.operate(queue_name, &op);
          if (ret < 0) {
            ldout(cct, 1) << "ERROR: failed to remove entries from queue: " 
              << queue_name << ". error: " << ret << dendl;
              // TODO: error handling
          } else {
            ldout(cct, 20) << "INFO: remove entries from queue: " << queue_name << dendl;
          }
        }
      	// TODO: cleanup expired reservations
      }
      if (idle) {
        // sleep if idle
        std::this_thread::sleep_for(idle_sleep);
      }
    }
  }

public:
  Manager(CephContext* _cct, long _max_queue_size, long queues_update_period_ms, 
          long queues_update_retry_ms, long idle_sleep_usec, long failover_time_sec, rgw::sal::RGWRadosStore* store) : 
    stopped(false),
    max_queue_size(_max_queue_size),
    queues_update_period(queues_update_period_ms),
    queues_update_retry(queues_update_retry_ms),
    idle_sleep(idle_sleep_usec),
    failover_time(std::chrono::seconds(failover_time_sec)),
    cct(_cct),
    ioctx(store->getRados()->get_notif_pool_ctx()),
    ps_user(store, rgw_user("user")),  
    runner(&Manager::run, this),
    list_of_queues_object_created(false) {
      constexpr auto COOKIE_LEN = 16;
      char buf[COOKIE_LEN + 1];
      gen_rand_alphanumeric(cct, buf, sizeof(buf) - 1);
      lock_cookie = buf;
    }

  int add_persistent_topic(const std::string& topic_name, optional_yield y) {
    if (topic_name == Q_LIST_OBJECT_NAME) {
      ldout(cct, 1) << "ERROR: topic name cannot be '" << Q_LIST_OBJECT_NAME << "'" << dendl;
      return -EINVAL;
    }
    librados::ObjectWriteOperation op;
    op.create(true);
    cls_2pc_queue_init(op, topic_name, max_queue_size);
    auto ret = rgw_rados_operate(ioctx, topic_name, &op, y);
    if (ret == -EEXIST) {
      // queue already exists - nothing to do
      return 0;
    }
    if (ret < 0) {
      // failed to create queue
      return ret;
    }
    // lock the queue to be owned by this rgw
    ret = rados::cls::lock::lock(&ioctx, topic_name, topic_name+"_lock", 
            LOCK_EXCLUSIVE,
            lock_cookie, 
            "" /*no tag*/,
            "" /*no description*/,
            failover_time,
            LOCK_FLAG_MAY_RENEW);

    if (ret == -EBUSY) {
      // lock is already taken by another RGW
      return 0;
    }
    if (ret < 0) {
      // failed to lock for another reason
      return ret;
    }
    
    // create the object holding the list of queues if not created so far
    // note that in case of race from two threads, one would get -EEXIST
    if (!list_of_queues_object_created) {
      // create the object holding the list of queues
      ret = ioctx.create(Q_LIST_OBJECT_NAME, false);
      if (ret < 0 && ret != -EEXIST) {
        return ret;
      }
      list_of_queues_object_created = true;
    }
    // update the new queue in the list of queues
    // TODO: make read-modify-write atomic
    bufferlist bl;
    constexpr auto chunk_size = 1024U;
    auto start_offset = 0U;
    do {
      // TODO: add yield ?
      ret = ioctx.read(Q_LIST_OBJECT_NAME, bl, chunk_size, start_offset);
      if (ret < 0) {
        return ret;
      }
      start_offset += chunk_size;
    } while (ret > 0);

    queues_t queues;
    if (bl.length() > 0) {
      auto iter = bl.cbegin();
      try {
        decode(queues, iter);
      } catch (buffer::error& err) {
        ldout(cct, 1) << "ERROR: failed to decode queue list. error: " << err.what() << dendl;
        return -EINVAL;
      }
      bl.clear();
    }
    // no need to check for duplicate names
    queues.list.insert(topic_name);
    encode(queues, bl);
    return ioctx.write_full(Q_LIST_OBJECT_NAME, bl);
  }
};

// singleton manager
// note that the manager itself is not a singleton, and multiple instances may co-exist
// TODO make the pointer atomic in allocation and deallocation to avoid race conditions
static Manager* s_manager = nullptr;

constexpr size_t MAX_QUEUE_SIZE = 128*1024*1024; // 128MB
constexpr long Q_LIST_UPDATE_MSEC = 1000*30;     // check queue list every 30seconds
constexpr long Q_LIST_RETRY_MSEC = 1000;         // retry every second if queue list update failed
constexpr long IDLE_TIMEOUT_USEC = 100*1000;     // idle sleep 100ms
constexpr long FAILOVER_TIME_SEC = 30;           // FAILOVER TIME 30 SEC

bool init(CephContext* cct, rgw::sal::RGWRadosStore* store) {
  if (s_manager) {
    return false;
  }
  // TODO: take conf from CephContext
  s_manager = new Manager(cct, MAX_QUEUE_SIZE, Q_LIST_UPDATE_MSEC, Q_LIST_RETRY_MSEC,
          IDLE_TIMEOUT_USEC, FAILOVER_TIME_SEC, store);
  return true;
}

void shutdown() {
  delete s_manager;
  s_manager = nullptr;
}

int add_persistent_topic(const std::string& topic_name, optional_yield y) {
  if (!s_manager) {
    return -EAGAIN;
  }
  return s_manager->add_persistent_topic(topic_name, y);
}

// populate record from request
void populate_record_from_request(const req_state *s, 
        const rgw_obj_key& key,
        uint64_t size,
        const ceph::real_time& mtime, 
        const std::string& etag, 
        EventType event_type,
        rgw_pubsub_s3_record& record) { 
  record.eventTime = mtime;
  record.eventName = to_string(event_type);
  record.userIdentity = s->user->get_id().id;    // user that triggered the change
  record.x_amz_request_id = s->req_id;          // request ID of the original change
  record.x_amz_id_2 = s->host_id;               // RGW on which the change was made
  // configurationId is filled from notification configuration
  record.bucket_name = s->bucket_name;
  record.bucket_ownerIdentity = s->bucket_owner.get_id().id;
  record.bucket_arn = to_string(rgw::ARN(s->bucket));
  record.object_key = key.name;
  record.object_size = size;
  record.object_etag = etag;
  record.object_versionId = key.instance;
  // use timestamp as per key sequence id (hex encoded)
  const utime_t ts(real_clock::now());
  boost::algorithm::hex((const char*)&ts, (const char*)&ts + sizeof(utime_t), 
          std::back_inserter(record.object_sequencer));
  set_event_id(record.id, etag, ts);
  record.bucket_id = s->bucket.bucket_id;
  // pass meta data
  record.x_meta_map = s->info.x_meta_map;
  // pass tags
  record.tags = s->tagset.get_tags();
  // opaque data will be filled from topic configuration
}

bool match(const rgw_pubsub_topic_filter& filter, const req_state* s, EventType event) {
  if (!::match(filter.events, event)) { 
    return false;
  }
  if (!::match(filter.s3_filter.key_filter, s->object.name)) {
    return false;
  }
  if (!::match(filter.s3_filter.metadata_filter, s->info.x_meta_map)) {
    return false;
  }
  if (!::match(filter.s3_filter.tag_filter, s->tagset.get_tags())) {
    return false;
  }
  return true;
}

int publish_reserve(EventType event_type,
      reservation_t& res) {
  RGWUserPubSub ps_user(res.store, res.s->user->get_id());
  RGWUserPubSub::Bucket ps_bucket(&ps_user, res.s->bucket);
  rgw_pubsub_bucket_topics bucket_topics;
  auto rc = ps_bucket.get_topics(&bucket_topics);
  if (rc < 0) {
    // failed to fetch bucket topics
    return rc;
  }
  for (const auto& bucket_topic : bucket_topics.topics) {
    const rgw_pubsub_topic_filter& topic_filter = bucket_topic.second;
    const rgw_pubsub_topic& topic_cfg = topic_filter.topic;
    if (!match(topic_filter, res.s, event_type)) {
      // topic does not apply to req_state
      continue;
    }
    ldout(res.s->cct, 20) << "INFO: notification: '" << topic_filter.s3_id << 
        "' on topic: '" << topic_cfg.dest.arn_topic << 
        "' and bucket: '" << res.s->bucket.name << 
        "' (unique topic: '" << topic_cfg.name <<
        "') apply to event of type: '" << to_string(event_type) << "'" << dendl;

    cls_2pc_reservation::id_t res_id;
    if (topic_cfg.dest.persistent) {
      librados::ObjectWriteOperation op;
      // TODO: calculate based on max strings sizes?
      const auto size_to_reserve = 1024;
      const auto ret = cls_2pc_queue_reserve(res.store->getRados()->get_notif_pool_ctx(),
            topic_cfg.dest.arn_topic, op, size_to_reserve, 1, res_id);
      if (ret < 0) {
        return ret;
      }
    }
    res.topics.emplace_back(topic_filter.s3_id, topic_cfg, res_id);
  }
  return 0;
}


int publish_commit(const rgw_obj_key& key,
        uint64_t size,
        const ceph::real_time& mtime, 
        const std::string& etag, 
        EventType event_type,
        reservation_t& res) {
  for (auto& topic : res.topics) {
    if (topic.cfg.dest.persistent && topic.res_id == cls_2pc_reservation::NO_ID) {
      // nothing to commit or already committed/aborted
      continue;
    }
    record_with_endpoint_t record_with_endpoint;
    populate_record_from_request(res.s, key, size, mtime, etag, event_type, record_with_endpoint.record);
    record_with_endpoint.record.configurationId = topic.configurationId;
    record_with_endpoint.record.opaque_data = topic.cfg.opaque_data;
    if (topic.cfg.dest.persistent) { 
      record_with_endpoint.push_endpoint = std::move(topic.cfg.dest.push_endpoint);
      record_with_endpoint.push_endpoint_args = std::move(topic.cfg.dest.push_endpoint_args);
      record_with_endpoint.arn_topic = std::move(topic.cfg.dest.arn_topic);
      bufferlist bl;
      encode(record_with_endpoint, bl);
      std::vector<bufferlist> bl_data_vec{std::move(bl)};
      // TODO: check bl size
      librados::ObjectWriteOperation op;
      cls_2pc_queue_commit(op, bl_data_vec, topic.res_id);
      const auto ret = rgw_rados_operate(res.store->getRados()->get_notif_pool_ctx(),
            topic.cfg.dest.arn_topic, &op,
            res.s->yield);
      topic.res_id = cls_2pc_reservation::NO_ID;
      if (ret < 0) {
          return ret;
      }
    } else {
      try {
        // TODO add endpoint LRU cache
        const auto push_endpoint = RGWPubSubEndpoint::create(topic.cfg.dest.push_endpoint, 
                topic.cfg.dest.arn_topic,
                RGWHTTPArgs(topic.cfg.dest.push_endpoint_args), 
                res.s->cct);
        ldout(res.s->cct, 20) << "INFO: push endpoint created: " << topic.cfg.dest.push_endpoint << dendl;
        const auto ret = push_endpoint->send_to_completion_async(res.s->cct, record_with_endpoint.record, res.s->yield);
        if (ret < 0) {
          ldout(res.s->cct, 1) << "ERROR: push to endpoint " << topic.cfg.dest.push_endpoint << " failed. error: " << ret << dendl;
          if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_failed);
          return ret;
        }
        if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_ok);
      } catch (const RGWPubSubEndpoint::configuration_error& e) {
        ldout(res.s->cct, 1) << "ERROR: failed to create push endpoint: " 
            << topic.cfg.dest.push_endpoint << ". error: " << e.what() << dendl;
        if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_failed);
        return -EINVAL;
      }
    }
  }
  return 0;
}

int publish_abort(reservation_t& res) {
  for (auto& topic : res.topics) {
    if (!topic.cfg.dest.persistent || topic.res_id == cls_2pc_reservation::NO_ID) {
      // nothing to abort or already committed/aborted
      continue;
    }
    librados::ObjectWriteOperation op;
    cls_2pc_queue_abort(op,  topic.res_id);
    const auto ret = rgw_rados_operate(res.store->getRados()->get_notif_pool_ctx(),
      topic.cfg.dest.arn_topic, &op,
      res.s->yield);
    if (ret < 0) {
      ldout(res.s->cct, 1) << "ERROR: failed to abort reservation: " << topic.res_id <<
          ". error: " << ret << dendl;
      return ret;
    }
    topic.res_id = cls_2pc_reservation::NO_ID;
  }
  return 0;
}

reservation_t::~reservation_t() {
  publish_abort(*this);
}

}

