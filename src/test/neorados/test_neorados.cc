// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/rados/librados.hpp"
#include "include/neorados/RADOS.hpp"
#include "include/scope_guard.h"
#include "common/async/blocked_completion.h"
#include "common/async/context_pool.h"
#include "test/librados/test_cxx.h"
#include "gtest/gtest.h"
#include <iostream>
#include <thread>
#include <ranges>
#include "global/global_init.h"

boost::intrusive_ptr<CephContext> cct;

namespace neorados {

using namespace std::literals;

class TestNeoRADOS : public ::testing::Test {
public:
  TestNeoRADOS() {
  }
};

class TestNeoRADOSAsync : public ::testing::Test {
public:
  TestNeoRADOSAsync() {
  }
};

TEST_F(TestNeoRADOS, MakeWithLibRADOS) {
  librados::Rados paleo_rados;
  auto result = connect_cluster_pp(paleo_rados);
  ASSERT_EQ("", result);

  auto rados = RADOS::make_with_librados(paleo_rados);

  ReadOp op;
  bufferlist bl;
  op.read(0, 0, &bl);

  // provide pool that doesn't exists -- just testing round-trip
  ASSERT_THROW(
    rados.execute({"dummy-obj"}, IOContext(std::numeric_limits<int64_t>::max()),
                  std::move(op), &bl, ceph::async::use_blocked),
    boost::system::system_error);
}

TEST_F(TestNeoRADOS, MakeWithCCT) {

  ceph::async::io_context_pool p(1);
  auto rados = RADOS::make_with_cct(cct.get(), p,
      ceph::async::use_blocked);

  ReadOp op;
  bufferlist bl;
  op.read(0, 0, &bl);

  // provide pool that doesn't exists -- just testing round-trip
  ASSERT_THROW(
      rados.execute({"dummy-obj"}, IOContext(std::numeric_limits<int64_t>::max()),
                  std::move(op), &bl, ceph::async::use_blocked),
      boost::system::system_error);
}

TEST_F(TestNeoRADOS, CreatePool) {
  librados::Rados paleo_rados;
  auto result = connect_cluster_pp(paleo_rados);
  ASSERT_EQ("", result);

  auto rados = RADOS::make_with_librados(paleo_rados);

  const std::string pool_id{"piscine"};
  rados.create_pool(pool_id, std::nullopt, ceph::async::use_blocked);
  auto pool_deleter = make_scope_guard(
    [&pool_id, &rados]() {
      rados.delete_pool(pool_id, ceph::async::use_blocked);
    });
  auto pool = rados.lookup_pool(pool_id, ceph::async::use_blocked);

  ASSERT_GT(pool, 0);
}

TEST_F(TestNeoRADOS, CreateObjects) {
  ceph::async::io_context_pool p(1);
  auto rados = RADOS::make_with_cct(cct.get(), p,
      ceph::async::use_blocked);

  const std::string pool_id{"piscine"};
  rados.create_pool(pool_id, std::nullopt, ceph::async::use_blocked);
  auto pool_deleter = make_scope_guard(
    [&pool_id, &rados]() {
      rados.delete_pool(pool_id, ceph::async::use_blocked);
    });
  const auto pool = rados.lookup_pool(pool_id, ceph::async::use_blocked);
  IOContext io_ctx(pool);
  std::ranges::iota_view numbers{1, 100};
  std::unordered_set<std::string> objects;
  std::transform(numbers.begin(), numbers.end(), 
      std::inserter(objects, objects.begin()),
      [](auto i){return std::to_string(i);});
  for (const auto& o : objects) {
    WriteOp op;
    ceph::bufferlist bl;
    bl.append("nothing to see here");
    op.write_full(std::move(bl));
    rados.execute(o, io_ctx, std::move(op), ceph::async::use_blocked);
  }

  std::unordered_set<std::string> fetched_objects;
  auto b = Cursor::begin();
  const auto e = Cursor::end();
  auto [v, next] = rados.enumerate_objects(io_ctx, b, e, 1000, {}, ceph::async::use_blocked);
  std::transform(v.cbegin(), v.cend(), 
      std::inserter(fetched_objects, fetched_objects.begin()), 
      [](const Entry& e){return e.oid;});
  ASSERT_EQ(fetched_objects, objects);
}

TEST_F(TestNeoRADOSAsync, MakeWithLibRADOS) {
  librados::Rados paleo_rados;
  auto result = connect_cluster_pp(paleo_rados);
  ASSERT_EQ("", result);

  auto rados = RADOS::make_with_librados(paleo_rados);

  ReadOp op;
  bufferlist bl;
  op.read(0, 0, &bl);

  // provide pool that doesn't exists -- just testing round-trip
  ASSERT_THROW(
    rados.execute({"dummy-obj"}, IOContext(std::numeric_limits<int64_t>::max()),
                  std::move(op), &bl, boost::asio::use_future).get(),
    boost::system::system_error);
}

TEST_F(TestNeoRADOSAsync, MakeWithCCT) {
  ceph::async::io_context_pool p(1);
  auto rados = RADOS::make_with_cct(cct.get(), p,
      boost::asio::use_future).get();

  ReadOp op;
  bufferlist bl;
  op.read(0, 0, &bl);

  // provide pool that doesn't exists -- just testing round-trip
  ASSERT_THROW(
      rados.execute({"dummy-obj"}, IOContext(std::numeric_limits<int64_t>::max()),
                  std::move(op), &bl, boost::asio::use_future).get(),
      boost::system::system_error);
}

TEST_F(TestNeoRADOSAsync, CreatePool) {
  librados::Rados paleo_rados;
  auto result = connect_cluster_pp(paleo_rados);
  ASSERT_EQ("", result);

  auto rados = RADOS::make_with_librados(paleo_rados);

  const std::string pool_id{"piscine"};
  rados.create_pool(pool_id, std::nullopt, boost::asio::use_future).get();
  auto pool_deleter = make_scope_guard(
    [&pool_id, &rados]() {
      rados.delete_pool(pool_id, boost::asio::use_future).get();
    });
  auto pool = rados.lookup_pool(pool_id, boost::asio::use_future).get();

  ASSERT_GT(pool, 0);
}

TEST_F(TestNeoRADOSAsync, CreateObjects) {
  ceph::async::io_context_pool p(1);
  auto rados = RADOS::make_with_cct(cct.get(), p,
      boost::asio::use_future).get();

  const std::string pool_id{"piscine"};
  rados.create_pool(pool_id, std::nullopt, boost::asio::use_future).get();
  auto pool_deleter = make_scope_guard(
    [&pool_id, &rados]() {
      rados.delete_pool(pool_id, boost::asio::use_future).get();
    });
  const auto pool = rados.lookup_pool(pool_id, boost::asio::use_future).get();
  IOContext io_ctx(pool);
  std::ranges::iota_view numbers{1, 100};
  std::unordered_set<std::string> objects;
  std::transform(numbers.begin(), numbers.end(), 
      std::inserter(objects, objects.begin()),
      [](auto i){return std::to_string(i);});
  std::vector<std::future<void>> waiters; 
  for (const auto& o : objects) {
    WriteOp op;
    ceph::bufferlist bl;
    bl.append("there is nothing to see here");
    op.write_full(std::move(bl));
    waiters.emplace_back(rados.execute(o, io_ctx, std::move(op), boost::asio::use_future));
  }

  for (auto& w : waiters) {
    w.get();
  }

  std::unordered_set<std::string> fetched_objects;
  auto b = Cursor::begin();
  const auto e = Cursor::end();
  auto waiter = rados.enumerate_objects(io_ctx, b, e, 1000, {}, boost::asio::use_future);
  auto [v, next] = waiter.get();
  std::transform(v.cbegin(), v.cend(), 
      std::inserter(fetched_objects, fetched_objects.begin()), 
      [](const Entry& e){return e.oid;});
  ASSERT_EQ(fetched_objects, objects);
}

} // namespace neorados

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  int seed = getpid();
  std::cout << "seed " << seed << std::endl;
  srand(seed);

  std::vector<const char*> args;
  cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct.get());

  return RUN_ALL_TESTS();
}

