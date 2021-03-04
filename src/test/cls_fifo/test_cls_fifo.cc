// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <cerrno>
#include <iostream>
#include <string_view>

#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>

#include <spawn/spawn.hpp>

#include "include/scope_guard.h"
#include "include/types.h"
#include "include/neorados/RADOS.hpp"

#include "cls/fifo/cls_fifo_ops.h"

#include "neorados/cls/fifo.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

namespace R = neorados;
namespace ba = boost::asio;
namespace bs = boost::system;
namespace cb = ceph::buffer;
namespace fifo = rados::cls::fifo;
namespace RCf = neorados::cls::fifo;
namespace s = spawn;

namespace {
void fifo_create(R::RADOS& r,
		 const R::IOContext& ioc,
		 const R::Object& oid,
		 std::string_view id,
		 s::yield_context y,
		 std::optional<fifo::objv> objv = std::nullopt,
		 std::optional<std::string_view> oid_prefix = std::nullopt,
		 bool exclusive = false,
		 std::uint64_t max_part_size = RCf::default_max_part_size,
		 std::uint64_t max_entry_size = RCf::default_max_entry_size,
     std::uint64_t visibility_timeout = 0,
     std::uint64_t retention_period = 0)
{
  R::WriteOp op;
  RCf::create_meta(op, id, objv, oid_prefix, exclusive, max_part_size,
		   max_entry_size, visibility_timeout, retention_period);
  r.execute(oid, ioc, std::move(op), y);
}
}

TEST(ClsFIFO, TestCreate) {
  ba::io_context c;
  auto fifo_id = "fifo"sv;
  R::Object oid(fifo_id);

  s::spawn(c, [&](s::yield_context y) {
		auto r = R::RADOS::Builder{}.build(c, y);
		auto pool = create_pool(r, get_temp_pool_name(), y);
		auto sg = make_scope_guard(
		  [&] {
		    r.delete_pool(pool, y);
		  });
		R::IOContext ioc(pool);
		bs::error_code ec;
		fifo_create(r, ioc, oid, ""s, y[ec]);
		EXPECT_EQ(bs::errc::invalid_argument, ec);
		fifo_create(r, ioc, oid, fifo_id, y[ec], std::nullopt,
			    std::nullopt, false, 0);
		EXPECT_EQ(bs::errc::invalid_argument, ec);
		fifo_create(r, ioc, oid, {}, y[ec],
			    std::nullopt, std::nullopt,
			    false, RCf::default_max_part_size, 0);
		EXPECT_EQ(bs::errc::invalid_argument, ec);
		fifo_create(r, ioc, oid, fifo_id, y);
		{
		  std::uint64_t size;
		  std::uint64_t size2;
		  {
		    R::ReadOp op;
		    op.stat(&size, nullptr);
		    r.execute(oid, ioc, std::move(op),
			      nullptr, y);
		    EXPECT_GT(size, 0);
		  }

		  {
		    R::ReadOp op;
		    op.stat(&size2, nullptr);
		    r.execute(oid, ioc, std::move(op), nullptr, y);
		  }
		  EXPECT_EQ(size2, size);
		}
		/* test idempotency */
		fifo_create(r, ioc, oid, fifo_id, y);
		fifo_create(r, ioc, oid, {}, y[ec], std::nullopt,
			    std::nullopt, false);
		EXPECT_EQ(bs::errc::invalid_argument, ec);
		fifo_create(r, ioc, oid, {}, y[ec], std::nullopt,
			    "myprefix"sv, false);
		EXPECT_EQ(bs::errc::invalid_argument, ec);
		fifo_create(r, ioc, oid, "foo"sv, y[ec],
			    std::nullopt, std::nullopt, false);
		EXPECT_EQ(bs::errc::file_exists, ec);
	      });
  c.run();
}

TEST(ClsFIFO, TestGetInfo) {
  ba::io_context c;
  auto fifo_id = "fifo"sv;
  R::Object oid(fifo_id);

  s::spawn(c, [&](s::yield_context y) {
		auto r = R::RADOS::Builder{}.build(c, y);
		auto pool = create_pool(r, get_temp_pool_name(), y);
		auto sg = make_scope_guard(
		  [&] {
		    r.delete_pool(pool, y);
		  });
		R::IOContext ioc(pool);
		/* first successful create */
		fifo_create(r, ioc, oid, fifo_id, y);

		fifo::info info;
		std::uint32_t part_header_size;
		std::uint32_t part_entry_overhead;
		{
		  R::ReadOp op;
		  RCf::get_meta(op, std::nullopt,
				nullptr, &info, &part_header_size,
				&part_entry_overhead);
		  r.execute(oid, ioc, std::move(op), nullptr, y);
		  EXPECT_GT(part_header_size, 0);
		  EXPECT_GT(part_entry_overhead, 0);
		  EXPECT_FALSE(info.version.instance.empty());
		}
		{
		  R::ReadOp op;
		  RCf::get_meta(op, info.version,
				nullptr, &info, &part_header_size,
				&part_entry_overhead);
		  r.execute(oid, ioc, std::move(op), nullptr, y);
		}
		{
		  R::ReadOp op;
		  fifo::objv objv;
		  objv.instance = "foo";
		  objv.ver = 12;
		  RCf::get_meta(op, objv,
				nullptr, &info, &part_header_size,
				&part_entry_overhead);
		  ASSERT_ANY_THROW(r.execute(oid, ioc, std::move(op),
					     nullptr, y));
		}
	      });
  c.run();
}

TEST(FIFO, TestOpenDefault) {
  ba::io_context c;
  auto fifo_id = "fifo"s;

  s::spawn(c, [&](s::yield_context y) {
		auto r = R::RADOS::Builder{}.build(c, y);
		auto pool = create_pool(r, get_temp_pool_name(), y);
		auto sg = make_scope_guard(
		  [&] {
		    r.delete_pool(pool, y);
		  });
		R::IOContext ioc(pool);
		auto fifo = RCf::FIFO::create(r, ioc, fifo_id, y);
		// force reading from backend
		fifo->read_meta(y);
		auto info = fifo->meta();
		EXPECT_EQ(info.id, fifo_id);
	      });
  c.run();
}

TEST(FIFO, TestOpenParams) {
  ba::io_context c;
  auto fifo_id = "fifo"sv;

  s::spawn(c, [&](s::yield_context y) {
		auto r = R::RADOS::Builder{}.build(c, y);
		auto pool = create_pool(r, get_temp_pool_name(), y);
		auto sg = make_scope_guard(
		  [&] {
		    r.delete_pool(pool, y);
		  });
		R::IOContext ioc(pool);

		const std::uint64_t max_part_size = 10 * 1024;
		const std::uint64_t max_entry_size = 128;
		auto oid_prefix = "foo.123."sv;
		fifo::objv objv;
		objv.instance = "fooz"s;
		objv.ver = 10;

		/* first successful create */
		auto f = RCf::FIFO::create(r, ioc, fifo_id, y, objv, oid_prefix,
					   false, max_part_size,
					   max_entry_size);


		/* force reading from backend */
		f->read_meta(y);
		auto info = f->meta();
		ASSERT_EQ(info.id, fifo_id);
		ASSERT_EQ(info.params.max_part_size, max_part_size);
		ASSERT_EQ(info.params.max_entry_size, max_entry_size);
		ASSERT_EQ(info.version, objv);
	      });
  c.run();
}

TEST(FIFO2, TestOpenParams) {
  ba::io_context c;
  auto fifo_id = "fifo"sv;

  s::spawn(c, [&](s::yield_context y) {
		auto r = R::RADOS::Builder{}.build(c, y);
		auto pool = create_pool(r, get_temp_pool_name(), y);
		auto sg = make_scope_guard(
		  [&] {
		    r.delete_pool(pool, y);
		  });
		R::IOContext ioc(pool);

		const std::uint64_t max_part_size = 10 * 1024;
		const std::uint64_t max_entry_size = 128;
		const std::uint64_t visibility_timeout = 600;
		const std::uint64_t retention_period = 3600;
		auto oid_prefix = "foo.123."sv;
		fifo::objv objv;
		objv.instance = "fooz"s;
		objv.ver = 10;

		/* first successful create */
		auto f = RCf::FIFO::create(r, ioc, fifo_id, y, objv, oid_prefix,
					   false, max_part_size,
					   max_entry_size, visibility_timeout, retention_period);


		/* force reading from backend */
		f->read_meta(y);
		auto info = f->meta();
		ASSERT_EQ(info.id, fifo_id);
		ASSERT_EQ(info.params.max_part_size, max_part_size);
		ASSERT_EQ(info.params.max_entry_size, max_entry_size);
		ASSERT_EQ(info.params.visibility_timeout, visibility_timeout);
		ASSERT_EQ(info.params.retention_period, retention_period);
		ASSERT_EQ(info.version, objv);
	      });
  c.run();
}

namespace {
template<class T>
std::pair<T, std::string> decode_entry(const RCf::list_entry& entry)
{
  T val;
  auto iter = entry.data.cbegin();
  decode(val, iter);
  return std::make_pair(std::move(val), entry.marker);
}
}



TEST(FIFO, TestPushListTrim) {
  ba::io_context c;
  auto fifo_id = "fifo"sv;

  s::spawn(c, [&](s::yield_context y) mutable {
		auto r = R::RADOS::Builder{}.build(c, y);
		auto pool = create_pool(r, get_temp_pool_name(), y);
		auto sg = make_scope_guard(
		  [&] {
		    r.delete_pool(pool, y);
		  });
		R::IOContext ioc(pool);
		auto f = RCf::FIFO::create(r, ioc, fifo_id, y);
		static constexpr auto max_entries = 10u;
		for (uint32_t i = 0; i < max_entries; ++i) {
		  cb::list bl;
		  encode(i, bl);
		  f->push(bl, y);
		}

		std::optional<std::string> marker;
		/* get entries one by one */

		for (auto i = 0u; i < max_entries; ++i) {
		  auto [result, more] = f->list(1, marker, y);

		  bool expected_more = (i != (max_entries - 1));
		  ASSERT_EQ(expected_more, more);
		  ASSERT_EQ(1, result.size());

		  std::uint32_t val;
		  std::tie(val, marker) =
		    decode_entry<std::uint32_t>(result.front());

		  ASSERT_EQ(i, val);
		}

		/* get all entries at once */
		std::string markers[max_entries];
		std::uint32_t min_entry = 0;
		{
		  auto [result, more] = f->list(max_entries * 10, std::nullopt,
						y);

		  ASSERT_FALSE(more);
		  ASSERT_EQ(max_entries, result.size());


		  for (auto i = 0u; i < max_entries; ++i) {
		    std::uint32_t val;

		    std::tie(val, markers[i]) =
		      decode_entry<std::uint32_t>(result[i]);
		    ASSERT_EQ(i, val);
		  }


		  /* trim one entry */
		  f->trim(markers[min_entry], false, y);
		  ++min_entry;
		}

		auto [result, more] = f->list(max_entries * 10,
					      std::nullopt, y);

		ASSERT_FALSE(more);
		ASSERT_EQ(max_entries - min_entry, result.size());

		for (auto i = min_entry; i < max_entries; ++i) {
		  std::uint32_t val;

		  std::tie(val, markers[i - min_entry]) =
		    decode_entry<std::uint32_t>(result[i - min_entry]);
		  ASSERT_EQ(i, val);
		}

	      });
  c.run();
}

std::random_device rd;
std::mt19937 gen(rd());

std::string random_string(std::size_t min, std::size_t max) {
  static constexpr auto chars =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";
 		
	std::uniform_int_distribution size_dist(min, max);
	const auto len = size_dist(gen);
  std::uniform_int_distribution<> char_dist(0, std::strlen(chars) - 1);
  auto result = std::string(len, '\0');
  std::generate_n(begin(result), len, [&]() { return chars[char_dist(gen)]; });
  return result;
}

TEST(FIFO2, TestPushList) {
  // this tests verifies that list2 can work in the same way as list
  // when provided with markers
  ba::io_context c;
  auto fifo_id = "fifo"sv;

  s::spawn(c, [&](s::yield_context y) mutable {
		auto r = R::RADOS::Builder{}.build(c, y);
		auto pool = create_pool(r, get_temp_pool_name(), y);
		auto sg = make_scope_guard(
		  [&] {
		    r.delete_pool(pool, y);
		  });
		R::IOContext ioc(pool);
		auto f = RCf::FIFO::create(r, ioc, fifo_id, y);
		static constexpr auto max_entries = 5000U;

		std::vector<std::size_t> hashed_inputs;
		bs::error_code ec;
		for (auto i = 0U; i < max_entries; ++i) {
		  cb::list bl;
			const auto val = random_string(1024*8, 1024*16);
			hashed_inputs.push_back(std::hash<std::string>{}(val));
		  encode(val, bl);
		  f->push(bl, y[ec]);
      ASSERT_FALSE(ec.failed());
		}

	  std::uniform_int_distribution num_of_entries(64, 512);
    const auto entries_to_list = num_of_entries(gen);

		bool more = true;
		std::optional<std::string> next_marker;
		std::uint64_t index = 0;
		while (more) {
			std::string marker;
			std::vector<neorados::cls::fifo::list_entry> result;
		  std::tie(result, more) = f->list2(entries_to_list, next_marker, y[ec]);
      EXPECT_FALSE(ec.failed());
			
			const auto size = result.size();
      EXPECT_GE(entries_to_list, size);

			std::string first_marker = decode_entry<std::string>(*result.begin()).second;
		  for (const auto& entry : result) {
		    std::string val;
		    std::tie(val, marker) = decode_entry<std::string>(entry);
				EXPECT_EQ(std::hash<std::string>{}(val), hashed_inputs[index++]); 
		  }
			next_marker = marker;
		}
	});
  c.run();
}

TEST(FIFO2, TestPushListNoMarker) {
  // this tests verifies that list2 can work in the same way as list
  // when not provided with markers
  ba::io_context c;
  auto fifo_id = "fifo"sv;

  s::spawn(c, [&](s::yield_context y) mutable {
		auto r = R::RADOS::Builder{}.build(c, y);
		auto pool = create_pool(r, get_temp_pool_name(), y);
		auto sg = make_scope_guard(
		  [&] {
		    r.delete_pool(pool, y);
		  });
		R::IOContext ioc(pool);
		auto f = RCf::FIFO::create(r, ioc, fifo_id, y);
		static constexpr auto max_entries = 5000U;

		std::vector<std::size_t> hashed_inputs;
		bs::error_code ec;
		for (auto i = 0U; i < max_entries; ++i) {
		  cb::list bl;
			const auto val = random_string(1024*8, 1024*16);
			hashed_inputs.push_back(std::hash<std::string>{}(val));
		  encode(val, bl);
		  f->push(bl, y[ec]);
      ASSERT_FALSE(ec.failed());
		}

	  std::uniform_int_distribution num_of_entries(64, 512);
    const auto entries_to_list = num_of_entries(gen);

		bool more = true;
		std::optional<std::string> next_marker;
		std::uint64_t index = 0;

		// list all entries using the new api without markers
		while (more) {
			std::string marker;
			std::vector<neorados::cls::fifo::list_entry> result;
      std::tie(result, more) = f->list2(entries_to_list, std::nullopt, y[ec]);
      EXPECT_FALSE(ec.failed());
			
			const auto size = result.size();
      EXPECT_GE(entries_to_list, size);

			std::string first_marker = decode_entry<std::string>(*result.begin()).second;
		  for (const auto& entry : result) {
		    std::string val;
		    std::tie(val, marker) = decode_entry<std::string>(entry);
				EXPECT_EQ(std::hash<std::string>{}(val), hashed_inputs[index++]); 
		  }
		}
	});
  c.run();
}


void print_fifo(RCf::FIFO* f, s::yield_context& y, bool list2) {
	constexpr auto entries_to_list = 512UL;

  bool more = true;
  std::optional<std::string> next_marker;
  while (more) {
  std::vector<neorados::cls::fifo::list_entry> result;
		bs::error_code ec;
    if (list2) {
      std::tie(result, more) = f->list2(entries_to_list, next_marker, y[ec]);
    } else {
      std::tie(result, more) = f->list(entries_to_list, next_marker, y[ec]);
    }
    if (ec.failed()) {
      std::cout << "listing entries failed: " << ec.message() << std::endl;
      return;
    }
    if (result.empty()) {
      std::cout << "no entries to list" << std::endl;
      return;
    }

    const auto first_marker = decode_entry<std::string>(*result.begin()).second;
    const auto last_marker = decode_entry<std::string>(result.back()).second;
    std::cout << "listed entries: " << first_marker << " - " << last_marker << std::endl;
    next_marker = last_marker;
  }
}

void push_list_trim_test(bool out_of_order) {
  ba::io_context c;
  auto fifo_id = "fifo_"+random_string(5,5);

  s::spawn(c, [&](s::yield_context y) mutable {
		auto r = R::RADOS::Builder{}.build(c, y);
		auto pool = create_pool(r, get_temp_pool_name(), y);
		auto sg = make_scope_guard(
		  [&] {
		    r.delete_pool(pool, y);
		  });
		R::IOContext ioc(pool);
		auto f = RCf::FIFO::create(r, ioc, fifo_id, y);
		static constexpr auto max_entries = 2000U;

		std::vector<std::size_t> hashed_inputs;
		bs::error_code ec;
		for (auto i = 0U; i < max_entries; ++i) {
		  cb::list bl;
			const auto val = random_string(1024*8, 1024*16);
			hashed_inputs.push_back(std::hash<std::string>{}(val));
		  encode(val, bl);
		  f->push(bl, y[ec]);
      ASSERT_FALSE(ec.failed());
		}

	  std::uniform_int_distribution num_of_entries(64, 256);
    const auto entries_to_list = num_of_entries(gen);

		bool more = true;
		std::optional<std::string> next_marker;
		std::uint64_t index = 0;
    std::vector<std::pair<std::string, std::string>> listed_segments;
		while (more) {
			std::vector<neorados::cls::fifo::list_entry> result;
		  std::tie(result, more) = f->list2(entries_to_list, next_marker, y[ec]);
      EXPECT_FALSE(ec.failed());

			const auto size = result.size();
      EXPECT_GE(entries_to_list, size);

			const auto first_marker = decode_entry<std::string>(*result.begin()).second;
			std::string marker;
		  for (const auto& entry : result) {
		    std::string val;
		    std::tie(val, marker) = decode_entry<std::string>(entry);
				EXPECT_EQ(std::hash<std::string>{}(val), hashed_inputs[index++]); 
		  }

      std::cout << "listed entries: " << first_marker << " - " << marker << std::endl;
      listed_segments.emplace_back(first_marker, marker);
			next_marker = marker;
		}

    if (out_of_order) {
      std::random_shuffle(listed_segments.begin(), listed_segments.end());
    }
    for (auto& segment : listed_segments) {
      std::cout << "entries to trim: " << segment.first << " - " << segment.second << std::endl;

      f->trim2(segment.first, segment.second, false, y[ec]);
      EXPECT_FALSE(ec.failed());
    }

    print_fifo(f.get(), y, false);
    //print_fifo(f.get(), y, true);
    // make sure that the queue is empty
    //std::vector<neorados::cls::fifo::list_entry> result;
    //std::tie(result, more) = f->list2(1024, std::nullopt, y[ec]);
    //std::cout << ec.message() << std::endl;
    //EXPECT_FALSE(ec.failed());
    //EXPECT_TRUE(result.empty());
	});
  c.run();
}

TEST(FIFO2, TestPushListTrim) {
  push_list_trim_test(false);
}

TEST(FIFO2, TestPushListTrimOutOfOrder) {
  push_list_trim_test(true);
}

TEST(FIFO, TestPushTooBig) {
  ba::io_context c;
  auto fifo_id = "fifo"sv;
  static constexpr auto max_part_size = 2048ull;
  static constexpr auto max_entry_size = 128ull;

  s::spawn(c, [&](s::yield_context y) {
		auto r = R::RADOS::Builder{}.build(c, y);
		auto pool = create_pool(r, get_temp_pool_name(), y);
		auto sg = make_scope_guard(
		  [&] {
		    r.delete_pool(pool, y);
		  });
		R::IOContext ioc(pool);

		auto f = RCf::FIFO::create(r, ioc, fifo_id, y, std::nullopt,
					   std::nullopt, false, max_part_size,
					   max_entry_size);

		char buf[max_entry_size + 1];
		memset(buf, 0, sizeof(buf));

		cb::list bl;
		bl.append(buf, sizeof(buf));

		bs::error_code ec;
		f->push(bl, y[ec]);
		EXPECT_EQ(RCf::errc::entry_too_large, ec);
	      });
  c.run();
}


TEST(FIFO, TestMultipleParts) {
  ba::io_context c;
  auto fifo_id = "fifo"sv;
  static constexpr auto max_part_size = 2048ull;
  static constexpr auto max_entry_size = 128ull;

  s::spawn(c, [&](s::yield_context y) mutable {
		auto r = R::RADOS::Builder{}.build(c, y);
		auto pool = create_pool(r, get_temp_pool_name(), y);
		auto sg = make_scope_guard(
		  [&] {
		    r.delete_pool(pool, y);
		  });
		R::IOContext ioc(pool);

		auto f = RCf::FIFO::create(r, ioc, fifo_id, y, std::nullopt,
					   std::nullopt, false, max_part_size,
					   max_entry_size);


		char buf[max_entry_size];
		memset(buf, 0, sizeof(buf));

		const auto [part_header_size, part_entry_overhead] =
		  f->get_part_layout_info();

		const auto entries_per_part =
		  (max_part_size - part_header_size) /
		  (max_entry_size + part_entry_overhead);

		const auto max_entries = entries_per_part * 4 + 1;

		/* push enough entries */
		for (auto i = 0u; i < max_entries; ++i) {
		  cb::list bl;

		  *(int *)buf = i;
		  bl.append(buf, sizeof(buf));

		  f->push(bl, y);
		}

		auto info = f->meta();

		ASSERT_EQ(info.id, fifo_id);
		/* head should have advanced */
		ASSERT_GT(info.head_part_num, 0);


		/* list all at once */
		auto [result, more] = f->list(max_entries, std::nullopt, y);
		EXPECT_EQ(false, more);

		ASSERT_EQ(max_entries, result.size());

		for (auto i = 0u; i < max_entries; ++i) {
		  auto& bl = result[i].data;
		  ASSERT_EQ(i, *(int *)bl.c_str());
		}

		std::optional<std::string> marker;
		/* get entries one by one */

		for (auto i = 0u; i < max_entries; ++i) {
		  auto [result, more] = f->list(1, marker, y);
		  ASSERT_EQ(result.size(), 1);
		  const bool expected_more = (i != (max_entries - 1));
		  ASSERT_EQ(expected_more, more);

		  std::uint32_t val;
		  std::tie(val, marker) =
		    decode_entry<std::uint32_t>(result.front());

		  auto& entry = result.front();
		  auto& bl = entry.data;
		  ASSERT_EQ(i, *(int *)bl.c_str());
		  marker = entry.marker;
		}

		/* trim one at a time */
		marker.reset();
		for (auto i = 0u; i < max_entries; ++i) {
		  /* read single entry */
		  {
		    auto [result, more] = f->list(1, marker, y);
		    ASSERT_EQ(result.size(), 1);
		    const bool expected_more = (i != (max_entries - 1));
		    ASSERT_EQ(expected_more, more);

		    marker = result.front().marker;

		    f->trim(*marker, false, y);
		  }

		  /* check tail */
		  info = f->meta();
		  ASSERT_EQ(info.tail_part_num, i / entries_per_part);

		  /* try to read all again, see how many entries left */
		  auto [result, more] = f->list(max_entries, marker, y);
		  ASSERT_EQ(max_entries - i - 1, result.size());
		  ASSERT_EQ(false, more);
		}

		/* tail now should point at head */
		info = f->meta();
		ASSERT_EQ(info.head_part_num, info.tail_part_num);

		/* check old tails are removed */
		for (auto i = 0; i < info.tail_part_num; ++i) {
		  bs::error_code ec;
		  f->get_part_info(i, y[ec]);
		  ASSERT_EQ(bs::errc::no_such_file_or_directory, ec);
		}
		/* check current tail exists */
		f->get_part_info(info.tail_part_num, y);
	      });
  c.run();
}


TEST(FIFO, TestTwoPushers) {
  ba::io_context c;
  auto fifo_id = "fifo"sv;
  static constexpr auto max_part_size = 2048ull;
  static constexpr auto max_entry_size = 128ull;

  s::spawn(c, [&](s::yield_context y) {
		auto r = R::RADOS::Builder{}.build(c, y);
		auto pool = create_pool(r, get_temp_pool_name(), y);
		auto sg = make_scope_guard(
		  [&] {
		    r.delete_pool(pool, y);
		  });
		R::IOContext ioc(pool);

		auto f = RCf::FIFO::create(r, ioc, fifo_id, y, std::nullopt,
					   std::nullopt, false, max_part_size,
					   max_entry_size);



		char buf[max_entry_size];
		memset(buf, 0, sizeof(buf));


		auto [part_header_size, part_entry_overhead] =
		  f->get_part_layout_info();

		const auto entries_per_part =
		  (max_part_size - part_header_size) /
		  (max_entry_size + part_entry_overhead);

		const auto max_entries = entries_per_part * 4 + 1;

		auto f2 = RCf::FIFO::open(r, ioc, fifo_id, y);

		std::vector fifos{&f, &f2};

		for (auto i = 0u; i < max_entries; ++i) {
		  cb::list bl;
		  *(int *)buf = i;
		  bl.append(buf, sizeof(buf));

		  auto& f = fifos[i % fifos.size()];

		  (*f)->push(bl, y);
		}

		/* list all by both */
		{
		  auto [result, more] = f2->list(max_entries, std::nullopt, y);

		  ASSERT_EQ(false, more);
		  ASSERT_EQ(max_entries, result.size());
		}
		auto [result, more] = f2->list(max_entries, std::nullopt, y);
		ASSERT_EQ(false, more);
		ASSERT_EQ(max_entries, result.size());

		for (auto i = 0u; i < max_entries; ++i) {
		  auto& bl = result[i].data;
		  ASSERT_EQ(i, *(int *)bl.c_str());
		}
	      });
  c.run();
}


TEST(FIFO, TestTwoPushersTrim) {
  ba::io_context c;
  auto fifo_id = "fifo"sv;
  static constexpr auto max_part_size = 2048ull;
  static constexpr auto max_entry_size = 128ull;

  s::spawn(c, [&](s::yield_context y) {
		auto r = R::RADOS::Builder{}.build(c, y);
		auto pool = create_pool(r, get_temp_pool_name(), y);
		auto sg = make_scope_guard(
		  [&] {
		    r.delete_pool(pool, y);
		  });
		R::IOContext ioc(pool);

		auto f1 = RCf::FIFO::create(r, ioc, fifo_id, y, std::nullopt,
					    std::nullopt, false, max_part_size,
					    max_entry_size);

		char buf[max_entry_size];
		memset(buf, 0, sizeof(buf));


		auto [part_header_size, part_entry_overhead] =
		  f1->get_part_layout_info();

		const auto entries_per_part =
		  (max_part_size - part_header_size) /
		  (max_entry_size + part_entry_overhead);

		const auto max_entries = entries_per_part * 4 + 1;

		auto f2 = RCf::FIFO::open(r, ioc, fifo_id, y);

		/* push one entry to f2 and the rest to f1 */

		for (auto i = 0u; i < max_entries; ++i) {
		  cb::list bl;

		  *(int *)buf = i;
		  bl.append(buf, sizeof(buf));

		  auto f = (i < 1 ? &f2 : &f1);
		  (*f)->push(bl, y);
		}

		/* trim half by fifo1 */
		auto num = max_entries / 2;

		std::string marker;
		{
		  auto [result, more] = f1->list(num, std::nullopt, y);

		  ASSERT_EQ(true, more);
		  ASSERT_EQ(num, result.size());

		  for (auto i = 0u; i < num; ++i) {
		    auto& bl = result[i].data;
		    ASSERT_EQ(i, *(int *)bl.c_str());
		  }

		  auto& entry = result[num - 1];
		  marker = entry.marker;

		  f1->trim(marker, false, y);

		  /* list what's left by fifo2 */

		}

		const auto left = max_entries - num;
		auto [result, more] = f2->list(left, marker, y);
		ASSERT_EQ(left, result.size());
		ASSERT_EQ(false, more);

		for (auto i = num; i < max_entries; ++i) {
		  auto& bl = result[i - num].data;
		  ASSERT_EQ(i, *(int *)bl.c_str());
		}
	      });
  c.run();
}

TEST(FIFO, TestPushBatch) {
  ba::io_context c;
  auto fifo_id = "fifo"sv;
  static constexpr auto max_part_size = 2048ull;
  static constexpr auto max_entry_size = 128ull;

  s::spawn(c, [&](s::yield_context y) {
		auto r = R::RADOS::Builder{}.build(c, y);
		auto pool = create_pool(r, get_temp_pool_name(), y);
		auto sg = make_scope_guard(
		  [&] {
		    r.delete_pool(pool, y);
		  });
		R::IOContext ioc(pool);

		auto f = RCf::FIFO::create(r, ioc, fifo_id, y, std::nullopt,
					   std::nullopt, false, max_part_size,
					   max_entry_size);


		char buf[max_entry_size];
		memset(buf, 0, sizeof(buf));

		auto [part_header_size, part_entry_overhead]
		  = f->get_part_layout_info();

		auto entries_per_part =
		  (max_part_size - part_header_size) /
		  (max_entry_size + part_entry_overhead);

		auto max_entries = entries_per_part * 4 + 1; /* enough entries to span multiple parts */

		std::vector<cb::list> bufs;

		for (auto i = 0u; i < max_entries; ++i) {
		  cb::list bl;

		  *(int *)buf = i;
		  bl.append(buf, sizeof(buf));

		  bufs.push_back(bl);
		}

		f->push(bufs, y);

		/* list all */

		auto [result, more] = f->list(max_entries, std::nullopt, y);
		ASSERT_EQ(false, more);
		ASSERT_EQ(max_entries, result.size());

		for (auto i = 0u; i < max_entries; ++i) {
		  auto& bl = result[i].data;
		  ASSERT_EQ(i, *(int *)bl.c_str());
		}

		auto& info = f->meta();
		ASSERT_EQ(info.head_part_num, 4);
	      });
  c.run();
}

TEST(FIFO, TestTrimExclusive) {
  ba::io_context c;
  auto fifo_id = "fifo"sv;

  s::spawn(c, [&](s::yield_context y) mutable {
		auto r = R::RADOS::Builder{}.build(c, y);
		auto pool = create_pool(r, get_temp_pool_name(), y);
		auto sg = make_scope_guard(
		  [&] {
		    r.delete_pool(pool, y);
		  });
		R::IOContext ioc(pool);
		auto f = RCf::FIFO::create(r, ioc, fifo_id, y);
		static constexpr auto max_entries = 10u;
		for (uint32_t i = 0; i < max_entries; ++i) {
		  cb::list bl;
		  encode(i, bl);
		  f->push(bl, y);
		}

		{
		  auto [result, more] = f->list(1, std::nullopt, y);
		  auto [val, marker] =
		    decode_entry<std::uint32_t>(result.front());
		  ASSERT_EQ(0, val);
		  f->trim(marker, true, y);
		}
		{
		  auto [result, more] = f->list(max_entries, std::nullopt, y);
		  auto [val, marker] = decode_entry<std::uint32_t>(result.front());
		  ASSERT_EQ(0, val);
		  f->trim(result[4].marker, true, y);
		}
		{
		  auto [result, more] = f->list(max_entries, std::nullopt, y);
		  auto [val, marker] =
		    decode_entry<std::uint32_t>(result.front());
		  ASSERT_EQ(4, val);
		  f->trim(result.back().marker, true, y);
		}
		{
		  auto [result, more] = f->list(max_entries, std::nullopt, y);
		  auto [val, marker] =
		    decode_entry<std::uint32_t>(result.front());
		  ASSERT_EQ(result.size(), 1);
		  ASSERT_EQ(max_entries - 1, val);
		}
	      });
  c.run();
}
