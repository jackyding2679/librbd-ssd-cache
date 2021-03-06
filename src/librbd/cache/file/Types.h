// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_FILE_TYPES
#define CEPH_LIBRBD_CACHE_FILE_TYPES

#include "include/buffer_fwd.h"
#include "include/encoding.h"
#include "include/int_types.h"
#include "librbd/cache/Types.h"
#include <deque>
#include <list>
#include <type_traits>
#include <utility>

namespace ceph { struct Formatter; }

namespace librbd {
namespace cache {
namespace file {

/**
 * Persistent on-disk cache structures
 */

namespace stupid_policy {

struct Entry_t {
  //static const size_t ENCODED_SIZE = 32;
  bool dirty = false;
  uint64_t cache_block = 0;//modified by dingl
  uint64_t image_block = 0;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;//add by dingl
};

} // namespace stupid_policy

namespace meta_store {

struct Header {

  // TODO using ring-buffer -- use separate files as alternative (?)
  uint8_t journal_sequence = 0;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<Header *> &o);
};

} // namespace meta_store

namespace journal_store {

struct Event {
  static const size_t ENCODED_SIZE = 64;
  //static const size_t ENCODED_FIELDS_OFFSET = 26;
  //modified by dingl
  static const size_t ENCODED_FIELDS_OFFSET = 34;
  static const size_t EVENT_CRC_LENGTH = 24;//tid, image_block, cache_block

  uint64_t tid;   /// TODO can this be eliminated safely?
  uint64_t image_block;//modified by dingl
  uint64_t cache_block;
  uint32_t crc;   /// TODO

  struct {
    IOType io_type : 2;
    bool demoted : 1;
    bool committed : 1;
    bool allocated : 1;
    //uint8_t  io_type : 2;
    //uint8_t demoted : 1;
    //uint8_t committed : 1;
    //uint8_t allocated : 1;
  } fields;
  uint64_t journal_event_idx;//add by dingl
  
  void encode_fields(bufferlist& bl) const;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<Event *> &o);
};
typedef std::deque<Event> Events;

struct EventBlock {
  uint64_t sequence;
  Events events;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<EventBlock *> &o);
};

} // namespace journal_store

} // namespace file
} // namespace cache
} // namespace librbd

WRITE_CLASS_ENCODER(librbd::cache::file::stupid_policy::Entry_t);
WRITE_CLASS_ENCODER(librbd::cache::file::meta_store::Header);
WRITE_CLASS_ENCODER(librbd::cache::file::journal_store::Event);
WRITE_CLASS_ENCODER(librbd::cache::file::journal_store::EventBlock);

#endif // CEPH_LIBRBD_CACHE_FILE_TYPES
