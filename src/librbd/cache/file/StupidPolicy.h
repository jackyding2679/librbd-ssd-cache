// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_FILE_STUPID_POLICY
#define CEPH_LIBRBD_CACHE_FILE_STUPID_POLICY

#include "librbd/cache/file/Policy.h"
#include "include/lru.h"
#include "common/Mutex.h"
#include "librbd/cache/file/Types.h"
#include <unordered_map>
#include <vector>

namespace librbd {

struct ImageCtx;

namespace cache {

struct BlockGuard;

namespace file {

/**
 * Stupid LRU-style policy
 */
using namespace stupid_policy;

template <typename ImageCtxT>
class StupidPolicy : public Policy {
public:
  StupidPolicy(ImageCtxT &image_ctx, BlockGuard &block_guard);

  virtual void set_write_mode(uint8_t write_mode);
  virtual uint8_t get_write_mode();
  virtual void set_block_count(uint64_t block_count);

  virtual int invalidate(uint64_t block);

  virtual bool contains_dirty() const;
  virtual bool is_dirty(uint64_t block) const;
  virtual void set_dirty(uint64_t block);
  virtual void clear_dirty(uint64_t block);

  virtual int get_writeback_block(uint64_t *block);

  virtual int map(IOType io_type, uint64_t block, bool partial_block,
                         PolicyMapResult *policy_map_result,
                         uint64_t *cache_block, uint64_t *replace_image_block);
  virtual void tick();
  virtual int get_entry_size();
  virtual void entry_to_bufferlist(uint64_t block, bufferlist *bl);
  //virtual void bufferlist_to_entry(bufferlist &bl);
  //modified by dingl
  virtual void bufferlist_to_entry(bufferlist *bl);

  //add by dingl
  void dump_entry(Entry_t *e, int log_level);

private:

	CephContext *cct;//add by dingl
 /* struct Entry : public LRUObject {
    uint64_t block = 0;
    bool dirty = false;
  };*/
  //modified by dingl
   struct Entry : public LRUObject {
	 uint64_t cache_block = 0;
	 uint64_t image_block = 0;
	 bool dirty = false;
   };

  typedef std::vector<Entry> Entries;
  typedef std::unordered_map<uint64_t, Entry*> BlockToEntries;

  ImageCtxT &m_image_ctx;
  BlockGuard &m_block_guard;

  mutable Mutex m_lock;
  //uint64_t m_block_count = 0;
  //modidied by dingl
  uint64_t m_image_block_count;
  uint64_t m_cache_block_count;
	
  uint8_t m_write_mode = 0; // 0:w-t, 1:w-b

  Entries m_entries;
  BlockToEntries m_block_to_entries;

  LRUList m_free_lru;
  LRUList m_clean_lru;
  mutable LRUList m_dirty_lru;

};

} // namespace file
} // namespace cache
} // namespace librbd

extern template class librbd::cache::file::StupidPolicy<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_FILE_STUPID_POLICY
