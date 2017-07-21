// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/file/StupidPolicy.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include "librbd/cache/BlockGuard.h"

#define dout_context cct
#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::file::StupidPolicy: " << this \
                           << " " <<  __func__ << ": "

#define BLOCK_SIZE 4096

namespace librbd {
namespace cache {
namespace file {

template <typename I>
StupidPolicy<I>::StupidPolicy(I &image_ctx, BlockGuard &block_guard)
  : cct(image_ctx.cct), 
    m_image_ctx(image_ctx), m_block_guard(block_guard),
    m_lock("librbd::cache::file::StupidPolicy::m_lock"),
    m_image_block_count(m_image_ctx.size / BLOCK_SIZE), 
    m_cache_block_count(m_image_ctx.ssd_cache_size / BLOCK_SIZE) {
    CephContext *cct = image_ctx.cct;
	uint64_t cache_block_id = 0;

  // TODO support resizing of entries based on number of provisioned blocks
  //m_entries.resize(m_image_ctx.ssd_cache_size / BLOCK_SIZE); // 1GB of storage
  m_entries.resize(m_cache_block_count); 
  for (auto &entry : m_entries) {
    m_free_lru.insert_tail(&entry);
  	entry.cache_block = cache_block_id;//modified by dingl
	++cache_block_id;
  }
  ldout(cct, 5) << "cache_block_id is:" << cache_block_id << dendl;
}

template <typename I>
void StupidPolicy<I>::set_write_mode(uint8_t write_mode) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "write_mode=" << write_mode << dendl;

  // TODO change mode on-the-fly
  Mutex::Locker locker(m_lock);
  m_write_mode = write_mode;

}

template <typename I>
uint8_t StupidPolicy<I>::get_write_mode() {
  CephContext *cct = m_image_ctx.cct;

  // TODO change mode on-the-fly
  Mutex::Locker locker(m_lock);
  return m_write_mode;

}

template <typename I>
void StupidPolicy<I>::set_block_count(uint64_t block_count) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "block_count=" << block_count << dendl;

  // TODO ensure all entries are in-bound
  Mutex::Locker locker(m_lock);
  //m_block_count = block_count;
  m_image_block_count = block_count;

}

template <typename I>
int StupidPolicy<I>::invalidate(uint64_t block) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "block=" << block << dendl;

  // TODO handle case where block is in prison (shouldn't be possible
  // if core properly registered blocks)

  Mutex::Locker locker(m_lock);
  auto entry_it = m_block_to_entries.find(block);
  if (entry_it == m_block_to_entries.end()) {
    return 0;
  }

  Entry *entry = entry_it->second;
  m_block_to_entries.erase(entry_it);

  LRUList *lru;
  if (entry->dirty) {
    lru = &m_dirty_lru;
  } else {
    lru = &m_clean_lru;
  }
  lru->remove(entry);

  m_free_lru.insert_tail(entry);
  return 0;
}

template <typename I>
bool StupidPolicy<I>::contains_dirty() const {
  Mutex::Locker locker(m_lock);
  return m_dirty_lru.get_tail() != nullptr;
}

template <typename I>
bool StupidPolicy<I>::is_dirty(uint64_t block) const {
  Mutex::Locker locker(m_lock);
  auto entry_it = m_block_to_entries.find(block);
  assert(entry_it != m_block_to_entries.end());

  bool dirty = entry_it->second->dirty;

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "block=" << block << ", "
                 << "dirty=" << dirty << dendl;
  return dirty;
}

template <typename I>
void StupidPolicy<I>::set_dirty(uint64_t block) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "block=" << block << dendl;

  Mutex::Locker locker(m_lock);
  auto entry_it = m_block_to_entries.find(block);
  assert(entry_it != m_block_to_entries.end());

  Entry *entry = entry_it->second;
  if (entry->dirty) {
    return;
  }

  entry->dirty = true;
  m_clean_lru.remove(entry);
  m_dirty_lru.insert_head(entry);
}

template <typename I>
void StupidPolicy<I>::clear_dirty(uint64_t block) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "block=" << block << dendl;

  Mutex::Locker locker(m_lock);
  auto entry_it = m_block_to_entries.find(block);
  assert(entry_it != m_block_to_entries.end());

  Entry *entry = entry_it->second;
  if (!entry->dirty) {
    return;
  }

  entry->dirty = false;
  m_dirty_lru.remove(entry);
  m_clean_lru.insert_head(entry);
}

template <typename I>
int StupidPolicy<I>::get_writeback_block(uint64_t *block) {
  CephContext *cct = m_image_ctx.cct;

  // TODO make smarter writeback policy instead of "as fast as possible"

  Mutex::Locker locker(m_lock);
  Entry *entry = reinterpret_cast<Entry*>(m_dirty_lru.get_tail());
  if (entry == nullptr) {
    ldout(cct, 20) << "no dirty blocks to writeback" << dendl;
    return -ENODATA;
  }

  //int r = m_block_guard.detain(entry->block, nullptr);
  //modified by dingl
  int r = m_block_guard.detain(entry->image_block, nullptr);
  if (r < 0) {
    ldout(cct, 20) << "dirty block " << entry->image_block << " already detained"
                   << dendl;
    return -EBUSY;
  }

  // move to clean list to prevent "double" writeback -- since the
  // block is detained, it cannot be evicted from the cache until
  // writeback is complete
  assert(entry->dirty);
  entry->dirty = false;
  m_dirty_lru.remove(entry);
  m_clean_lru.insert_head(entry);

  //*block = entry->block;
  //modified by dingl
  *block = entry->image_block;
  ldout(cct, 20) << "image_block=" << *block << dendl;
  return 0;
}

template <typename I>
int StupidPolicy<I>::map(IOType io_type, uint64_t block, bool partial_block,
                         PolicyMapResult *policy_map_result,
                         uint64_t *cache_block, uint64_t *replace_image_block) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "block=" << block << dendl;

  Mutex::Locker locker(m_lock);
 // if (block >= m_block_count) {
  if (block >= m_image_block_count) {
    lderr(cct) << "block outside of valid range" << dendl;
    *policy_map_result = POLICY_MAP_RESULT_MISS;
    // TODO return error once resize handling is in-place
    return 0;
  }

  Entry *entry;
  auto entry_it = m_block_to_entries.find(block);
  if (entry_it != m_block_to_entries.end()) {
    // cache hit -- move entry to the front of the queue
    ldout(cct, 20) << "cache hit" << dendl;
    *policy_map_result = POLICY_MAP_RESULT_HIT;

    entry = entry_it->second;
    LRUList *lru;
    if (entry->dirty) {
      lru = &m_dirty_lru;
    } else {
      lru = &m_clean_lru;
    }

    lru->remove(entry);
    lru->insert_head(entry);

	*cache_block = entry->cache_block;//return cache block,add by dingl
    return 0;
  }

  // cache miss
  entry = reinterpret_cast<Entry*>(m_free_lru.get_head());
  if (entry != nullptr) {
    // entries are available -- allocate a slot
    ldout(cct, 20) << "cache miss -- new entry" << dendl;
    *policy_map_result = POLICY_MAP_RESULT_NEW;
	*cache_block = entry->cache_block;//return cache block
    m_free_lru.remove(entry);

    //entry->block = block;
    entry->image_block = block;//modified by dingl
    m_block_to_entries[block] = entry;
    m_clean_lru.insert_head(entry);
    return 0;
  }

  //TODO-move item in clean list to free list,add by dingl
  // if we have clean entries we can demote, attempt to steal the oldest
  entry = reinterpret_cast<Entry*>(m_clean_lru.get_tail());
  if (entry != nullptr) {
    //int r = m_block_guard.detain(entry->block, nullptr);
    int r = m_block_guard.detain(entry->image_block, nullptr);
    if (r >= 0) {                                         
	  ldout(cct, 20) << "cache miss -- replace entry" << dendl;
	  *policy_map_result = POLICY_MAP_RESULT_REPLACE;
	  *replace_image_block = entry->image_block;
	  *cache_block = entry->cache_block;/*find a evict entry£¬
	                                    write data to this cacheblock*/
	  //m_block_to_entries.erase(entry->block);
	  m_block_to_entries.erase(entry->image_block);
	  m_clean_lru.remove(entry);

	  //entry->block = block;
	  entry->image_block = block;
	  m_block_to_entries[block] = entry;
	  m_clean_lru.insert_head(entry);
	  return 0;
    }
    ldout(cct, 20) << "cache miss -- replacement deferred" << dendl;
  } else {
    ldout(cct, 5) << "cache miss" << dendl;
  }

  // no clean entries to evict -- treat this as a miss
  *policy_map_result = POLICY_MAP_RESULT_MISS;
  return 0;
}

template <typename I>
void StupidPolicy<I>::tick() {
  // stupid policy -- do nothing
}

template <typename I>
int StupidPolicy<I>::get_entry_size() {
  //return sizeof(struct Entry); 
  //shouldn't return sizeof(Entry), modified by dingl
  bufferlist bl;
  Entry_t e;
  e.dirty = false;
  e.image_block = 0;
  e.cache_block = 0;
  e.encode(bl);
  return bl.length();
}

template <typename I>
void StupidPolicy<I>::entry_to_bufferlist(uint64_t block, bufferlist *bl){
  Mutex::Locker locker(m_lock);
  auto entry_it = m_block_to_entries.find(block);
  assert(entry_it != m_block_to_entries.end());

  //TODO
  Entry_t entry;
  entry.cache_block = entry_it->second->cache_block;//modified by dingl
  entry.image_block = entry_it->second->image_block;
  entry.dirty = entry_it->second->dirty;
  bufferlist encode_bl;
  entry.encode(encode_bl);
  bl->append(encode_bl);
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "block=" << block << " bl=" << *bl << dendl;
}

template <typename I>
void StupidPolicy<I>::bufferlist_to_entry(bufferlist *bl){
	CephContext *cct = m_image_ctx.cct;

	Mutex::Locker locker(m_lock);
 // uint64_t entry_index = 0;
  //TODO
  //TODO-add to dirty list-add by dingl
	Entry_t entry;
  //for (bufferlist::iterator it = bl.begin(); it != bl.end(); ++it) {
  /*When using encode and iterator at the same time,we shouldn't use operator++*/
	for (bufferlist::iterator it = bl->begin(); it != bl->end();) {
		//entry.decode(it);
		//auto entry_it = m_entries[entry_index++];
		//entry_it.block = entry.block;
		//entry_it.dirty = entry.dirty;
		bool is_dirty;
		uint64_t cache_block;
		uint64_t image_block;

		entry.decode(it);
		is_dirty = entry.dirty;
		image_block = entry.image_block;
		cache_block = entry.cache_block;
		
		if (image_block < 0 || (image_block > m_image_block_count - 1)) {
			lderr(cct) << "invalid image block " << image_block << dendl;
			assert(false);
		}
		if (cache_block < 0 || (cache_block > m_cache_block_count - 1)) {
			lderr(cct) << "invalid cache block " << cache_block << dendl;
			assert(false);
		}
		//Debug
		dump_entry(&entry, 6);
		auto entry_it = m_entries[cache_block];
		//Remove from the free lru list, and insert to the corresponding list
		m_free_lru.remove(&entry_it);
		if (is_dirty) {
			m_dirty_lru.insert_head(&entry_it);
		} else {
			m_clean_lru.insert_head(&entry_it);
		}
		entry_it.cache_block = entry.cache_block;
		entry_it.image_block = entry.image_block;
		entry_it.dirty = entry.dirty;
		//Update the block_to_entries map
		m_block_to_entries[image_block] = &entry_it;
	}
	//debug
	ldout(cct, 6) << "m_free_lru size " << m_free_lru.get_length()
				  << " m_dirty_lru size " << m_dirty_lru.get_length()
				  << " m_clean_lru size " << m_clean_lru.get_length() << dendl;
  //CephContext *cct = m_image_ctx.cct;
  //ldout(cct, 20) << "Total load " << entry_index << " entries" << dendl;
}

//dump Entry_t, add by dingl
template <typename I>
void StupidPolicy<I>::dump_entry(Entry_t *e, int log_level)
{
	dout(log_level) << " Entry_t dump:\n";
	JSONFormatter f(false);
	f.open_object_section("Entry_t");
	e->dump(&f);
	f.close_section();
	f.flush(*_dout);
	*_dout << dendl;
}


} // namespace file
} // namespace cache
} // namespace librbd

template class librbd::cache::file::StupidPolicy<librbd::ImageCtx>;
