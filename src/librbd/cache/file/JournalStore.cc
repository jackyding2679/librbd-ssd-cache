// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/file/JournalStore.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include "librbd/cache/BlockGuard.h"
#include "librbd/cache/file/MetaStore.h"
#include "librbd/cache/file/Types.h"

#define dout_context cct
#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::file::JournalStore: " << this \
                           << " " <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace file {

using namespace journal_store;

/// TODO use dynamic amount of in-memory event ref slots
static const uint32_t EVENT_REF_COUNT = 5*1024*1024L;

/// TODO use dynamic ring buffer size
static const uint32_t RING_BUFFER_COUNT = 131072; /// ~512MB of 4K writeback

/// TODO configurable block size
static const uint32_t BLOCK_SIZE = 4096;

template <typename I>
JournalStore<I>::JournalStore(I &image_ctx, BlockGuard &block_guard,
                              MetaStore<I> &metastore)
  : m_image_ctx(image_ctx), m_block_guard(block_guard), m_metastore(metastore),
    m_event_file(image_ctx, *image_ctx.op_work_queue,
                 image_ctx.id + ".journal_events"),
    m_block_file(image_ctx, *image_ctx.op_work_queue,
                 image_ctx.id + ".journal_blocks"),
    m_lock("librbd::cache::file::JournalStore::m_lock"), cct(image_ctx.cct){
 // CephContext *cct = m_image_ctx.cct;
  m_ring_buf_cnt = cct->_conf->rbd_persistent_cache_journal_ring_buffer_count;
  m_event_ref_cnt = cct->_conf->rbd_persistent_cache_journal_event_ref_count;
  m_block_size = cct->_conf->rbd_persistent_cache_journal_block_size;

  m_event_ref_pool.resize(m_event_ref_cnt);
  for (auto it = m_event_ref_pool.rbegin(); it != m_event_ref_pool.rend();
       ++it) {
    m_event_refs.push_front(*it);
  }
  m_event_ref_alloc_iter = m_event_refs.begin();
  m_event_ref_writeback_iter = m_event_refs.begin();
  m_event_ref_commit_iter = m_event_refs.begin();

  //add by dingl
  m_need_commit_journal = false;
}

template <typename I>
void JournalStore<I>::init(bufferlist *bl, Context *on_finish) {
  // TODO don't reset the writeback journal
  Context *ctx = new FunctionContext(
    [this, bl, on_finish](int r) {
      if (r < 0) {
        on_finish->complete(r);
        return;
      }

	  //add by dingl
	  if (m_event_file.filesize() > 0) {
	  	load_events(bl, on_finish);
		if (bl->length() > 0) {
			m_need_commit_journal = true;
		}
	  } else {
      	reset(on_finish);
	  }
    });
  ctx = new FunctionContext(
    [this, ctx](int r) {
      if (r < 0) {
        ctx->complete(r);
        return;
      }
      m_block_file.open(ctx);
    });
  m_event_file.open(ctx);

  //add by dingl
  set_encoded_event_size();
}

template <typename I>
void JournalStore<I>::shut_down(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO
  Context *ctx = new FunctionContext(
    [this, on_finish](int r) {
      Context *next_ctx = on_finish;
      if (r < 0) {
        next_ctx = new FunctionContext(
          [this, next_ctx, r](int _r) {
            next_ctx->complete(r);
          });
      }
      m_event_file.close(next_ctx);
    });
  m_block_file.close(ctx);
}

template <typename I>
void JournalStore<I>::reset(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO
  Context *ctx = new FunctionContext(
    [this, on_finish](int r) {
      Context *next_ctx = on_finish;
      if (r < 0) {
        next_ctx = new FunctionContext(
          [next_ctx, r](int _r) {
            next_ctx->complete(r);
          });
      }
      m_event_file.truncate(m_ring_buf_cnt * Event::ENCODED_SIZE, false,
                            next_ctx);
    });
  m_block_file.truncate(m_ring_buf_cnt * m_block_size, false, ctx);
}

template <typename I>
int JournalStore<I>::allocate_tid(uint64_t *tid) {
  CephContext *cct = m_image_ctx.cct;
  Mutex::Locker locker(m_lock);
  //add by dingl
  if (m_event_ref_alloc_iter == m_event_refs.end()) {
     lderr(cct) << "iterator reach the end of the list" << dendl;
     return -ENOMEM;
   } 
  EventRef &event_ref = *m_event_ref_alloc_iter;
  if (event_ref.event_state != EVENT_STATE_INVALID) {
    ldout(cct, 20) << "no free journal slots" << dendl;
    return -ENOMEM;
  }
  ++m_event_ref_alloc_iter;
   //add by dingl
  if (m_event_ref_alloc_iter == m_event_refs.end()) {
     ldout(cct, 5) << "iterator m_event_ref_alloc_iter reach the end of the list" << dendl;
     m_event_ref_alloc_iter = m_event_refs.begin();
  }

  event_ref.tid = ++m_tid;
  event_ref.event_state = EVENT_STATE_ALLOCATED;
  m_tid_to_event_refs.insert(event_ref);

  ldout(cct, 20) << "tid=" << event_ref.tid << dendl;
  *tid = event_ref.tid;
  return 0;
}

template <typename I>
void JournalStore<I>::append_event(uint64_t tid, uint64_t image_block,
					uint64_t cache_block, IOType io_type, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "tid=" << tid << ", "
                 << "image_block=" << image_block << ", "
                 << "cache_block=" << cache_block << ", "
                 << "io_type=" << io_type << dendl;

  EventRef *event_ref;
  {
    // only need to hold lock long enough to get pointer to event
    Mutex::Locker locker(m_lock);
    EventRef key;
    key.tid = tid;
    auto it = m_tid_to_event_refs.find(key);
    assert(it != m_tid_to_event_refs.end());

    event_ref = &(*it);
    //m_block_to_tids[block] = tid;
    //modified by dingl
    m_block_to_tids[image_block] = tid;
  }

  // ring-buffer event offset can be calculated
  size_t event_idx = event_ref - &m_event_ref_pool.front();
  //add by dingl
  ldout(cct, 6) << "before modulo, event_idx " << event_idx << dendl;
  event_idx %= m_ring_buf_cnt;
  ldout(cct, 6) << "after modulo, event_idx " << event_idx << dendl;
  uint64_t event_offset = event_idx * Event::ENCODED_SIZE;

  // on-disk event format
  Event event;
  memset(&event, 0, sizeof(event));//add by dingl
  event.tid = tid;
  event.image_block = image_block;//modified by dingl
  event.cache_block = cache_block;
  event.crc = ceph_crc32c(0, (unsigned char *)&event, Event::EVENT_CRC_LENGTH);
  event.fields.io_type = io_type;
  event.fields.demoted = false;
  event.fields.committed = false;
  event.journal_event_idx = event_idx;

  // in-memory event format
  event_ref->image_block = image_block;//modified by dingl
  event_ref->cache_block = cache_block;
  event_ref->journal_event_idx = event_idx;
  event_ref->io_type = io_type;
  event_ref->event_state = EVENT_STATE_VALID;
  event_ref->demoted = false;

  bufferlist bl;
  ::encode(event, bl);
  m_event_file.write(event_offset, std::move(bl), true, on_finish);
}

template <typename I>
bool JournalStore<I>::is_demote_required(uint64_t block) {
  EventRef *event_ref;
  {
    // only need to hold lock long enough to get pointer to event
    Mutex::Locker locker(m_lock);
    auto block_tid_it = m_block_to_tids.find(block);
    if (block_tid_it == m_block_to_tids.end()) {
      return false;
    }

    EventRef key;
    key.tid = block_tid_it->second;
    auto it = m_tid_to_event_refs.find(key);
    assert(it != m_tid_to_event_refs.end());

    event_ref = &(*it);
  }

  bool demote_required = (event_ref->io_type == IO_TYPE_WRITE);

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "block=" << block << ", "
                 << "demote_required=" << demote_required << dendl;
  return demote_required;
}

template <typename I>
void JournalStore<I>::demote_block(uint64_t block, bufferlist &&bl,
                                   Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  EventRef *event_ref;
  {
    // only need to hold lock long enough to get pointer to event
    Mutex::Locker locker(m_lock);
    auto block_tid_it = m_block_to_tids.find(block);
    assert(block_tid_it != m_block_to_tids.end());

    EventRef key;
    key.tid = block_tid_it->second;
    auto it = m_tid_to_event_refs.find(key);
    assert(it != m_tid_to_event_refs.end());

    event_ref = &(*it);
  }

  assert(event_ref->io_type == IO_TYPE_WRITE);
  assert(bl.length() == m_block_size);

  // ring-buffer event offset can be calculated
  size_t event_idx = event_ref - &m_event_ref_pool.front();
  //add by dingl
  ldout(cct, 6) << "before modulo, event_idx " << event_idx << dendl;
  event_idx %= m_ring_buf_cnt;
  ldout(cct, 6) << "after modulo, event_idx " << event_idx << dendl;
  uint64_t event_offset = event_idx * Event::ENCODED_SIZE;
  uint64_t block_offset = event_idx * m_block_size;

  Context *ctx = new FunctionContext(
    [this, event_ref, event_offset, event_idx, on_finish](int r) {
      if (r < 0) {
        // TODO
        on_finish->complete(r);
        return;
      }

      // block is still detained -- safe to flag as demoted
      event_ref->demoted = true;
	  event_ref->journal_event_idx = event_idx;//add by d

      Event event;
      event.fields.io_type = event_ref->io_type;
      event.fields.demoted = true;
      event.fields.allocated = true;
      event.fields.committed = false; // NOTE: block locked, writeback not in-progress
      event.journal_event_idx = event_idx;//add by d

      bufferlist event_bl;
      event.encode_fields(event_bl);
      m_event_file.write(event_offset + Event::ENCODED_FIELDS_OFFSET,
                         std::move(event_bl), true, on_finish);
    });
  m_block_file.write(block_offset, std::move(bl), true, ctx);
}

template <typename I>
bool JournalStore<I>::is_writeback_pending() const {
  Mutex::Locker locker(m_lock);
  return (m_event_ref_writeback_iter != m_event_ref_alloc_iter &&
          m_event_ref_commit_iter == m_event_ref_writeback_iter);

}

template <typename I>
int JournalStore<I>::get_writeback_event(uint64_t *tid, uint64_t *image_block, 
  					uint64_t *cache_block, IOType *io_type, bool *demoted) {
  CephContext *cct = m_image_ctx.cct;
  EventRef *event_ref;
  {
    Mutex::Locker locker(m_lock);
    if (m_event_ref_writeback_iter == m_event_ref_alloc_iter) {
      lderr(cct) << "no blocks available" << dendl;
      return -ENODATA;
    }

    event_ref = &(*m_event_ref_writeback_iter);
    if (event_ref->event_state != EVENT_STATE_VALID) {
      ldout(cct, 20) << "no writeback blocks available" << dendl;
      return -ENODATA;
    }

    // if block guard full or block already detained, cannot writeback
    //int r = m_block_guard.detain(event_ref->block, nullptr);
    //detain image block,in case this image block is being dirty-write, add by dingl
    int r = m_block_guard.detain(event_ref->image_block, nullptr);
    if (r != 0) {
      ldout(cct, 20) << "block " << event_ref->image_block << " busy" << dendl;
      return -EBUSY;
    }

    ++m_event_ref_writeback_iter;
    //add by dingl
    if (m_event_ref_writeback_iter == m_event_refs.end()) {
      lderr(cct) << "iterator m_event_ref_writeback_iter reach the end of the list" << dendl;
       m_event_ref_writeback_iter = m_event_refs.begin();
    }
    event_ref->event_state = EVENT_STATE_WRITEBACK;
  }

  ldout(cct, 20) << "tid=" << event_ref->tid << ", "
                 << "image block=" << event_ref->image_block << ", "
                 << "cache block=" << event_ref->cache_block << ", "
                 << "io_type=" << event_ref->io_type << ", "
                 << "demoted=" << event_ref->demoted << dendl;
  *tid = event_ref->tid;
  *image_block = event_ref->image_block;//modified by dingl
  *cache_block = event_ref->cache_block;
  *io_type = event_ref->io_type;
  *demoted = event_ref->demoted;
  return 0;
}

template <typename I>
void JournalStore<I>::get_writeback_block(uint64_t tid, bufferlist *bl,
                                          Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  EventRef *event_ref;
  {
    Mutex::Locker locker(m_lock);

    EventRef key;
    key.tid = tid;
    auto it = m_tid_to_event_refs.find(key);
    assert(it != m_tid_to_event_refs.end());

    event_ref = &(*it);
  }

  assert(event_ref->tid == tid);
  assert(event_ref->demoted);

  // ring-buffer event offset can be calculated
  size_t event_idx = event_ref - &m_event_ref_pool.front();
  //add by dingl
  ldout(cct, 6) << "before modulo, event_idx " << event_idx << dendl;
  event_idx %= m_ring_buf_cnt;
  ldout(cct, 6) << "after modulo, event_idx " << event_idx << dendl;
  uint64_t block_offset = event_idx * m_block_size;
  m_block_file.read(block_offset, m_block_size, bl, on_finish);
}

template <typename I>
void JournalStore<I>::commit_event(uint64_t tid, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << "tid=" << tid << dendl;
  //ldout(cct, -1) << "tid=" << tid << dendl;

  EventRef *event_ref;
  {
    Mutex::Locker locker(m_lock);

    EventRef key;
    key.tid = tid;
    auto it = m_tid_to_event_refs.find(key);
    assert(it != m_tid_to_event_refs.end());

    event_ref = &(*it);

    //auto block_it = m_block_to_tids.find(event_ref->block);
    //modified by dingl
    auto block_it = m_block_to_tids.find(event_ref->image_block);
    assert(block_it != m_block_to_tids.end());
    if (block_it->second == tid) {
      m_block_to_tids.erase(block_it);
    }
  }

  // ring-buffer event offset can be calculated
  size_t event_idx = event_ref - &m_event_ref_pool.front();
  //add by dingl
  ldout(cct, 6) << "before modulo, event_idx " << event_idx << dendl;
  event_idx %= m_ring_buf_cnt;
  ldout(cct, 6) << "after modulo, event_idx " << event_idx << dendl;
  assert(event_idx == event_ref->journal_event_idx);
  uint64_t event_offset = (event_idx * Event::ENCODED_SIZE) +
                          Event::ENCODED_FIELDS_OFFSET;
  //uint64_t block = event_ref->block;
  //modified by dingl
  uint64_t block_offset = event_idx * m_block_size;
  //uint64_t image_block = event_ref->image_block;
  //uint64_t cache_block = event_ref->cache_block;

  Event event;
  memset(&event, 0, sizeof(event));//add by dingl
  event.fields.io_type = event_ref->io_type;
  event.fields.demoted = event_ref->demoted;
  event.fields.allocated = event_ref->demoted;
  event.fields.committed = true;
  event.journal_event_idx = event_ref->journal_event_idx;

  //ldout(cct, 5) << "test1------------" << dendl;

  Context *ctx = new FunctionContext(
    [this, event_ref, on_finish, cct](int r) {
      Context *next_ctx = on_finish;
      if (r < 0) {
        next_ctx = new FunctionContext(
          [on_finish, r](int _r) {
            // TODO
            on_finish->complete(r);
          });
      }
     // ldout(cct, 5) << "test3------------" << dendl;
      {
        Mutex::Locker locker(m_lock);
        event_ref->event_state = EVENT_STATE_COMMITTED;

        while (m_event_ref_commit_iter != m_event_ref_writeback_iter) {
          EventRef &ref = *m_event_ref_commit_iter;
          if (ref.event_state != EVENT_STATE_COMMITTED) {
            break;
          }
          ref.event_state = EVENT_STATE_INVALID;
          m_tid_to_event_refs.erase(ref);//add by dingl
          ++m_event_ref_commit_iter;
          if (m_event_ref_commit_iter == m_event_refs.end()) {//add by dingl
          	lderr(cct) << "iterator m_event_ref_commit_iter reach the end of the list" << dendl;
	       	m_event_ref_commit_iter = m_event_refs.begin();
	      }
        }
      }

      next_ctx->complete(0);
    });
  if (event_ref->demoted) {
    // need to chain the clean-up for consistency
    ctx = new FunctionContext(
      [this, event, event_offset, ctx](int r) {
        Context *next_ctx = ctx;
        if (r < 0) {
          next_ctx = new FunctionContext(
            [next_ctx, r](int _r) {
              // TODO
              next_ctx->complete(r);
            });
        }

        Event event_copy;
        event_copy.fields = event.fields;
        event_copy.fields.allocated = false;

        bufferlist event_bl;
        event_copy.encode_fields(event_bl);
        m_event_file.write(event_offset, std::move(event_bl), false, next_ctx);
      });
    ctx = new FunctionContext(
     // [this, block, ctx](int r) {
      [this, block_offset, ctx](int r) {
        Context *next_ctx = ctx;
        if (r < 0) {
          next_ctx = new FunctionContext(
            [next_ctx, r](int _r) {
              // TODO
              next_ctx->complete(r);
            });
        }
        //m_block_file.discard(block * m_block_size, m_block_size, true, next_ctx);
        //modified by dingl
        m_block_file.discard(block_offset * m_block_size, m_block_size, true, next_ctx);
      });
  }

 //ldout(cct, 5) << "test2------------" << dendl;

  // TODO throttle commit updates
  bufferlist event_bl;
  event_bl.clear();//add by dingl
  event.encode_fields(event_bl);
  m_event_file.write(event_offset, std::move(event_bl), (ctx != on_finish),
                     ctx);
}

//set encoded event size,add by dingl
template <typename I>
void JournalStore<I>::set_encoded_event_size()
{
	CephContext *cct = m_image_ctx.cct;
	Event e;
	bufferlist bl;

	e.encode(bl);
	ldout(cct, 6) <<"encoded event size " << bl.length() << dendl;
	Mutex::Locker locker(m_lock);
	encoded_event_size = bl.length();
}

//get encoded envet size, add by dingl
template <typename I>
uint32_t JournalStore<I>::get_encoded_event_size() const {
	return encoded_event_size;
}

//read a event,add by dingl
template <typename I> 
int JournalStore<I>::read_event_sync(uint32_t event, bufferlist *bl) {
	uint64_t event_offset;

	event_offset = event * Event::ENCODED_SIZE;
	return m_event_file.read_sync(event_offset, Event::ENCODED_SIZE, bl);
}

//load all events in journal,add by dingl
template <typename I> 
void JournalStore<I>::load_events(bufferlist *bl, Context *on_finish) {
	 uint32_t uncommit_event_num = 0;
	 
     for(uint32_t event = 0; event < m_ring_buf_cnt; ++event) {
	 	bufferlist bl_tmp;
		bufferlist::iterator it;
		Event e;
		int ret;

		ldout(cct, 20) << "read event:" << event << dendl; 
		ret = read_event_sync(event, &bl_tmp);
		if (ret < 0) {
			lderr(cct) << "error to read event:" << event << dendl;
			on_finish->complete(ret);
			return;
		}
		if (bl_tmp.is_zero()) {
			ldout(cct, 20) << "bufferlist is zero,skip this" << dendl; 
			bl_tmp.clear();
			continue;
		}
		it = bl_tmp.begin();
		e.decode(it);
		//debug
		dump_event(&e, 6);
		if (e.fields.committed) {
			ldout(cct, 20) << "event has been commited,ignore it" << dendl;
			bl_tmp.clear();
			continue;
		}
		++uncommit_event_num;
        bl->claim_append(bl_tmp);
     }
     ldout(cct, 6) << "uncommited event number " << uncommit_event_num << dendl; 
	 on_finish->complete(0);
}

//dump event, add by dingl
template <typename I>
void JournalStore<I>::dump_event(journal_store::Event *e, int log_level) {
	dout(log_level) << " event dump:\n";
	JSONFormatter f(false);
	f.open_object_section("Event");
	e->dump(&f);
	f.close_section();
	f.flush(*_dout);
	*_dout << dendl;
}

//get journal block, add by dingl
template <typename I>
void JournalStore<I>::get_journal_block(uint64_t block, bufferlist *bl,
											Context *on_finish) {
	ldout(cct, 6) << "read block " << block << dendl;
	uint64_t journal_offset = block * m_block_size;
	m_block_file.read(journal_offset, m_block_size, bl, on_finish);
}

//commmit event when replaying journal, add by dingl
template <typename I>
void JournalStore<I>::commit_event(uint64_t event_idx, 
				IOType io_type, bool demoted, Context *on_finish) {
	ldout(cct, 6) << "commit journal event " << event_idx << dendl;
	Context *ctx = on_finish;

	Event event;
	memset(&event, 0, sizeof(event));//add by dingl
	event.fields.io_type = io_type;
	event.fields.demoted = demoted;
	event.fields.allocated = demoted;
	event.fields.committed = true;
	event.journal_event_idx = event_idx;
	uint64_t block_offset = event_idx * m_block_size;
	uint64_t event_offset = (event_idx * Event::ENCODED_SIZE) +
							Event::ENCODED_FIELDS_OFFSET;

	if (demoted) {
		ctx = new FunctionContext(
	 	[this, event, event_offset, ctx](int r) {
	   		Context *next_ctx = ctx;
	   		if (r < 0) {
		 		next_ctx = new FunctionContext(
		  	 	[next_ctx, r](int _r) {
			 		// TODO
			 		next_ctx->complete(r);
		   		});
	   		}

	   		Event event_copy;
	   		event_copy.fields = event.fields;
	   		event_copy.fields.allocated = false;

	   		bufferlist event_bl;
	   		event_copy.encode_fields(event_bl);
	   		m_event_file.write(event_offset, std::move(event_bl), true, next_ctx);
	 	});
		
		ctx = new FunctionContext(
		 [this, block_offset, ctx](int r) {
	  	 	Context *next_ctx = ctx;
	   		if (r < 0) {
		 		next_ctx = new FunctionContext(
		   		[next_ctx, r](int _r) {
			 		// TODO
			 		next_ctx->complete(r);
		   		});
	   		}
	   		m_block_file.discard(block_offset * m_block_size, 
										m_block_size, true, next_ctx);
	 	});
	}

	bufferlist event_bl;
	event.encode_fields(event_bl);
	m_event_file.write(event_offset, std::move(event_bl), true, ctx);
}

//check event, add by dingl
template <typename I>
int JournalStore<I>::check_event(journal_store::Event &e) {
	uint32_t read_crc = e.crc;
	uint32_t calc_crc = 0;
	int ret = 0;

	if (e.tid < 0) {
		ret = -1;
		lderr(cct) << "bad tid " << e.tid << dendl;
	}
	
	if (e.image_block < 0 || e.image_block > (m_image_ctx.size / BLOCK_SIZE)) {
		ret = -1;
		lderr(cct) << "bad image_block " << e.image_block << dendl;
	}

	if (e.cache_block < 0 ||
			e.cache_block > (m_image_ctx.ssd_cache_size / BLOCK_SIZE)) {
		ret = -1;
		lderr(cct) << "bad cache_block " << e.cache_block << dendl;
	}

	if (e.journal_event_idx < 0 || e.journal_event_idx > m_ring_buf_cnt) {
		ret = -1;
		lderr(cct) << "bad journal event " << e.journal_event_idx << dendl;
	}

	calc_crc = ceph_crc32c(0, (unsigned char *)&e, Event::EVENT_CRC_LENGTH);
	if (read_crc != calc_crc) {
		ret = -1;
		lderr(cct) << "bad event crc, read_crc " << e.crc 
							<< "calc_crc " << calc_crc << dendl;
	}

	return ret;
}


} // namespace file
} // namespace cache
} // namespace librbd

template class librbd::cache::file::JournalStore<librbd::ImageCtx>;
