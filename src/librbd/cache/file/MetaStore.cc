// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/file/MetaStore.h"
#include "include/stringify.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include <string>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::file::MetaStore: " << this \
                           << " " <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace file {

template <typename I>
MetaStore<I>::MetaStore(I &image_ctx, uint32_t block_size)//modified by dingl
  : m_image_ctx(image_ctx), m_block_size(block_size),
    m_cache_file_size(image_ctx.ssd_cache_size), 
    m_meta_file(image_ctx, *image_ctx.op_work_queue, image_ctx.id + ".meta") {
    CephContext *cct = m_image_ctx.cct;
    ldout(cct, 20) << "m_cache_file_size is:" << m_cache_file_size << dendl;
}

template <typename I>
void MetaStore<I>::init(bufferlist *bl, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO
  Context *ctx = new FunctionContext(
    [this, bl, on_finish, cct](int r) {
      if (r < 0) {
        on_finish->complete(r);
        return;
      }
      //check if exists? yes: load_all, no: truncate
      if(m_meta_file.filesize() == 0) {
        reset(on_finish);
      } else {
        ldout(cct, 20) << "begin load all"<< dendl;
        load_all(bl, on_finish);
		ldout(cct, 20) << "end load all"<< dendl;
      }
    });
  m_meta_file.open(ctx);
}

template <typename I>
void MetaStore<I>::shut_down(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO
  m_meta_file.close(on_finish);
}

template <typename I>
void MetaStore<I>::reset(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO
  //m_meta_file.truncate(offset_to_block(m_image_ctx.size) * m_entry_size, false, on_finish);
  //meta文件大小跟cache文件大小有关
  ldout(cct, 5) << "m_cache_file_size is:" << m_cache_file_size << dendl;
  m_meta_file.truncate(offset_to_block(m_cache_file_size) * m_entry_size, false, on_finish);//add by dingl
  
}

template <typename I>
void MetaStore<I>::set_entry_size(uint32_t entry_size) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  ldout(cct, 5) << "entry_size is:" << entry_size << dendl;
  //TODO persistent to metastore header
  m_entry_size = entry_size;
}
template <typename I>
void MetaStore<I>::write_block(uint64_t cache_block, bufferlist bl, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;
  uint64_t meta_block_offset = cache_block * m_entry_size;
  m_meta_file.write(meta_block_offset, std::move(bl), false, on_finish);
}

template <typename I>
void MetaStore<I>::read_block(uint64_t cache_block, bufferlist *bl, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;
  uint64_t meta_block_offset = cache_block * m_entry_size;
  m_meta_file.read(meta_block_offset, m_entry_size, bl, on_finish);
}

//read block sync, add by dingl
template <typename I>
int MetaStore<I>::read_block_sync(uint64_t cache_block, bufferlist *bl) {
  CephContext *cct = m_image_ctx.cct;
  //ldout(cct, 20) << dendl;
  uint64_t meta_block_offset = cache_block * m_entry_size;
  return m_meta_file.read_sync(meta_block_offset, m_entry_size, bl);
}

template <typename I>
void MetaStore<I>::load_all(bufferlist *bl, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  uint64_t valid_block_count = 0;
  ldout(cct, 20) << dendl;
  //1. get total file length
  Context *ctx = new FunctionContext(
    [this, on_finish, cct](int r) {
      if (r < 0) {
	  	lderr(cct) << "error to read block" << dendl;
        on_finish->complete(r);
        return;
      }
  });
  //for(uint64_t block_id = 0; block_id < offset_to_block(m_image_ctx.size); block_id++){//modyfied by dingl
  for(uint64_t block_id = 0; block_id < offset_to_block(m_cache_file_size); block_id++){
    //read_block(block_id, bl, ctx);
    //if (bl->is_zero()) break;
    //TO BE TEST
	bufferlist bl_tmp;
	int ret;

	ldout(cct, 20) << "read block:" << block_id<< dendl; 
	ret = read_block_sync(block_id, &bl_tmp);
	if (ret < 0) {
		lderr(cct) << "error to read block:" << block_id << dendl;
		on_finish->complete(ret);
		return;
	}
    if (bl_tmp.is_zero()) {
		ldout(cct, 20) << "bufferlist is zero,skip this" << dendl; 
		bl_tmp.clear();
		continue;
    }
	++valid_block_count;
	ldout(cct, 20) << "read block " << block_id << " done, bl_tmp size is " <<
		bl_tmp.length() << ", buffer count " << bl_tmp.get_num_buffers()<< dendl; 
	bl->claim_append(bl_tmp);
	ldout(cct, 20) << "after claim " << block_id 
		<< " done, bl_tmp size is " <<bl_tmp.length() 
		<< ", bl_tmp buffer count " << bl_tmp.get_num_buffers()<< dendl; 
	ldout(cct, 20) << "bl size is " << bl->length() << ", bl buffer count " 
					<< bl->get_num_buffers()<< dendl; 
  }
  ldout(cct, 6) << "valid block count " << valid_block_count 
  						<< ",bl buffer length " << bl->length()  
  					<< ",bl buffer count " << bl->get_num_buffers() << dendl; 
  on_finish->complete(0);
}

} // namespace file
} // namespace cache
} // namespace librbd

template class librbd::cache::file::MetaStore<librbd::ImageCtx>;
