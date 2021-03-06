// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "FileImageCache.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "common/AsyncOpTracker.h"
#include "librbd/ImageCtx.h"
#include "librbd/cache/file/ImageStore.h"
#include "librbd/cache/file/JournalStore.h"
#include "librbd/cache/file/MetaStore.h"
#include "librbd/cache/file/StupidPolicy.h"
#include "librbd/cache/file/Types.h"
#include <map>
#include <vector>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::FileImageCache: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {

using namespace librbd::cache::file;

namespace {

typedef std::list<bufferlist> Buffers;
typedef std::map<uint64_t, bufferlist> ExtentBuffers;
typedef std::function<void(uint64_t)> ReleaseBlock;
typedef std::function<void(BlockGuard::BlockIO &)> AppendDetainedBlock;

static const uint32_t BLOCK_SIZE = 4096;

bool is_block_aligned(const ImageCache::Extents &image_extents) {
  for (auto &extent : image_extents) {
    if (extent.first % BLOCK_SIZE != 0 || extent.second % BLOCK_SIZE != 0) {
      return false;
    }
  }
  return true;
}

struct C_BlockIORequest : public Context {
  CephContext *cct;
  C_BlockIORequest *next_block_request;

  C_BlockIORequest(CephContext *cct, C_BlockIORequest *next_block_request)
    : cct(cct), next_block_request(next_block_request) {
  }

  virtual void finish(int r) override {
    ldout(cct, 20) << "(" << get_name() << "): r=" << r << dendl;

    if (r < 0) {
      // abort the chain of requests upon failure
      next_block_request->complete(r);
    } else {
      // execute next request in chain
      next_block_request->send();
    }
  }

  virtual void send() = 0;
  virtual const char *get_name() const = 0;
};

struct C_ReleaseBlockGuard : public C_BlockIORequest {
  uint64_t block;
  ReleaseBlock &release_block;
  BlockGuard::C_BlockRequest *block_request;

  C_ReleaseBlockGuard(CephContext *cct, uint64_t block,
                      ReleaseBlock &release_block,
                      BlockGuard::C_BlockRequest *block_request)
    : C_BlockIORequest(cct, nullptr), block(block),
      release_block(release_block), block_request(block_request) {
  }

  virtual void send() override {
    complete(0);
  }
  virtual const char *get_name() const override {
    return "C_ReleaseBlockGuard";
  }

  virtual void finish(int r) override {
    ldout(cct, 20) << "(" << get_name() << "): r=" << r << dendl;

    // IO operation finished -- release guard
    release_block(block);

    // complete block request
    block_request->complete_request(r);
  }
};

template <typename I>
struct C_PromoteToCache : public C_BlockIORequest {
  ImageStore<I> &image_store;
  BlockGuard::BlockIO block_io;
  const bufferlist &bl;
  uint64_t cache_block_id;

  C_PromoteToCache(CephContext *cct, ImageStore<I> &image_store,
                   BlockGuard::BlockIO &block_io, uint64_t cache_block, //modified by dingl
                   const bufferlist &bl, C_BlockIORequest *next_block_request)
    : C_BlockIORequest(cct, next_block_request),
      image_store(image_store), block_io(block_io), 
      bl(bl), cache_block_id(cache_block) {
  }

  virtual void send() override {
    ldout(cct, 20) << "(" << get_name() << "): "
                   //<< "block=" << block_io.block << dendl;
                   << "block=" << cache_block_id << dendl;
    // promote the clean block to the cache
    bufferlist sub_bl;
    if(bl.length() > BLOCK_SIZE) {
      sub_bl.substr_of(bl, block_io.extents[0].buffer_offset, 
	  						block_io.extents[0].block_length);
    } else {
      sub_bl.append(bl);
    }
    //image_store.write_block(block_io.block, {{0, BLOCK_SIZE}}, std::move(sub_bl),
    //                        this);
    //modified by dingl
    image_store.write_block(cache_block_id, {{0, BLOCK_SIZE}}, std::move(sub_bl),
                             this);
  }
  virtual const char *get_name() const override {
    return "C_PromoteToCache";
  }
};

template <typename I>
struct C_DemoteFromCache : public C_BlockIORequest {
  ImageStore<I> &image_store;
  ReleaseBlock &release_block;
  uint64_t block;

  C_DemoteFromCache(CephContext *cct, ImageStore<I> &image_store,
                    ReleaseBlock &release_block, uint64_t block,
                    C_BlockIORequest *next_block_request)
    : C_BlockIORequest(cct, next_block_request),
      image_store(image_store), release_block(release_block), block(block) {
  }

  virtual void send() override {
    ldout(cct, 20) << "(" << get_name() << "): "
                   << "block=" << block << dendl;
    //image_store.discard_block(block, this);
    //No need to discard block,just release detained block,add by dingl
    complete(0);
  }
  virtual const char *get_name() const override {
    return "C_DemoteFromCache";
  }

  virtual void finish(int r) {
    // IO against the demote block was detained -- release
    release_block(block);

    C_BlockIORequest::finish(r);
  }
};

template <typename I>
struct C_ReadFromCacheRequest : public C_BlockIORequest {
  ImageStore<I> &image_store;
  BlockGuard::BlockIO block_io;
  ExtentBuffers *extent_buffers;
  uint64_t cache_block_id;//add by dingl

  C_ReadFromCacheRequest(CephContext *cct, ImageStore<I> &image_store,
                         BlockGuard::BlockIO &&block_io, uint64_t cache_block, 
                         ExtentBuffers *extent_buffers,
                         C_BlockIORequest *next_block_request)
    : C_BlockIORequest(cct, next_block_request),
      image_store(image_store), block_io(block_io),
      extent_buffers(extent_buffers), cache_block_id(cache_block) {
  }

  virtual void send() override {
    ldout(cct, 20) << "(" << get_name() << "): "
                   << "block_io=[" << block_io << "]" << dendl;
    C_Gather *ctx = new C_Gather(cct, this);
    for (auto &extent : block_io.extents) {
      /*image_store.read_block(block_io.block,
                             {{extent.block_offset, extent.block_length}},
                             &(*extent_buffers)[extent.buffer_offset],
                             ctx->new_sub());*/
      //read data from cache block, add by dingl
      image_store.read_block(cache_block_id,
                             {{extent.block_offset, extent.block_length}},
                             &(*extent_buffers)[extent.buffer_offset],
                             ctx->new_sub());                       
    }
    ctx->activate();
  }
  virtual const char *get_name() const override {
    return "C_ReadFromCacheRequest";
  }
};

template <typename I>
struct C_ReadFromImageRequest : public C_BlockIORequest {
  ImageWriteback<I> &image_writeback;
  BlockGuard::BlockIO block_io;
  ExtentBuffers *extent_buffers;

  C_ReadFromImageRequest(CephContext *cct, ImageWriteback<I> &image_writeback,
                         BlockGuard::BlockIO &&block_io,
                         ExtentBuffers *extent_buffers,
                         C_BlockIORequest *next_block_request)
    : C_BlockIORequest(cct, next_block_request),
      image_writeback(image_writeback), block_io(block_io),
      extent_buffers(extent_buffers) {
  }

  virtual void send() override {
    ldout(cct, 20) << "(" << get_name() << "): "
                   << "block_io=[" << block_io << "]" << dendl;

    // TODO improve scatter/gather to include buffer offsets
    uint64_t image_offset = block_io.block * BLOCK_SIZE;
    C_Gather *ctx = new C_Gather(cct, this);
    for (auto &extent : block_io.extents) {
      image_writeback.aio_read({{image_offset + extent.block_offset,
                                 extent.block_length}},
                               &(*extent_buffers)[extent.buffer_offset],
                               0, ctx->new_sub());
    }
    ctx->activate();
  }
  virtual const char *get_name() const override {
    return "C_ReadFromImageRequest";
  }
};

template <typename I>
struct C_WriteToMetaRequest : public C_BlockIORequest {
  MetaStore<I> &meta_store;
  uint64_t image_block_id;
  uint64_t cache_block_id;
  Policy *policy;

  C_WriteToMetaRequest(CephContext *cct, MetaStore<I> &meta_store,
		                uint64_t image_block, uint64_t cache_block, 
		                Policy *policy, C_BlockIORequest *next_block_request)
    : C_BlockIORequest(cct, next_block_request), meta_store(meta_store),
    image_block_id(image_block), cache_block_id(cache_block), policy(policy) {
  }

  virtual void send() override {
    ldout(cct, 20) << "(" << get_name() << "): "
                   << "cache_block_id=" << cache_block_id << dendl;

    bufferlist meta_bl;
    policy->entry_to_bufferlist(image_block_id, &meta_bl);
    ldout(cct, 20) << "entry_to_bufferlist bl:" << meta_bl << dendl;
    meta_store.write_block(cache_block_id, std::move(meta_bl), this);
  }
  virtual const char *get_name() const override {
    return "C_WriteToMetaRequest";
  }
};

template <typename I>
struct C_WriteToImageRequest : public C_BlockIORequest {
  ImageWriteback<I> &image_writeback;
  BlockGuard::BlockIO block_io;
  const bufferlist &bl;

  C_WriteToImageRequest(CephContext *cct, ImageWriteback<I> &image_writeback,
                        BlockGuard::BlockIO &&block_io, const bufferlist &bl,
                        C_BlockIORequest *next_block_request)
    : C_BlockIORequest(cct, next_block_request),
      image_writeback(image_writeback), block_io(block_io), bl(bl) {
  }

  virtual void send() override {
    ldout(cct, 20) << "(" << get_name() << "): "
                   << "block_io=[" << block_io << "]" << dendl;

    uint64_t image_offset = block_io.block * BLOCK_SIZE;

    ImageCache::Extents image_extents;
    bufferlist scatter_bl;
    for (auto &extent : block_io.extents) {
      image_extents.emplace_back(image_offset + extent.block_offset,
                                 extent.block_length);

      bufferlist sub_bl;
      sub_bl.substr_of(bl, extent.buffer_offset, extent.block_length);
      scatter_bl.claim_append(sub_bl);
    }

    image_writeback.aio_write(std::move(image_extents), std::move(scatter_bl),
                              0, this);
  }
  virtual const char *get_name() const override {
    return "C_WriteToImageRequest";
  }
};

template <typename I>
struct C_ReadBlockFromCacheRequest : public C_BlockIORequest {
  ImageStore<I> &image_store;
  uint64_t block;
  uint32_t block_size;
  bufferlist *block_bl;

  C_ReadBlockFromCacheRequest(CephContext *cct, ImageStore<I> &image_store,
                              uint64_t block, uint32_t block_size,
                               bufferlist *block_bl,
                              C_BlockIORequest *next_block_request)
    : C_BlockIORequest(cct, next_block_request),
      image_store(image_store), block(block), block_size(block_size),
      block_bl(block_bl) {
  }

  virtual void send() override {
    ldout(cct, 20) << "(" << get_name() << "): "
                   << "block=" << block << dendl;
    image_store.read_block(block, {{0, block_size}}, block_bl, this);
  }
  virtual const char *get_name() const override {
    return "C_ReadBlockFromCacheRequest";
  }
};

template <typename I>
struct C_ReadBlockFromImageRequest : public C_BlockIORequest {
  ImageWriteback<I> &image_writeback;
  uint64_t block;
  bufferlist *block_bl;

  C_ReadBlockFromImageRequest(CephContext *cct,
                              ImageWriteback<I> &image_writeback,
                              uint64_t block, bufferlist *block_bl,
                              C_BlockIORequest *next_block_request)
    : C_BlockIORequest(cct, next_block_request),
      image_writeback(image_writeback), block(block), block_bl(block_bl) {
  }

  virtual void send() override {
    ldout(cct, 20) << "(" << get_name() << "): "
                   << "block=" << block << dendl;

    uint64_t image_offset = block * BLOCK_SIZE;
    image_writeback.aio_read({{image_offset, BLOCK_SIZE}}, block_bl, 0, this);
  }
  virtual const char *get_name() const override {
    return "C_ReadBlockFromImageRequest";
  }
};

struct C_CopyFromBlockBuffer : public C_BlockIORequest {
  BlockGuard::BlockIO block_io;
  const bufferlist &block_bl;
  ExtentBuffers *extent_buffers;

  C_CopyFromBlockBuffer(CephContext *cct, const BlockGuard::BlockIO &block_io,
                        const bufferlist &block_bl,
                        ExtentBuffers *extent_buffers,
                        C_BlockIORequest *next_block_request)
    : C_BlockIORequest(cct, next_block_request),
      block_io(block_io), block_bl(block_bl), extent_buffers(extent_buffers) {
  }

  virtual void send() override {
    ldout(cct, 20) << "(" << get_name() << "): "
                   << "block_io=[" << block_io << "]" << dendl;

    for (auto &extent : block_io.extents) {
      bufferlist &sub_bl = (*extent_buffers)[extent.buffer_offset];
      sub_bl.substr_of(block_bl, extent.block_offset, extent.block_length);
    }
    complete(0);
  }
  virtual const char *get_name() const override {
    return "C_CopyFromBlockBuffer";
  }
};

struct C_ModifyBlockBuffer : public C_BlockIORequest {
  BlockGuard::BlockIO block_io;
  const bufferlist &bl;
  bufferlist *block_bl;

  C_ModifyBlockBuffer(CephContext *cct, const BlockGuard::BlockIO &block_io,
                      const bufferlist &bl, bufferlist *block_bl,
                      C_BlockIORequest *next_block_request)
    : C_BlockIORequest(cct, next_block_request),
      block_io(block_io), bl(bl), block_bl(block_bl) {
  }

  virtual void send() override {
    ldout(cct, 20) << "(" << get_name() << "): "
                   << "block_io=[" << block_io << "]" << dendl;

    for (auto &extent : block_io.extents) {
      bufferlist modify_bl;
      if (extent.block_offset > 0) {
        bufferlist sub_bl;
        sub_bl.substr_of(*block_bl, 0, extent.block_offset);
        modify_bl.claim_append(sub_bl);
      }
      if (extent.block_length > 0) {
        bufferlist sub_bl;
        sub_bl.substr_of(bl, extent.buffer_offset, extent.block_length);
        modify_bl.claim_append(sub_bl);
      }
      uint32_t remaining_offset = extent.block_offset + extent.block_length;
      if (remaining_offset < block_bl->length()) {
        bufferlist sub_bl;
        sub_bl.substr_of(*block_bl, remaining_offset,
                         block_bl->length() - remaining_offset);
        modify_bl.claim_append(sub_bl);
      }
      std::swap(*block_bl, modify_bl);
    }
    complete(0);
  }
  virtual const char *get_name() const override {
    return "C_ModifyBlockBuffer";
  }
};

template <typename I>
struct C_AppendEventToJournal : public C_BlockIORequest {
  JournalStore<I> &journal_store;
  uint64_t tid;
  uint64_t image_block_id;//modified by dingl
  uint64_t cache_block_id;
  IOType io_type;

  C_AppendEventToJournal(CephContext *cct, JournalStore<I> &journal_store,
                         uint64_t tid, uint64_t image_block, uint64_t cache_block, 
                         IOType io_type, C_BlockIORequest *next_block_request)
    : C_BlockIORequest(cct, next_block_request),
      journal_store(journal_store), tid(tid), image_block_id(image_block), 
      cache_block_id(cache_block), io_type(io_type) {
  }

  virtual void send() override {
    ldout(cct, 20) << "(" << get_name() << "): "
                   << "tid=" << tid << ", "
                   << "image_block=" << image_block_id << ", "
                   << "cache_block=" << cache_block_id << ", "
                   << "io_type=" << io_type << dendl;
    //journal_store.append_event(tid, block, io_type, this);
    //modified by dingl
    journal_store.append_event(tid, image_block_id, cache_block_id, io_type, this);
  }
  virtual const char *get_name() const override {
    return "C_AppendEventToJournal";
  }
};

template <typename I>
struct C_DemoteBlockToJournal : public C_BlockIORequest {
  JournalStore<I> &journal_store;
  uint64_t block;
  const bufferlist &bl;

  C_DemoteBlockToJournal(CephContext *cct, JournalStore<I> &journal_store,
                         uint64_t block, const bufferlist &bl,
                         C_BlockIORequest *next_block_request)
    : C_BlockIORequest(cct, next_block_request),
      journal_store(journal_store), block(block), bl(bl) {
  }

  virtual void send() override {
    ldout(cct, 20) << "(" << get_name() << "): "
                   << "block=" << block << dendl;

    bufferlist copy_bl;
    copy_bl.append(bl);
    journal_store.demote_block(block, std::move(copy_bl), this);
  }
  virtual const char *get_name() const override {
    return "C_DemoteBlockToJournal";
  }
};

template <typename I>
struct C_ReadBlockRequest : public BlockGuard::C_BlockRequest {
  I &image_ctx;
  ImageWriteback<I> &image_writeback;
  ImageStore<I> &image_store;
  ReleaseBlock &release_block;
  bufferlist *bl;

  ExtentBuffers extent_buffers;
  Buffers promote_buffers;

  C_ReadBlockRequest(I &image_ctx,
                     ImageWriteback<I> &image_writeback,
                     ImageStore<I> &image_store,
                     ReleaseBlock &release_block, bufferlist *bl,
                     Context *on_finish)
    : C_BlockRequest(on_finish), image_ctx(image_ctx),
      image_writeback(image_writeback), image_store(image_store),
      release_block(release_block), bl(bl) {
  }

  virtual void remap(PolicyMapResult policy_map_result,
                     BlockGuard::BlockIO &&block_io, uint64_t cache_block, 
                     uint64_t replace_image_block) {
    assert(block_io.tid == 0);
    CephContext *cct = image_ctx.cct;

    // TODO: consolidate multiple reads into a single request (i.e. don't
    // have 1024 4K requests to read a single object)

    // NOTE: block guard active -- must be released after IO completes
    C_BlockIORequest *req = new C_ReleaseBlockGuard(cct, block_io.block,
                                                         release_block, this);
    switch (policy_map_result) {
    case POLICY_MAP_RESULT_HIT:
      /*req = new C_ReadFromCacheRequest<I>(cct, image_store, std::move(block_io),
                                          &extent_buffers, req);*/
      //modified by dingl
      req = new C_ReadFromCacheRequest<I>(cct, image_store, std::move(block_io),
                                          cache_block, &extent_buffers, req);                                   
      break;
    case POLICY_MAP_RESULT_MISS:
      req = new C_ReadFromImageRequest<I>(cct, image_writeback,
                                          std::move(block_io), &extent_buffers,
                                          req);
      break;
    case POLICY_MAP_RESULT_NEW:
    case POLICY_MAP_RESULT_REPLACE:
      promote_buffers.emplace_back();
      req = new C_CopyFromBlockBuffer(cct, block_io, promote_buffers.back(),
                                      &extent_buffers, req);
      /*req = new C_PromoteToCache<I>(cct, image_store, block_io,
                                    promote_buffers.back(), req);*/
      //modified by dingl
      req = new C_PromoteToCache<I>(cct, image_store, block_io, cache_block, 
                                    promote_buffers.back(), req);
      req = new C_ReadBlockFromImageRequest<I>(cct, image_writeback,
                                               block_io.block,
                                               &promote_buffers.back(), req);
      if (policy_map_result == POLICY_MAP_RESULT_REPLACE) {
        //req = new C_DemoteFromCache<I>(cct, image_store, release_block,
         //                              block_io.block, req);
         ////Just release the detained block in policy when replace, add by dingl
        req = new C_DemoteFromCache<I>(cct, image_store, release_block,
                                       replace_image_block, req);
      }
      break;
    default:
      assert(false);
    }
    req->send();
  }

  virtual void finish(int r) override {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << "(C_ReadBlockRequest): r=" << r << dendl;

    if (r < 0) {
      C_BlockRequest::finish(r);
      return;
    }

    ldout(cct, 20) << "assembling read extents" << dendl;
    for (auto &extent_bl : extent_buffers) {
      ldout(cct, 20) << extent_bl.first << "~" << extent_bl.second.length()
                     << dendl;
      bl->claim_append(extent_bl.second);
    }
    C_BlockRequest::finish(0);
  }
};

template <typename I>
struct C_WriteBlockRequest : BlockGuard::C_BlockRequest {
  I &image_ctx;
  ImageWriteback<I> &image_writeback;
  Policy &policy;
  JournalStore<I> &journal_store;
  ImageStore<I> &image_store;
  MetaStore<I> &meta_store;
  ReleaseBlock &release_block;
  AppendDetainedBlock &append_detain_block;
  bufferlist bl;
  uint32_t block_size;

  Buffers promote_buffers;

  C_WriteBlockRequest(I &image_ctx, ImageWriteback<I> &image_writeback,
                      Policy &policy, JournalStore<I> &journal_store,
                      ImageStore<I> &image_store, MetaStore<I> &meta_store,
                      ReleaseBlock &release_block, AppendDetainedBlock &append_detain_block,
                      bufferlist &&bl, uint32_t block_size, Context *on_finish)
    : C_BlockRequest(on_finish),
      image_ctx(image_ctx), image_writeback(image_writeback), policy(policy),
      journal_store(journal_store), image_store(image_store),
      meta_store(meta_store), release_block(release_block),
      append_detain_block(append_detain_block),
      bl(std::move(bl)), block_size(block_size) {
  }

  virtual void remap(PolicyMapResult policy_map_result,
                     BlockGuard::BlockIO &&block_io, uint64_t cache_block, 
                     uint64_t replace_image_block) {
    CephContext *cct = image_ctx.cct;

    // TODO: consolidate multiple writes into a single request (i.e. don't
    // have 1024 4K requests to read a single object)

    // NOTE: block guard active -- must be released after IO completes
    C_BlockIORequest *req = new C_ReleaseBlockGuard(cct, block_io.block,
                                                         release_block, this);

    if (policy_map_result == POLICY_MAP_RESULT_MISS) {
      req = new C_WriteToImageRequest<I>(cct, image_writeback,
                                         std::move(block_io), bl, req);
    } else {
      if (0 == policy.get_write_mode()) {
        //write-thru
        req = new C_WriteToImageRequest<I>(cct, image_writeback,
                                           std::move(block_io), bl, req);
      } else {
        // block is now dirty -- can't be replaced until flushed
        policy.set_dirty(block_io.block);
      }
      //req = new C_WriteToMetaRequest<I>(cct, meta_store, block_io.block, &policy, req);
      //Write metadata to cache_block
      //No need to write metadata now
	  //req = new C_WriteToMetaRequest<I>(cct, meta_store, block_io.block, 
	  //											cache_block, &policy, req);
	  
      IOType io_type = static_cast<IOType>(block_io.io_type);
      if ((io_type == IO_TYPE_WRITE || io_type == IO_TYPE_DISCARD) &&
          block_io.tid == 0) {
        // TODO support non-journal mode / writethrough-only
        int r = journal_store.allocate_tid(&block_io.tid);
        if (r < 0) {
          ldout(cct, 5) << "journal full -- detaining block IO" << dendl;
          append_detain_block(block_io);
          return;
        }
      }

      if (block_io.partial_block) {
        // block needs to be promoted to cache but we require a
        // read-modify-write cycle to fully populate the block

        // TODO optimize by only reading missing extents
        promote_buffers.emplace_back();

        if (block_io.tid > 0 && (1 == policy.get_write_mode())) {
          req = new C_AppendEventToJournal<I>(cct, journal_store, block_io.tid,
                                              block_io.block, cache_block,//modified by dingl 
                                              IO_TYPE_WRITE, req);
        }
        /*req = new C_PromoteToCache<I>(cct, image_store, block_io,
                                      promote_buffers.back(), req);*/
        //modified by dingl
        req = new C_PromoteToCache<I>(cct, image_store, block_io, cache_block, 
                                      promote_buffers.back(), req);                              
        req = new C_ModifyBlockBuffer(cct, block_io, bl,
                                      &promote_buffers.back(), req);
        if (policy_map_result == POLICY_MAP_RESULT_HIT) {
          if (block_io.tid > 0 &&
              journal_store.is_demote_required(block_io.block)) {
            req = new C_DemoteBlockToJournal<I>(cct, journal_store,
                                                block_io.block,
                                                promote_buffers.back(), req);
          }
          /*req = new C_ReadBlockFromCacheRequest<I>(cct, image_store,
                                                   block_io.block, BLOCK_SIZE,
                                                   &promote_buffers.back(),
                                                   req);*/
          //read from cache block,modify by dingl
          req = new C_ReadBlockFromCacheRequest<I>(cct, image_store,
                                                   cache_block, BLOCK_SIZE,
                                                   &promote_buffers.back(),
                                                   req);
        } else {
          req = new C_ReadBlockFromImageRequest<I>(cct, image_writeback,
                                                   block_io.block,
                                                   &promote_buffers.back(),
                                                   req);
        }
      } else {
        // full block overwrite
        if (block_io.tid > 0 && (1 == policy.get_write_mode())) {
          req = new C_AppendEventToJournal<I>(cct, journal_store, block_io.tid,
                                              block_io.block, cache_block,//modified by dingl 
                                              IO_TYPE_WRITE, req);
        }

        req = new C_PromoteToCache<I>(cct, image_store, block_io, cache_block, 
                                      bl, req);
      }

      if (policy_map_result == POLICY_MAP_RESULT_REPLACE) {
		ldout(cct, 5) << "C_DemoteFromCache, Replace" << dendl;
        //req = new C_DemoteFromCache<I>(cct, image_store, release_block,
         //                              block_io.block, req);
         //Just release the detained block in policy when replace, add by dingl
         req = new C_DemoteFromCache<I>(cct, image_store, release_block,
                                       replace_image_block, req);
      }
    }
    req->send();
  }
};

template <typename I>
struct C_WritebackRequest : public Context {
  I &image_ctx;
  ImageWriteback<I> &image_writeback;
  Policy &policy;
  JournalStore<I> &journal_store;
  ImageStore<I> &image_store;
  const ReleaseBlock &release_block;
  util::AsyncOpTracker &async_op_tracker;
  uint64_t tid;
  uint64_t image_block_id;//modified by dingl
  uint64_t cache_block_id;
  IOType io_type;
  bool demoted;
  uint32_t block_size;

  bufferlist bl;

  C_WritebackRequest(I &image_ctx, ImageWriteback<I> &image_writeback,
                     Policy &policy, JournalStore<I> &journal_store,
                     ImageStore<I> &image_store,
                     const ReleaseBlock &release_block,
                     util::AsyncOpTracker &async_op_tracker, uint64_t tid,
                     uint64_t image_block, uint64_t cache_block, 
                     IOType io_type, bool demoted, uint32_t block_size)
    : image_ctx(image_ctx), image_writeback(image_writeback),
      policy(policy), journal_store(journal_store), image_store(image_store),
      release_block(release_block), async_op_tracker(async_op_tracker),
      tid(tid), image_block_id(image_block), cache_block_id(cache_block), 
      io_type(io_type), demoted(demoted), block_size(block_size) {
    async_op_tracker.start_op();
  }
  virtual ~C_WritebackRequest() {
    async_op_tracker.finish_op();
  }

  void send() {
    read_from_cache();
  }

  void read_from_cache() {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << "(C_WritebackRequest)" << dendl;

    Context *ctx = util::create_context_callback<
      C_WritebackRequest<I>,
      &C_WritebackRequest<I>::handle_read_from_cache>(this);
    if (demoted) {
      journal_store.get_writeback_block(tid, &bl, ctx);
    } else {
      //image_store.read_block(block, {{0, block_size}}, &bl, ctx);
      //modified by dingl
      image_store.read_block(cache_block_id, {{0, block_size}}, &bl, ctx);
    }
  }

  void handle_read_from_cache(int r) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << "(C_WritebackRequest): r=" << r << dendl;

    if (r < 0) {
      lderr(cct) << "failed to read writeback block from cache: "
                 << cpp_strerror(r) << dendl;
      complete(r);
      return;
    }

    write_to_image();
  }

  void write_to_image() {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << "(C_WritebackRequest)" << dendl;

    Context *ctx = util::create_context_callback<
      C_WritebackRequest<I>,
      &C_WritebackRequest<I>::handle_write_to_image>(this);
    // image_writeback.aio_write({{block * block_size, block_size}}, std::move(bl),
    //                          0, ctx);
    //modified by dingl
    image_writeback.aio_write({{image_block_id * block_size, block_size}}, std::move(bl),
                              0, ctx);
  }

  void handle_write_to_image(int r) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << "(C_WritebackRequest): r=" << r << dendl;

    if (r < 0) {
      lderr(cct) << "failed to read writeback block from cache: "
                 << cpp_strerror(r) << dendl;
      complete(r);
      return;
    }

    commit_event();
  }

  void commit_event() {
    if (tid == 0) {
      complete(0);
      return;
    }

    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << "(C_WritebackRequest)" << dendl;

    if (!demoted) {
      //policy.clear_dirty(block);
      //modified by dingl
      policy.clear_dirty(image_block_id);
    }

    Context *ctx = util::create_context_callback<
      C_WritebackRequest<I>,
      &C_WritebackRequest<I>::handle_commit_event>(this);
    journal_store.commit_event(tid, ctx);
  }

  void handle_commit_event(int r) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << "(C_WritebackRequest): r=" << r << dendl;

    if (r < 0) {
      lderr(cct) << "failed to commit event in cache: "
                 << cpp_strerror(r) << dendl;
      complete(r);
      return;
    }

    complete(0);
  }

  virtual void finish(int r) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << "r=" << r << dendl;

    if (r < 0) {
      lderr(cct) << "failed to writeback block " << image_block_id << ": "
                 << cpp_strerror(r) << dendl;
      //policy.set_dirty(block);
      //modified by dingl
      policy.set_dirty(image_block_id);
    }

    //release_block(block);
    //modified by dingl
    release_block(image_block_id);
  }
};

//commit event reqeust,add by dingl
template <typename I>
struct C_CommitEventRequest : public Context {
  I &image_ctx;
  ImageWriteback<I> &image_writeback;
  //Policy &policy;
  JournalStore<I> &journal_store;
  ImageStore<I> &image_store;
  //const ReleaseBlock &release_block;
  util::AsyncOpTracker &async_op_tracker;
  //uint64_t tid;
  uint64_t image_block_id;
  uint64_t cache_block_id;
  uint64_t journal_event_idx;
  IOType io_type;
  bool demoted;
  uint32_t block_size;

  bufferlist bl;

  C_CommitEventRequest(I &image_ctx, ImageWriteback<I> &image_writeback,
                     JournalStore<I> &journal_store,ImageStore<I> &image_store,
                     util::AsyncOpTracker &async_op_tracker, uint64_t image_block,
                     uint64_t cache_block, uint64_t journal_event,
                     IOType io_type, bool demoted, uint32_t block_size)
    : image_ctx(image_ctx), image_writeback(image_writeback),
      journal_store(journal_store), image_store(image_store),
      async_op_tracker(async_op_tracker), image_block_id(image_block),
      cache_block_id(cache_block), journal_event_idx(journal_event),
      io_type(io_type), demoted(demoted), block_size(block_size) {
    async_op_tracker.start_op();
  }
  
  virtual ~C_CommitEventRequest() {
    async_op_tracker.finish_op();
  }
  
  void send() {
    read_from_cache();
  }

  void read_from_cache() {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 6) << "(C_CommitEventRequest)" << dendl;

    Context *ctx = util::create_context_callback<
      C_CommitEventRequest<I>,
      &C_CommitEventRequest<I>::handle_read_from_cache>(this);
    if (demoted) {
      journal_store.get_journal_block(journal_event_idx, &bl, ctx);
    } else {
      
	  ldout(cct, 6) << "debug,before read" << dendl;
      image_store.read_block(cache_block_id, {{0, block_size}}, &bl, ctx);
	  
	  ldout(cct, 6) << "debug,after read" << dendl;
    }
  }
  
  void handle_read_from_cache(int r) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 6) << "(C_CommitEventRequest): r=" << r << dendl;

    if (r < 0) {
      lderr(cct) << "failed to read cache block: "<< cpp_strerror(r) << dendl;
      complete(r);
      return;
    }

    write_to_image();
  }
  
  void write_to_image() {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 6) << "(C_CommitEventRequest)" << dendl;

    Context *ctx = util::create_context_callback<
      C_CommitEventRequest<I>,
      &C_CommitEventRequest<I>::handle_write_to_image>(this);
    image_writeback.aio_write({{image_block_id * block_size, block_size}},
		                      			std::move(bl), 0, ctx);
  }

  void handle_write_to_image(int r) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 6) << "(C_CommitEventRequest): r=" << r << dendl;

    if (r < 0) {
      lderr(cct) << "failed to write block to image: " << cpp_strerror(r) << dendl;
      complete(r);
      return;
    }

    commit_event();
  }

  void commit_event() {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 6) << "(C_CommitEventRequest)" << dendl;

    Context *ctx = util::create_context_callback<
      C_CommitEventRequest<I>,
      &C_CommitEventRequest<I>::handle_commit_event>(this);
    journal_store.commit_event(journal_event_idx, io_type, demoted, ctx);
  }

  void handle_commit_event(int r) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 6) << "(C_CommitEventRequest): r=" << r << dendl;

    if (r < 0) {
      lderr(cct) << "failed to commit event" << cpp_strerror(r) << dendl;
      complete(r);
      return;
    }

    complete(0);
  }

  virtual void finish(int r) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 6) << "r=" << r << dendl;

    if (r < 0) {
      lderr(cct) << "failed to writeback block " << image_block_id << ": "
                 << cpp_strerror(r) << dendl;
    }
  }
};


} // anonymous namespace

template <typename I>
FileImageCache<I>::FileImageCache(ImageCtx &image_ctx)
  : m_image_ctx(image_ctx), m_image_writeback(image_ctx),
    m_block_guard(image_ctx.cct, 256, BLOCK_SIZE),
    //m_block_guard(image_ctx.cct, 4096, BLOCK_SIZE),
    m_policy(new StupidPolicy<I>(m_image_ctx, m_block_guard)),
    m_release_block(std::bind(&FileImageCache<I>::release_block, this,
                              std::placeholders::_1)),
    m_detain_block(std::bind(&FileImageCache<I>::append_detain_block, this, std::placeholders::_1)),
    m_lock("librbd::cache::FileImageCache::m_lock"), cct(m_image_ctx.cct) {
  CephContext *cct = m_image_ctx.cct;
  uint8_t write_mode = cct->_conf->rbd_persistent_cache_writeback?1:0;
  m_policy->set_write_mode(write_mode);
}

template <typename I>
FileImageCache<I>::~FileImageCache() {
  delete m_policy;
}

template <typename I>
void FileImageCache<I>::aio_read(Extents &&image_extents, bufferlist *bl,
                                 int fadvise_flags, Context *on_finish) {
  //CephContext *cct = m_image_ctx.cct;
  //ldout(cct, 20) << "image_extents=" << image_extents << ", "
  //               << "on_finish=" << on_finish << dendl;

  // TODO handle fadvise flags
  BlockGuard::C_BlockRequest *req = new C_ReadBlockRequest<I>(
    m_image_ctx, m_image_writeback, *m_image_store, m_release_block, bl,
    on_finish);
  map_blocks(IO_TYPE_READ, std::move(image_extents), req);
}

template <typename I>
void FileImageCache<I>::aio_write(Extents &&image_extents,
                                  bufferlist&& bl,
                                  int fadvise_flags,
                                  Context *on_finish) {
  //CephContext *cct = m_image_ctx.cct;
  //ldout(cct, 20) << "image_extents=" << image_extents << ", "
  //               << "on_finish=" << on_finish << dendl;

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  // TODO handle fadvise flags
  BlockGuard::C_BlockRequest *req = new C_WriteBlockRequest<I>(
    m_image_ctx, m_image_writeback, *m_policy, *m_journal_store, *m_image_store,
    *m_meta_store, m_release_block, m_detain_block, std::move(bl), BLOCK_SIZE, on_finish);
  map_blocks(IO_TYPE_WRITE, std::move(image_extents), req);
}

template <typename I>
void FileImageCache<I>::aio_discard(uint64_t offset, uint64_t length,
                                    bool skip_partial_discard, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "offset=" << offset << ", "
                 << "length=" << length << ", "
                 << "on_finish=" << on_finish << dendl;

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  if (!is_block_aligned({{offset, length}})) {
    // For clients that don't use LBA extents, re-align the discard request
    // to work with the cache
    ldout(cct, 20) << "aligning discard to block size" << dendl;

    // TODO: for non-aligned extents, invalidate the associated block-aligned
    // regions in the cache (if any), send the aligned extents to the cache
    // and the un-aligned extents directly to back to librbd
  }

  // TODO invalidate discard blocks until writethrough/back support added
  C_Gather *ctx = new C_Gather(cct, on_finish);
  Context *invalidate_done_ctx = ctx->new_sub();

  m_image_writeback.aio_discard(offset, length, skip_partial_discard, ctx->new_sub());

  ctx->activate();

  Context *invalidate_ctx = new FunctionContext(
    [this, offset, length, invalidate_done_ctx](int r) {
      invalidate({{offset, length}}, invalidate_done_ctx);
    });
  flush(invalidate_ctx);
}

template <typename I>
void FileImageCache<I>::aio_flush(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "on_finish=" << on_finish << dendl;

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  Context *ctx = new FunctionContext(
    [this, on_finish](int r) {
      if (r < 0) {
        on_finish->complete(r);
      }
      m_image_writeback.aio_flush(on_finish);
    });

  flush(ctx);

}

template <typename I>
void FileImageCache<I>::aio_writesame(uint64_t offset, uint64_t length,
                                      bufferlist&& bl, int fadvise_flags,
                                      Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "offset=" << offset << ", "
                 << "length=" << length << ", "
                 << "data_len=" << bl.length() << ", "
                 << "on_finish=" << on_finish << dendl;
  {

    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  bufferlist total_bl;

  uint64_t left = length;
  while(left) {
    total_bl.append(bl);
    left -= bl.length();
  }
  assert(length == total_bl.length());
  aio_write({{offset, length}}, std::move(total_bl), fadvise_flags, on_finish);
}

template <typename I>
void FileImageCache<I>::init(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  //bufferlist meta_bl;
  //this would cause stack overflow, so we use operator new, add by dingl
  bufferlist *meta_bl = new bufferlist();
  bufferlist *journal_bl = new bufferlist();
  // chain the initialization of the meta, image, and journal stores
  Context *ctx = new FunctionContext(
    [this, cct, on_finish](int r) {
      if (r >= 0) {
        // TODO need to support dynamic image resizes
        m_policy->set_block_count(
          m_meta_store->offset_to_block(m_image_ctx.size));
      }
	  ldout(cct, 6) << "replay journal done" << dendl;
      on_finish->complete(r);
    });

	//replay journal, add by dingl
	ctx = new FunctionContext([this, cct, journal_bl, ctx](int r) {
		if (r < 0) {
	  		ctx->complete(r);
	  		return;
		}
		if (journal_bl->length() > 0) {
			ldout(cct, 6) << "replay journal" << dendl;
			replay_journal(journal_bl, ctx);
		} else {
			ldout(cct, 6) << "no journal to replay" << dendl;
			ctx->complete(0);
		}
		
		delete journal_bl;
  	});
  
  ctx = new FunctionContext(
    [this, journal_bl, ctx](int r) {
      if (r < 0) {
        ctx->complete(r);
        return;
      }

      // TODO: do not enable journal store if writeback disabled
      m_journal_store = new JournalStore<I>(m_image_ctx, m_block_guard,
                                            *m_meta_store);
      //m_journal_store->init(ctx);
      //add by dingl
      m_journal_store->init(journal_bl, ctx);
    });
  ctx = new FunctionContext(
    [this, meta_bl, ctx](int r) mutable {
      if (r < 0) {
        ctx->complete(r);
        return;
      }
      //load meta_bl to policy entry
      m_policy->bufferlist_to_entry(meta_bl);
      m_image_store = new ImageStore<I>(m_image_ctx, *m_meta_store);
      m_image_store->init(ctx);

	  //we don't need meta_bl any more,add by dingl 
	  delete meta_bl;
    });
  m_meta_store = new MetaStore<I>(m_image_ctx, BLOCK_SIZE);
  m_meta_store->set_entry_size(m_policy->get_entry_size());
  //m_meta_store->init(&meta_bl, ctx);
  //modified by dingl
  m_meta_store->init(meta_bl, ctx);
}

template <typename I>
void FileImageCache<I>::shut_down(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO flush all in-flight IO and pending writeback prior to shut down

  // chain the shut down of the journal, image, and meta stores
  Context *ctx = new FunctionContext(
    [this, on_finish](int r) {
      delete m_journal_store;
      delete m_image_store;
      delete m_meta_store;
      on_finish->complete(r);
    });
  ctx = new FunctionContext(
    [this, ctx](int r) {
      Context *next_ctx = ctx;
      if (r < 0) {
        next_ctx = new FunctionContext(
          [r, ctx](int _r) {
            ctx->complete(r);
          });
      }
      m_meta_store->shut_down(next_ctx);
    });
  ctx = new FunctionContext(
    [this, ctx](int r) {
      Context *next_ctx = ctx;
      if (r < 0) {
        next_ctx = new FunctionContext(
          [r, ctx](int _r) {
            ctx->complete(r);
          });
      }
      m_image_store->shut_down(next_ctx);
    });
  ctx = new FunctionContext(
    [this, ctx](int r) {
      m_journal_store->shut_down(ctx);
    });
  ctx = new FunctionContext(
    [this, ctx](int r) {
      // flush writeback journal to OSDs
      flush(ctx);
    });

  {
    Mutex::Locker locker(m_lock);
    m_async_op_tracker.wait(m_image_ctx, ctx);
  }
}

template <typename I>
void FileImageCache<I>::invalidate(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO
  invalidate({{0, m_image_ctx.size}}, on_finish);
}

template <typename I>
void FileImageCache<I>::map_blocks(IOType io_type, Extents &&image_extents,
                                   BlockGuard::C_BlockRequest *block_request) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  BlockGuard::BlockIOs block_ios;
  m_block_guard.create_block_ios(io_type, image_extents, &block_ios,
                                 block_request);

  // map block IO requests to the cache or backing image based upon policy
  for (auto &block_io : block_ios) {
    map_block(true, std::move(block_io));
  }

  // advance the policy statistics
  m_policy->tick();
  block_request->activate();
}

template <typename I>
void FileImageCache<I>::map_block(bool detain_block,
                                  BlockGuard::BlockIO &&block_io) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "block_io=[" << block_io << "]" << dendl;

  int r;
  if (detain_block) {
	//add by dingl
    ldout(cct, 5) << "detain block:" << block_io.block <<", deferred block size:"
      << m_deferred_block_ios.size() << ",detained block size:" 
      << m_block_guard.get_detained_block_size() << ",free detained block size:"
      << m_block_guard.get_free_detained_block_size() << dendl;
    r = m_block_guard.detain(block_io.block, &block_io);
    if (r < 0) {
      Mutex::Locker locker(m_lock);
      ldout(cct, 5) << "block guard full -- deferring block IO" << dendl;
      m_deferred_block_ios.emplace_back(std::move(block_io));
      return;
    } else if (r > 0) {
      ldout(cct, 20) << "block already detained" << dendl;
      return;
    }
  }

  IOType io_type = static_cast<IOType>(block_io.io_type);
  PolicyMapResult policy_map_result;
  uint64_t cache_block;
  uint64_t replace_image_block;
  r = m_policy->map(io_type, block_io.block, block_io.partial_block,
                    &policy_map_result, &cache_block, &replace_image_block);
  if (r < 0) {
    // fail this IO and release any detained IOs to the block
    lderr(cct) << "failed to map block via cache policy: " << cpp_strerror(r)
               << dendl;
    block_io.block_request->fail(r);
    release_block(block_io.block);
    return;
  }

  block_io.block_request->remap(policy_map_result, std::move(block_io),
  								cache_block, replace_image_block);//modified by dingl
}

template <typename I>
void FileImageCache<I>::release_block(uint64_t block) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "block=" << block << dendl;

  //add by dingl
  //ldout(cct, 5) << "before release, deferred block size:" << m_deferred_block_ios.size()
    //   << ",detained block size:" << m_block_guard.get_detained_block_size()
   //    << ",free detained block size:" << m_block_guard.get_free_detained_block_size() << dendl;

  Mutex::Locker locker(m_lock);
  m_block_guard.release(block, &m_detained_block_ios);
  //ldout(cct, 5) << "after release, deferred block size:" << m_deferred_block_ios.size()
  //     << ",detained block size:" << m_block_guard.get_detained_block_size()
  //     << ",free detained block size:" << m_block_guard.get_free_detained_block_size() << dendl;
  wake_up();
}

template <typename I>
void FileImageCache<I>::append_detain_block(BlockGuard::BlockIO &block_io) {
  CephContext *cct = m_image_ctx.cct;
  //ldout(cct, 20) << "block=" << block_io.block << dendl;
  ldout(cct, -1) << "block=" << block_io.block << dendl;

  Mutex::Locker locker(m_lock);
  m_detained_block_ios.emplace_back(std::move(block_io));
}

template <typename I>
void FileImageCache<I>::wake_up() {
  assert(m_lock.is_locked());
  if (m_wake_up_scheduled || m_async_op_tracker.is_waiting()) {
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  m_wake_up_scheduled = true;
  m_async_op_tracker.start_op();
  m_image_ctx.op_work_queue->queue(new FunctionContext(
    [this](int r) {
      process_work();
    }), 0);
}

template <typename I>
void FileImageCache<I>::process_work() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  do {
    process_writeback_dirty_blocks();
    process_detained_block_ios();
    process_deferred_block_ios();

    // TODO
    Contexts post_work_contexts;
    {
      Mutex::Locker locker(m_lock);
      post_work_contexts.swap(m_post_work_contexts);
    }
    if (!post_work_contexts.empty()) {
      for (auto ctx : post_work_contexts) {
        //TODO: fix flush post work
        //ctx->complete(0);
      }
      continue;
    }
  } while (false); // TODO need metric to only perform X amount of work per cycle

  // process delayed shut down request (if any)
  {
    Mutex::Locker locker(m_lock);
    m_wake_up_scheduled = false;
    m_async_op_tracker.finish_op();
  }
}

template <typename I>
bool FileImageCache<I>::is_work_available() const {
  Mutex::Locker locker(m_lock);
  return (!m_detained_block_ios.empty() ||
          !m_deferred_block_ios.empty());
}

template <typename I>
void FileImageCache<I>::process_writeback_dirty_blocks() {
  CephContext *cct = m_image_ctx.cct;

  // TODO throttle the amount of in-flight writebacks
  while (true) {
    uint64_t tid = 0;
    uint64_t image_block;//modified by dingl
	uint64_t cache_block;
    IOType io_type;
    bool demoted;
    int r = m_journal_store->get_writeback_event(&tid, &image_block,
											&cache_block, &io_type, &demoted);
    //int r = m_policy->get_writeback_block(&block);
    if (r == -ENODATA || r == -EBUSY) {
      // nothing to writeback
      return;
    } else if (r < 0) {
      lderr(cct) << "failed to retrieve writeback block: "
                 << cpp_strerror(r) << dendl;
      return;
    }
	 ldout(cct, 5) << "image_block=" << image_block << ", cache_block=" 
	 		<< cache_block << dendl;
    // block is now detained -- safe for writeback
    C_WritebackRequest<I> *req = new C_WritebackRequest<I>(
      m_image_ctx, m_image_writeback, *m_policy, *m_journal_store,
      *m_image_store, m_release_block, m_async_op_tracker, tid, image_block, 
      cache_block, io_type, demoted, BLOCK_SIZE);
    req->send();
  }
}

template <typename I>
void FileImageCache<I>::process_detained_block_ios() {
  BlockGuard::BlockIOs block_ios;
  {
    Mutex::Locker locker(m_lock);
    std::swap(block_ios, m_detained_block_ios);
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "block_ios=" << block_ios.size() << dendl;
  for (auto &block_io : block_ios) {
    map_block(false, std::move(block_io));
  }
}

template <typename I>
void FileImageCache<I>::process_deferred_block_ios() {
  BlockGuard::BlockIOs block_ios;
  {
    Mutex::Locker locker(m_lock);
    std::swap(block_ios, m_deferred_block_ios);
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "block_ios=" << block_ios.size() << dendl;
  for (auto &block_io : block_ios) {
    map_block(true, std::move(block_io));
  }
}

template <typename I>
void FileImageCache<I>::invalidate(Extents&& image_extents,
                                   Context *on_finish) {
//  CephContext *cct = m_image_ctx.cct;
  //ldout(cct, 20) << "image_extents=" << image_extents << dendl;

  // TODO
  for (auto &extent : image_extents) {
    uint64_t image_offset = extent.first;
    uint64_t image_length = extent.second;
    while (image_length > 0) {
      uint64_t block = m_meta_store->offset_to_block(image_offset);
      uint32_t block_start_offset = image_offset % BLOCK_SIZE;
      uint32_t block_end_offset = MIN(block_start_offset + image_length,
                                      BLOCK_SIZE);
      uint32_t block_length = block_end_offset - block_start_offset;

      m_policy->invalidate(block);

      image_offset += block_length;
      image_length -= block_length;
    }
  }

  // dump specific extents within the cache
  on_finish->complete(0);
}

template <typename I>
void FileImageCache<I>::flush(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO -- need deferred set for tracking op ordering
  if (m_journal_store->is_writeback_pending()) {
    Mutex::Locker locker(m_lock);
    m_post_work_contexts.push_back(new FunctionContext(
      [this, on_finish](int r) {
        flush(on_finish);
      }));
  }

  on_finish->complete(0);

  // TODO
  // internal flush -- nothing to writeback but make sure
  // in-flight IO is flushed
  //aio_flush(on_finish);
}

//replay journal,add by dingl
template <typename I>
void FileImageCache<I>::replay_journal(bufferlist *bl, Context *on_finish) {
	ldout(cct, 6) << dendl;
	uint32_t encoded_event_size = m_journal_store->get_encoded_event_size();
	//util::AsyncOpTracker t_async_op_tracker;

	for (bufferlist::iterator it = bl->begin(); it != bl->end(); ) {
		journal_store::Event e;
		int ret;

		e.decode(it);
		//need to advance iterator to adjust the offset
		it.advance(journal_store::Event::ENCODED_SIZE - encoded_event_size);
		ret = m_journal_store->check_event(e);
		if (ret != 0) {
			lderr(cct) << "bad event, skip this" << dendl;
			continue;
		}
		uint64_t tid = e.tid;
		uint64_t image_block = e.image_block;
		uint64_t cache_block = e.cache_block;
		uint64_t journal_event = e.journal_event_idx;
		IOType io_type = e.fields.io_type;
		bool demoted = e.fields.demoted;
		ldout(cct, 6) << "commit event tid " << tid << " image_block " <<
			image_block << " cache_block " << cache_block <<" journal_event_idx " <<
			journal_event << " demoted " << demoted <<dendl;

		C_CommitEventRequest<I> *req = new C_CommitEventRequest<I>(
      		m_image_ctx, m_image_writeback, *m_journal_store, *m_image_store, 
      		m_async_op_tracker, image_block, cache_block, journal_event, 
      		io_type, demoted, BLOCK_SIZE);
		req->send();
	}
	//wait all journal event to finish
	m_async_op_tracker.wait(m_image_ctx, on_finish);
	//C_SaferCond ctx;
	//t_async_op_tracker.wait_for_ops(&ctx);
	//ctx.wait();
	//on_finish->complete(0);
}


} // namespace cache
} // namespace librbd

template class librbd::cache::FileImageCache<librbd::ImageCtx>;
