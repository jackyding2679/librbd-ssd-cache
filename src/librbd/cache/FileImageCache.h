// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_FILE_IMAGE_CACHE
#define CEPH_LIBRBD_CACHE_FILE_IMAGE_CACHE

#include "librbd/cache/ImageCache.h"
#include "librbd/Utils.h"
#include "librbd/cache/BlockGuard.h"
#include "librbd/cache/ImageWriteback.h"
#include "librbd/cache/file/Policy.h"
#include <functional>
#include <list>

namespace librbd {

struct ImageCtx;

namespace cache {

namespace file {

template <typename> class ImageStore;
template <typename> class JournalStore;
template <typename> class MetaStore;

} // namespace file

/**
 * Prototype file-based, client-side, image extent cache
 */
template <typename ImageCtxT = librbd::ImageCtx>
class FileImageCache : public ImageCache {
public:
  FileImageCache(ImageCtx &image_ctx);
  ~FileImageCache();

  /// client AIO methods
  void aio_read(Extents&& image_extents, ceph::bufferlist *bl,
                int fadvise_flags, Context *on_finish) override;
  void aio_write(Extents&& image_extents, ceph::bufferlist&& bl,
                 int fadvise_flags, Context *on_finish) override;
  void aio_discard(uint64_t offset, uint64_t length,
                   bool skip_partial_discard, Context *on_finish);
  void aio_flush(Context *on_finish) override;
  void aio_writesame(uint64_t offset, uint64_t length,
                     ceph::bufferlist&& bl,
                     int fadvise_flags, Context *on_finish) override;

  /// internal state methods
  void init(Context *on_finish) override;
  void shut_down(Context *on_finish) override;

  void invalidate(Context *on_finish) override;
  void flush(Context *on_finish) override;
  //replay journal, add by dingl
  void replay_journal(bufferlist *bl, Context *on_finish);

private:
  typedef std::function<void(uint64_t)> ReleaseBlock;
  typedef std::function<void(BlockGuard::BlockIO &)> AppendDetainedBlock;
  typedef std::list<Context *> Contexts;

  ImageCtxT &m_image_ctx;
  ImageWriteback<ImageCtxT> m_image_writeback;
  BlockGuard m_block_guard;

  file::Policy *m_policy = nullptr;
  file::MetaStore<ImageCtx> *m_meta_store = nullptr;
  file::JournalStore<ImageCtx> *m_journal_store = nullptr;
  file::ImageStore<ImageCtx> *m_image_store = nullptr;

  ReleaseBlock m_release_block;
  AppendDetainedBlock m_detain_block;

  util::AsyncOpTracker m_async_op_tracker;

  mutable Mutex m_lock;
  BlockGuard::BlockIOs m_deferred_block_ios;
  BlockGuard::BlockIOs m_detained_block_ios;

  bool m_wake_up_scheduled = false;

  Contexts m_post_work_contexts;

  void map_blocks(IOType io_type, Extents &&image_extents,
                  BlockGuard::C_BlockRequest *block_request);
  void map_block(bool detain_block, BlockGuard::BlockIO &&block_io);
  void release_block(uint64_t block);
  void append_detain_block(BlockGuard::BlockIO &block_io);

  void wake_up();
  void process_work();

  bool is_work_available() const;
  void process_writeback_dirty_blocks();
  void process_detained_block_ios();
  void process_deferred_block_ios();

  void invalidate(Extents&& image_extents, Context *on_finish);

  CephContext *cct;

};

} // namespace cache
} // namespace librbd

extern template class librbd::cache::FileImageCache<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_FILE_IMAGE_CACHE
