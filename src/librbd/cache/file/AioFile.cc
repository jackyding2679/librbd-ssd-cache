// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/file/AioFile.h"
#include "include/Context.h"
#include "common/dout.h"
#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <aio.h>
#include <errno.h>
#include <fcntl.h>
#include <utility>
//add by dingl
#ifdef HAVE_LINUX_FALLOC_H
# pragma message("NOTE: HAVE_LINUX_FALLOC_H")
# include <linux/falloc.h>      /* for FALLOC_FL_* flags */
#endif


#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::file::AioFile: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace file {

namespace {

// helper for supporting lamba move captures
template <typename T, typename F>
struct CaptureImpl {
    T t;
    F f;

    CaptureImpl(T &&t, F &&f) : t(std::forward<T>(t)), f(std::forward<F>(f)) {
    }

    template <typename ...Ts> auto operator()(Ts&&...args )
      -> decltype(f(t, std::forward<Ts>(args)...)) {
        return f(t, std::forward<Ts>(args)...);
    }

    template <typename ...Ts> auto operator()(Ts&&...args) const
      -> decltype(f(t, std::forward<Ts>(args)...))
    {
      return f(t, std::forward<Ts>(args)...);
    }
};

template <typename T, typename F>
CaptureImpl<T, F> make_capture(T &&t, F &&f) {
    return CaptureImpl<T, F>(std::forward<T>(t), std::forward<F>(f) );
}

} // anonymous namespace

template <typename I>
AioFile<I>::AioFile(I &image_ctx, ContextWQ &work_queue,
                    const std::string &name)
  : m_image_ctx(image_ctx), m_work_queue(work_queue){
  CephContext *cct = m_image_ctx.cct;
  m_name = cct->_conf->rbd_persistent_cache_path + "/rbd_cache." + name;
}

template <typename I>
AioFile<I>::~AioFile() {
  // TODO force proper cleanup
  if (m_fd != -1) {
    ::close(m_fd);
  }
}

template <typename I>
void AioFile<I>::open(Context *on_finish) {
  m_work_queue.queue(new FunctionContext([this, on_finish](int r) {
      while (true) {
        m_fd = ::open(m_name.c_str(), O_CREAT | O_NOATIME | O_RDWR,
                      S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
        if (m_fd == -1) {
          r = -errno;
          if (r == -EINTR) {
            continue;
          }
          on_finish->complete(r);
          return;
        }
        break;
      }

      on_finish->complete(0);
  }));
}

template <typename I>
void AioFile<I>::close(Context *on_finish) {
  assert(m_fd >= 0);
  m_work_queue.queue(new FunctionContext([this, on_finish](int r) {
      while (true) {
        r = ::close(m_fd);
        if (r == -1) {
          r = -errno;
          if (r == -EINTR) {
            continue;
          }
          on_finish->complete(r);
          return;
        }
        break;
      }

      m_fd = -1;
      on_finish->complete(0);
  }));
}

template <typename I>
void AioFile<I>::read(uint64_t offset, uint64_t length, ceph::bufferlist *bl,
                      Context *on_finish) {
  m_work_queue.queue(new FunctionContext(
    [this, offset, length, bl, on_finish](int r) {
      on_finish->complete(read(offset, length, bl));
    }));
}

//sync read, add by dingl
template <typename I>
int AioFile<I>::read_sync(uint64_t offset, uint64_t length, ceph::bufferlist *bl)
{
  return read(offset, length, bl);
}


template <typename I>
void AioFile<I>::write(uint64_t offset, ceph::bufferlist &&bl,
                       bool fdatasync, Context *on_finish) {
  m_work_queue.queue(new FunctionContext(make_capture(
    std::move(bl),
    [this, offset, fdatasync, on_finish](bufferlist &bl, int r) {
      on_finish->complete(write(offset, bl, fdatasync));
    })));
}

template <typename I>
void AioFile<I>::discard(uint64_t offset, uint64_t length, bool fdatasync,
                         Context *on_finish) {
if (on_finish) {
  m_work_queue.queue(new FunctionContext(
    [this, offset, length, fdatasync, on_finish](int r) {
      on_finish->complete(discard(offset, length, fdatasync));
    }));
} else {
   m_work_queue.queue(new FunctionContext(
    [this, offset, length, fdatasync](int r) {
      discard(offset, length, fdatasync);
    }));
}

}

template <typename I>
void AioFile<I>::truncate(uint64_t length, bool fdatasync, Context *on_finish) {
  m_work_queue.queue(new FunctionContext(
    [this, length, fdatasync, on_finish](int r) {
      on_finish->complete(truncate(length, fdatasync));
    }));
}

template <typename I>
void AioFile<I>::fsync(Context *on_finish) {
  m_work_queue.queue(new FunctionContext([this, on_finish](int r) {
      r = ::fsync(m_fd);
      if (r == -1) {
        r = -errno;
        on_finish->complete(r);
        return;
      }
      on_finish->complete(0);
    }));
}

template <typename I>
void AioFile<I>::fdatasync(Context *on_finish) {
  m_work_queue.queue(new FunctionContext(
    [this, on_finish](int r) {
      on_finish->complete(fdatasync());
    }));
}

template <typename I>
int AioFile<I>::write(uint64_t offset, const ceph::bufferlist &bl,
                      bool sync) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "offset=" << offset << ", "
                 << "length=" << bl.length() << dendl;

  int r = bl.write_fd(m_fd, offset);
  if (r < 0) {
    return r;
  }

  if (sync) {
    r = fdatasync();
  }
  return r;
}

template <typename I>
int AioFile<I>::read(uint64_t offset, uint64_t length, ceph::bufferlist *bl) {

  CephContext *cct = m_image_ctx.cct;
  bufferptr bp = buffer::create(length);
  bl->push_back(bp);

  int r = 0;
  char *buffer = reinterpret_cast<char *>(bp.c_str());
  uint64_t count = 0;
  while (count < length) {
    ssize_t ret_val = pread64(m_fd, buffer, length - count, offset + count);
    if (ret_val == 0) {
      break;
    } else if (ret_val < 0) {
      r = -errno;
      if (r == -EINTR) {
        continue;
      }

      return r;
    }

    count += ret_val;
    buffer += ret_val;
  }
  return count;
}

template <typename I>
int AioFile<I>::discard(uint64_t offset, uint64_t length, bool sync) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "offset=" << offset << ", "
                 << "length=" << length << dendl;

  int r;
  while (true) {
#if !defined(DARWIN) && !defined(__FreeBSD__)
# pragma message("NOTE: !DARWIN && !__FreeBSD__")
# ifdef CEPH_HAVE_FALLOCATE
# pragma message("NOTE: CEPH_HAVE_FALLOCATE")
#  ifdef FALLOC_FL_KEEP_SIZE
# pragma message("NOTE: FALLOC_FL_KEEP_SIZE")
   ldout(cct, 5) << "test============, fallocate"<< dendl;
    r = fallocate(m_fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                  offset, length);
    if (r == -1) {
      r = -errno;
      if (r == -EINTR) {
        continue;
      }
      return r;
    }
    goto out;
#  endif
# endif
#endif
  {
    // fall back to writing zeros
   ldout(cct, 5) << "test============, lseek64"<< dendl;
    bufferlist bl;
    bl.append_zero(length);
    r = ::lseek64(m_fd, offset, SEEK_SET);
    if (r < 0) {
      lderr(cct) << "lseek64 error:"<< strerror(errno) << dendl;
      r = -errno;
      goto out;
    }
   ldout(cct, 6) << "test============, write_fd"<< dendl;
    r = bl.write_fd(m_fd);
  }

  }

  if (sync) {
    r = fdatasync();
  }
out:
  return r;
}

template <typename I>
int AioFile<I>::truncate(uint64_t length, bool sync) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "length=" << length << dendl;

  int r;
  while (true) {
    r = ftruncate(m_fd, length);
    if (r == -1) {
      r = -errno;
      if (r == -EINTR) {
        continue;
      }
      return r;
    }
    break;
  }

  if (sync) {
    r = fdatasync();
  }
  return r;
}

template <typename I>
int AioFile<I>::fdatasync() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  int r = ::fdatasync(m_fd);
  if (r == -1) {
    r = -errno;
    return r;
  }
  return 0;
}

template <typename I>
uint64_t AioFile<I>::filesize() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  struct stat file_st;
  memset(&file_st, 0, sizeof(file_st));
  fstat(m_fd, &file_st);
  return file_st.st_size;
}

} // namespace file
} // namespace cache
} // namespace librbd

template class librbd::cache::file::AioFile<librbd::ImageCtx>;
