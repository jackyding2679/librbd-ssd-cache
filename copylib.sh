#!/bin/sh

scp /home/dingl/ceph-wip-librbd-cache/build/lib/librbd.so.1.12.0 root@10.3.193.212:/lib64/librbd.so.1.12.0.ssdcache
scp /home/dingl/ceph-wip-librbd-cache/build/lib/librados.so.2.0.0 root@10.3.193.212:/lib64/librados.so.2.0.0.ssdcache
scp /home/dingl/ceph-wip-librbd-cache/build/lib/libceph-common.so* root@10.3.193.212:/lib64/
