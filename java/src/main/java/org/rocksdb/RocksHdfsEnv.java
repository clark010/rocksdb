// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * RocksDB HDFS environment.
 */
public class RocksHdfsEnv extends Env {

  public RocksHdfsEnv() {
    super();
    nativeHandle_ = createHdfsEnv();
  }

  @Override
  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }

  private static native long createHdfsEnv();
  private native void disposeInternal(long handle);
}
