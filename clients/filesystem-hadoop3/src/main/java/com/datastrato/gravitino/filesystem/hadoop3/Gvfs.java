/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop3;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

public class Gvfs extends DelegateToFileSystem {
  protected Gvfs(URI uri, Configuration conf) throws IOException, URISyntaxException {
    super(
        uri,
        new GravitinoVirtualFileSystem(),
        conf,
        GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME,
        false);
  }
}
