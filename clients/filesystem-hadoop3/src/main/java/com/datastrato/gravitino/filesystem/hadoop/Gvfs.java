/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

/**
 * A {@link DelegateToFileSystem} implementation that delegates all operations to a {@link
 * GravitinoVirtualFileSystem}.
 */
public class Gvfs extends DelegateToFileSystem {

  /**
   * Creates a new instance of {@link Gvfs}.
   *
   * @param uri the URI of the file system
   * @param conf the configuration
   * @throws IOException if an I/O error occurs
   * @throws URISyntaxException if the URI is invalid
   */
  protected Gvfs(URI uri, Configuration conf) throws IOException, URISyntaxException {
    super(
        uri,
        new GravitinoVirtualFileSystem(),
        conf,
        GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME,
        false);
  }
}
