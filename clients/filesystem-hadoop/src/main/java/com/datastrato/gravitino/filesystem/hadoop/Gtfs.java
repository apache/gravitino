package com.datastrato.gravitino.filesystem.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

public class Gtfs extends DelegateToFileSystem {
  protected Gtfs(URI uri, Configuration conf) throws IOException, URISyntaxException {
    super(
        uri,
        new GravitinoFileSystem(),
        conf,
        GravitinoFilesetFileSystemConfiguration.GTFS_SCHEME,
        false);
  }
}
