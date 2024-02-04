/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.hive;

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFetchFileUtils {

  @Test
  public void testLinkLocalFile() throws Exception {

    File srcFile = new File("test");
    File destFile = new File("dest");

    srcFile.createNewFile();
    FetchFileUtils.fetchFileFromUri(srcFile.toURI().toString(), destFile, 10, new Configuration());
    Assertions.assertTrue(destFile.exists());

    srcFile.delete();
    destFile.delete();
  }

  @Test
  public void testDownloadFromHTTP() throws Exception {
    File destFile = new File("dest");
    FetchFileUtils.fetchFileFromUri(
        "https://downloads.apache.org/hadoop/common/KEYS", destFile, 10, new Configuration());
    Assertions.assertTrue(destFile.exists());
    destFile.delete();
  }
}
