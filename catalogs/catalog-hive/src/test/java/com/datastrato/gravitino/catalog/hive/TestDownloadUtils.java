/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.hive;

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDownloadUtils {

  @Test
  public void testDownloadFile() throws Exception {

    File srcFile = new File("test");
    File destFile = new File("dest");

    srcFile.createNewFile();
    DownloadUtils.downloadFile(srcFile.toURI().toString(), destFile, 10, new Configuration());
    Assertions.assertTrue(destFile.exists());

    srcFile.delete();
    destFile.delete();
  }
}
