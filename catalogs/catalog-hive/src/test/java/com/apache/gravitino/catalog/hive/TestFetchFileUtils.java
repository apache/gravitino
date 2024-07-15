/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.apache.gravitino.catalog.hive;

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
