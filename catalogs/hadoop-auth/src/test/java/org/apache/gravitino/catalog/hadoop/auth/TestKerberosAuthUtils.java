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

package org.apache.gravitino.catalog.hadoop.auth;

import java.io.File;
import java.nio.file.Files;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestKerberosAuthUtils {

  @TempDir private File tempDir;

  @Test
  public void testCheckPrincipalAndGetRealm() {
    Assertions.assertEquals(
        "EXAMPLE.COM", KerberosAuthUtils.checkPrincipalAndGetRealm("service/host@EXAMPLE.COM"));

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> KerberosAuthUtils.checkPrincipalAndGetRealm(""));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> KerberosAuthUtils.checkPrincipalAndGetRealm("service/host"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> KerberosAuthUtils.checkPrincipalAndGetRealm("service/host@REALM@EXTRA"));
  }

  @Test
  public void testFetchKeytabFromLocalUri() throws Exception {
    File source = new File(tempDir, "source.keytab");
    File destination = new File(tempDir, "destination.keytab");
    Files.writeString(source.toPath(), "keytab-content");

    File fetched =
        KerberosAuthUtils.fetchKeytabFromUri(
            source.toURI().toString(),
            destination,
            1,
            false /* allowHdfsKeytabUri */,
            null /* hadoopConf */);

    Assertions.assertEquals(destination.getAbsolutePath(), fetched.getAbsolutePath());
    Assertions.assertTrue(Files.isSymbolicLink(destination.toPath()));
    Assertions.assertEquals(source.toPath(), Files.readSymbolicLink(destination.toPath()));
  }

  @Test
  public void testFetchKeytabRejectsHdfsWhenDisabled() {
    File destination = new File(tempDir, "destination.keytab");

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                KerberosAuthUtils.fetchKeytabFromUri(
                    "hdfs://namenode/keytab",
                    destination,
                    1,
                    false /* allowHdfsKeytabUri */,
                    null /* hadoopConf */));

    Assertions.assertTrue(exception.getMessage().contains("HDFS"));
  }
}
