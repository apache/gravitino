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

package org.apache.gravitino.iceberg.common.io;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoCredentialFileIO {

  @Test
  void testRefreshesCredentialsBeforePathOperations() throws Exception {
    String supplierId =
        GravitinoCredentialFileIORegistry.register(
            path -> {
              int tokenId = RecordingFileIO.refreshCount.incrementAndGet();
              return Collections.singletonList(
                  StorageCredential.create(
                      "gs://bucket/warehouse",
                      ImmutableMap.of("gcs.oauth2.token", "token-" + tokenId)));
            });

    try {
      GravitinoCredentialFileIO fileIO = new GravitinoCredentialFileIO();
      fileIO.initialize(
          ImmutableMap.of(
              GravitinoCredentialFileIO.DELEGATE_IO_IMPL,
              RecordingFileIO.class.getName(),
              GravitinoCredentialFileIO.CREDENTIAL_SUPPLIER_ID,
              supplierId));

      fileIO.newInputFile("gs://bucket/warehouse/table/metadata/v1.metadata.json");
      fileIO.newOutputFile("gs://bucket/warehouse/table/metadata/v2.metadata.json");

      Assertions.assertEquals(2, RecordingFileIO.refreshCount.get());
      Assertions.assertEquals(
          "token-2", RecordingFileIO.lastCredentials.get(0).config().get("gcs.oauth2.token"));
    } finally {
      GravitinoCredentialFileIORegistry.unregister(supplierId);
      RecordingFileIO.refreshCount.set(0);
      RecordingFileIO.lastCredentials = Collections.emptyList();
    }
  }

  @Test
  void testWrapsOriginalIoImpl() {
    Map<String, String> properties =
        GravitinoCredentialFileIO.wrapProperties(
            ImmutableMap.of(IcebergConstants.IO_IMPL, RecordingFileIO.class.getName()), "id");

    Assertions.assertEquals(
        GravitinoCredentialFileIO.class.getName(), properties.get(IcebergConstants.IO_IMPL));
    Assertions.assertEquals(
        RecordingFileIO.class.getName(),
        properties.get(GravitinoCredentialFileIO.DELEGATE_IO_IMPL));
    Assertions.assertEquals("id", properties.get(GravitinoCredentialFileIO.CREDENTIAL_SUPPLIER_ID));
  }

  public static class RecordingFileIO implements FileIO, SupportsStorageCredentials {
    static AtomicInteger refreshCount = new AtomicInteger();
    static List<StorageCredential> lastCredentials = Collections.emptyList();

    @Override
    public InputFile newInputFile(String path) {
      return null;
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return null;
    }

    @Override
    public void deleteFile(String path) {}

    @Override
    public void setCredentials(List<StorageCredential> credentials) {
      lastCredentials = credentials;
    }

    @Override
    public List<StorageCredential> credentials() {
      return lastCredentials;
    }
  }
}
