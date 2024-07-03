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

package com.datastrato.gravitino.storage.kv;

import static com.datastrato.gravitino.Configs.DEFAULT_ENTITY_KV_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_KV_ROCKSDB_BACKEND_PATH;
import static com.datastrato.gravitino.Configs.ENTITY_KV_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_STORE;
import static com.datastrato.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static com.datastrato.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.EntitySerDeFactory;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.EntityStoreFactory;
import com.datastrato.gravitino.storage.StorageLayoutException;
import com.datastrato.gravitino.storage.StorageLayoutVersion;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@Disabled
class TestStorageVersion {

  @Test
  void testFromString() {
    StorageLayoutVersion version = StorageLayoutVersion.fromString("v1");
    Assertions.assertEquals(StorageLayoutVersion.V1, version);

    Assertions.assertThrowsExactly(
        StorageLayoutException.class, () -> StorageLayoutVersion.fromString("v200000.0"));
  }

  @Test
  void testStorageLayoutVersion() throws IOException {
    Config config = Mockito.mock(Config.class);
    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    File file = Files.createTempDirectory(baseDir.toPath(), "test").toFile();
    file.deleteOnExit();
    Mockito.when(config.get(ENTITY_STORE)).thenReturn("kv");
    Mockito.when(config.get(ENTITY_KV_STORE)).thenReturn(DEFAULT_ENTITY_KV_STORE);
    Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
    Mockito.when(config.get(ENTITY_KV_ROCKSDB_BACKEND_PATH)).thenReturn(file.getAbsolutePath());
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);
    Mockito.when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);

    // First time create entity store, the storage layout version should be DEFAULT_LAYOUT_VERSION
    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);
      Assertions.assertTrue(store instanceof KvEntityStore);
      store.setSerDe(EntitySerDeFactory.createEntitySerDe(config.get(Configs.ENTITY_SERDE)));
      KvEntityStore entityStore = (KvEntityStore) store;
      Assertions.assertEquals(StorageLayoutVersion.V1, entityStore.storageLayoutVersion);
    }

    // Second time create entity store, the storage layout version should be DEFAULT_LAYOUT_VERSION
    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);
      Assertions.assertTrue(store instanceof KvEntityStore);
      store.setSerDe(EntitySerDeFactory.createEntitySerDe(config.get(Configs.ENTITY_SERDE)));
      KvEntityStore entityStore = (KvEntityStore) store;
      Assertions.assertEquals(StorageLayoutVersion.V1, entityStore.storageLayoutVersion);
    }
  }
}
