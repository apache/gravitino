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

package org.apache.gravitino.iceberg.service.purge;

import com.google.common.collect.ImmutableMap;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestIcebergPurgeService {

  private JdbcClientPool pool;
  private IcebergPurgeJobStore store;

  @BeforeEach
  void setUp() throws Exception {
    String url = "jdbc:h2:mem:svc_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1;MODE=MySQL";
    Map<String, String> props = new HashMap<>();
    props.put("jdbc.user", "sa");
    props.put("jdbc.password", "");
    pool = new JdbcClientPool(url, props);
    pool.run(
        conn -> {
          try (Statement st = conn.createStatement()) {
            for (String ddl : PurgeTestSchema.H2_CREATE.split(";")) {
              if (!ddl.trim().isEmpty()) {
                st.execute(ddl);
              }
            }
          }
          return null;
        });
    store = new IcebergPurgeJobStore(pool);
  }

  @AfterEach
  void tearDown() {
    pool.close();
  }

  private static IcebergConfig fastPollConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("async-purge.worker-threads", "1");
    config.put("async-purge.poll-interval-ms", "50");
    return new IcebergConfig(config);
  }

  private static IcebergPurgeJob sampleJob() {
    return new IcebergPurgeJob(
        0L,
        "ml",
        "cat",
        "db",
        "t",
        "s3://b/db/t/metadata/0.json",
        NoopFileIO.class.getName(),
        ImmutableMap.of(),
        "alice");
  }

  @Test
  void testDeleteAllBatchesEveryFile() {
    CopyOnWriteArrayList<String> deleted = new CopyOnWriteArrayList<>();
    IcebergPurgeService svc = new IcebergPurgeService(store, new IcebergConfig(new HashMap<>()));
    try {
      svc.deleteAll(new RecordingFileIO(deleted), Arrays.asList("a", "b", "c", "d", "e", "f", "g"));
      Assertions.assertEquals(7, deleted.size());
    } finally {
      svc.close();
    }
  }

  @Test
  void testWorkerRunsJobToSucceeded() {
    AtomicInteger calls = new AtomicInteger();
    IcebergPurgeService svc =
        new IcebergPurgeService(store, fastPollConfig()) {
          @Override
          void purgeFiles(FileIO io, String metadataLocation) {
            calls.incrementAndGet();
          }
        };
    long id = store.enqueue(sampleJob());
    svc.start();
    try {
      Awaitility.await()
          .atMost(5, TimeUnit.SECONDS)
          .until(() -> store.stateOf(id) == IcebergPurgeJob.State.SUCCEEDED);
      Assertions.assertEquals(1, calls.get());
    } finally {
      svc.close();
    }
  }

  @Test
  void testTransientFailureRetriesThenFails() {
    Map<String, String> config = new HashMap<>();
    config.put("async-purge.worker-threads", "1");
    config.put("async-purge.poll-interval-ms", "50");
    config.put("async-purge.max-attempts", "3");
    IcebergPurgeService svc =
        new IcebergPurgeService(store, new IcebergConfig(config)) {
          @Override
          void purgeFiles(FileIO io, String metadataLocation) {
            throw new RuntimeException("transient");
          }
        };
    long id = store.enqueue(sampleJob());
    svc.start();
    try {
      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .until(() -> store.stateOf(id) == IcebergPurgeJob.State.FAILED);
    } finally {
      svc.close();
    }
  }

  @Test
  void testIsNameOccupiedDelegatesToStore() {
    IcebergPurgeService svc = new IcebergPurgeService(store, new IcebergConfig(new HashMap<>()));
    try {
      Assertions.assertFalse(svc.isNameOccupied("cat", "db", "t"));
      svc.enqueue(sampleJob());
      Assertions.assertTrue(svc.isNameOccupied("cat", "db", "t"));
    } finally {
      svc.close();
    }
  }

  static class RecordingFileIO implements SupportsBulkOperations {
    private final CopyOnWriteArrayList<String> deleted;

    RecordingFileIO(CopyOnWriteArrayList<String> deleted) {
      this.deleted = deleted;
    }

    @Override
    public InputFile newInputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public OutputFile newOutputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String path) {
      deleted.add(path);
    }

    @Override
    public void deleteFiles(Iterable<String> paths) {
      for (String path : paths) {
        deleted.add(path);
      }
    }
  }

  public static class NoopFileIO implements FileIO {
    @Override
    public InputFile newInputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public OutputFile newOutputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String path) {
      throw new UnsupportedOperationException();
    }
  }
}
