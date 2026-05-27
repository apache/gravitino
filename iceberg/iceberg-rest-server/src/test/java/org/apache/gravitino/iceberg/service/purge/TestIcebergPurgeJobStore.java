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
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestIcebergPurgeJobStore {

  private JdbcClientPool pool;
  IcebergPurgeJobStore store;

  @BeforeEach
  void setUp() throws Exception {
    String url = "jdbc:h2:mem:purge_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1;MODE=MySQL";
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

  static IcebergPurgeJob sampleJob() {
    return new IcebergPurgeJob(
        0L,
        "ml",
        "cat",
        "db",
        "t",
        "s3://b/db/t/metadata/0.json",
        "org.apache.iceberg.aws.s3.S3FileIO",
        ImmutableMap.of("k", "v"),
        "alice");
  }

  @Test
  void testEnqueueThenClaim() {
    long id = store.enqueue(sampleJob());
    Assertions.assertTrue(id > 0);

    long now = System.currentTimeMillis();
    IcebergPurgeJob claimed = store.claimNext(now, 300_000L, 10);
    Assertions.assertNotNull(claimed);
    Assertions.assertEquals(id, claimed.id());
    Assertions.assertEquals(ImmutableMap.of("k", "v"), claimed.fileIoProperties());
    Assertions.assertEquals(IcebergPurgeJob.State.RUNNING, store.stateOf(id));

    Assertions.assertNull(store.claimNext(now, 300_000L, 10));
  }

  @Test
  void testStaleRunningIsReclaimable() {
    long id = store.enqueue(sampleJob());
    long t0 = System.currentTimeMillis();
    Assertions.assertEquals(id, store.claimNext(t0, 300_000L, 10).id());
    Assertions.assertEquals(id, store.claimNext(t0 + 400_000L, 300_000L, 10).id());
  }
}
