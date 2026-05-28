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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestIcebergPurgeJob {

  @Test
  void testConstructorAndAccessors() {
    IcebergPurgeJob job =
        new IcebergPurgeJob(
            0L,
            "ml",
            "cat",
            "db",
            "t",
            "s3://b/db/t/metadata/0.json",
            "org.apache.iceberg.aws.s3.S3FileIO",
            ImmutableMap.of("k", "v"),
            "alice");
    Assertions.assertEquals("cat", job.catalogName());
    Assertions.assertEquals("db", job.namespace());
    Assertions.assertEquals(ImmutableMap.of("k", "v"), job.fileIOProperties());
    Assertions.assertEquals("alice", job.createdBy());
  }

  @Test
  void testTerminalStates() {
    Assertions.assertTrue(IcebergPurgeJob.State.SUCCEEDED.isTerminal());
    Assertions.assertTrue(IcebergPurgeJob.State.FAILED.isTerminal());
    Assertions.assertFalse(IcebergPurgeJob.State.PENDING.isTerminal());
    Assertions.assertFalse(IcebergPurgeJob.State.RUNNING.isTerminal());
  }
}
