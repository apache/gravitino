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
package org.apache.gravitino.filesystem.hadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.IOException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link BaseGVFSOperations.FileSystemCacheKey}. */
public class TestFileSystemCacheKey {

  @Test
  public void testEqualityWithSameValues() throws IOException {
    UserGroupInformation ugi1 = UserGroupInformation.getCurrentUser();
    UserGroupInformation ugi2 = UserGroupInformation.getCurrentUser();

    BaseGVFSOperations.FileSystemCacheKey key1 =
        new BaseGVFSOperations.FileSystemCacheKey("hdfs", "namenode:8020", ugi1);
    BaseGVFSOperations.FileSystemCacheKey key2 =
        new BaseGVFSOperations.FileSystemCacheKey("hdfs", "namenode:8020", ugi2);

    assertEquals(key1, key2);
    assertEquals(key1.hashCode(), key2.hashCode());
  }

  @Test
  public void testInequalityWithDifferentScheme() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    BaseGVFSOperations.FileSystemCacheKey key1 =
        new BaseGVFSOperations.FileSystemCacheKey("hdfs", "namenode:8020", ugi);
    BaseGVFSOperations.FileSystemCacheKey key2 =
        new BaseGVFSOperations.FileSystemCacheKey("s3a", "namenode:8020", ugi);

    assertNotEquals(key1, key2);
  }

  @Test
  public void testInequalityWithDifferentAuthority() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    BaseGVFSOperations.FileSystemCacheKey key1 =
        new BaseGVFSOperations.FileSystemCacheKey("hdfs", "namenode1:8020", ugi);
    BaseGVFSOperations.FileSystemCacheKey key2 =
        new BaseGVFSOperations.FileSystemCacheKey("hdfs", "namenode2:8020", ugi);

    assertNotEquals(key1, key2);
  }

  @Test
  public void testInequalityWithNullAuthority() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    BaseGVFSOperations.FileSystemCacheKey key1 =
        new BaseGVFSOperations.FileSystemCacheKey("file", null, ugi);
    BaseGVFSOperations.FileSystemCacheKey key2 =
        new BaseGVFSOperations.FileSystemCacheKey("file", "localhost", ugi);

    assertNotEquals(key1, key2);
  }

  @Test
  public void testEqualityWithBothNullAuthority() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    BaseGVFSOperations.FileSystemCacheKey key1 =
        new BaseGVFSOperations.FileSystemCacheKey("file", null, ugi);
    BaseGVFSOperations.FileSystemCacheKey key2 =
        new BaseGVFSOperations.FileSystemCacheKey("file", null, ugi);

    assertEquals(key1, key2);
    assertEquals(key1.hashCode(), key2.hashCode());
  }

  @Test
  public void testGetters() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    BaseGVFSOperations.FileSystemCacheKey key =
        new BaseGVFSOperations.FileSystemCacheKey("hdfs", "namenode:8020", ugi);

    assertEquals("hdfs", key.scheme());
    assertEquals("namenode:8020", key.authority());
    assertEquals(ugi, key.ugi());
  }

  @Test
  public void testNotEqualsWithDifferentType() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    BaseGVFSOperations.FileSystemCacheKey key =
        new BaseGVFSOperations.FileSystemCacheKey("hdfs", "namenode:8020", ugi);

    assertNotEquals(key, "not a FileSystemCacheKey");
    assertNotEquals(key, null);
  }
}
