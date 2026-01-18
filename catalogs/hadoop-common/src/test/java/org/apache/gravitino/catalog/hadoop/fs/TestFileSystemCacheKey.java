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
package org.apache.gravitino.catalog.hadoop.fs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link FileSystemCacheKey}. */
public class TestFileSystemCacheKey {

  @Test
  public void testConstructorWithUGI() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    FileSystemCacheKey key = new FileSystemCacheKey("hdfs", "namenode:8020", ugi);

    assertEquals("hdfs", key.scheme());
    assertEquals("namenode:8020", key.authority());
    assertEquals(ugi, key.ugi());
    assertEquals(ugi.getShortUserName(), key.currentUser());
    assertNull(key.conf());
  }

  @Test
  public void testConstructorWithConf() throws IOException {
    Map<String, String> conf = ImmutableMap.of("key1", "value1", "key2", "value2");

    FileSystemCacheKey key = new FileSystemCacheKey("s3a", "bucket", conf);

    assertEquals("s3a", key.scheme());
    assertEquals("bucket", key.authority());
    assertNull(key.ugi());
    assertNotNull(key.currentUser());
    assertEquals(conf, key.conf());
  }

  @Test
  public void testEqualityWithSameValuesUGI() throws IOException {
    UserGroupInformation ugi1 = UserGroupInformation.getCurrentUser();
    UserGroupInformation ugi2 = UserGroupInformation.getCurrentUser();

    FileSystemCacheKey key1 = new FileSystemCacheKey("hdfs", "namenode:8020", ugi1);
    FileSystemCacheKey key2 = new FileSystemCacheKey("hdfs", "namenode:8020", ugi2);

    assertEquals(key1, key2);
    assertEquals(key1.hashCode(), key2.hashCode());
  }

  @Test
  public void testEqualityWithSameValuesConf() {
    Map<String, String> conf1 = ImmutableMap.of("key", "value");
    Map<String, String> conf2 = ImmutableMap.of("key", "value");

    FileSystemCacheKey key1 = new FileSystemCacheKey("s3a", "bucket", conf1);
    FileSystemCacheKey key2 = new FileSystemCacheKey("s3a", "bucket", conf2);

    assertEquals(key1, key2);
    assertEquals(key1.hashCode(), key2.hashCode());
  }

  @Test
  public void testInequalityWithDifferentScheme() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    FileSystemCacheKey key1 = new FileSystemCacheKey("hdfs", "namenode:8020", ugi);
    FileSystemCacheKey key2 = new FileSystemCacheKey("s3a", "namenode:8020", ugi);

    assertNotEquals(key1, key2);
  }

  @Test
  public void testInequalityWithDifferentAuthority() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    FileSystemCacheKey key1 = new FileSystemCacheKey("hdfs", "namenode1:8020", ugi);
    FileSystemCacheKey key2 = new FileSystemCacheKey("hdfs", "namenode2:8020", ugi);

    assertNotEquals(key1, key2);
  }

  @Test
  public void testInequalityWithNullAuthority() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    FileSystemCacheKey key1 = new FileSystemCacheKey("file", null, ugi);
    FileSystemCacheKey key2 = new FileSystemCacheKey("file", "localhost", ugi);

    assertNotEquals(key1, key2);
  }

  @Test
  public void testEqualityWithBothNullAuthority() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    FileSystemCacheKey key1 = new FileSystemCacheKey("file", null, ugi);
    FileSystemCacheKey key2 = new FileSystemCacheKey("file", null, ugi);

    assertEquals(key1, key2);
    assertEquals(key1.hashCode(), key2.hashCode());
  }

  @Test
  public void testInequalityWithDifferentConf() {
    Map<String, String> conf1 = ImmutableMap.of("key", "value1");
    Map<String, String> conf2 = ImmutableMap.of("key", "value2");

    FileSystemCacheKey key1 = new FileSystemCacheKey("s3a", "bucket", conf1);
    FileSystemCacheKey key2 = new FileSystemCacheKey("s3a", "bucket", conf2);

    assertNotEquals(key1, key2);
  }

  @Test
  public void testNotEqualsWithDifferentType() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    FileSystemCacheKey key = new FileSystemCacheKey("hdfs", "namenode:8020", ugi);

    assertNotEquals(key, "not a FileSystemCacheKey");
    assertNotEquals(key, null);
  }

  @Test
  public void testToString() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    FileSystemCacheKey key = new FileSystemCacheKey("hdfs", "namenode:8020", ugi);
    String str = key.toString();

    assertNotNull(str);
    assertEquals(
        "FileSystemCacheKey{scheme='hdfs', authority='namenode:8020', currentUser='"
            + ugi.getShortUserName()
            + "'}",
        str);
  }

  @Test
  public void testEqualsWithSelf() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    FileSystemCacheKey key = new FileSystemCacheKey("hdfs", "namenode:8020", ugi);

    assertEquals(key, key);
  }
}
