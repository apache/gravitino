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

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * A key class for caching FileSystem instances. The cache key is based on the scheme, authority,
 * user identity, and optionally a configuration map.
 *
 * <p>This class is designed to be used as a key in a cache for FileSystem instances. The key is
 * unique based on the combination of scheme, authority, current user, and optional configuration.
 */
public class FileSystemCacheKey {

  @Nullable private final String scheme;
  @Nullable private final String authority;
  private final String currentUser;
  @Nullable private final Map<String, String> conf;
  @Nullable private final UserGroupInformation ugi;

  /**
   * Creates a new FileSystemCacheKey with scheme, authority, and UGI.
   *
   * @param scheme the scheme of the filesystem (e.g., "hdfs", "s3a")
   * @param authority the authority of the filesystem (e.g., "namenode:8020")
   * @param ugi the UserGroupInformation for the current user
   */
  public FileSystemCacheKey(String scheme, String authority, UserGroupInformation ugi) {
    this.scheme = scheme;
    this.authority = authority;
    this.ugi = ugi;
    this.currentUser = ugi.getShortUserName();
    this.conf = null;
  }

  /**
   * Creates a new FileSystemCacheKey with scheme, authority, and configuration map. The current
   * user is obtained from UserGroupInformation.
   *
   * @param scheme the scheme of the filesystem (e.g., "hdfs", "s3a")
   * @param authority the authority of the filesystem (e.g., "namenode:8020")
   * @param conf the configuration map used for this filesystem
   * @throws RuntimeException if unable to get the current user
   */
  public FileSystemCacheKey(String scheme, String authority, Map<String, String> conf) {
    this.scheme = scheme;
    this.authority = authority;
    this.conf = conf;
    this.ugi = null;

    try {
      this.currentUser = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      throw new RuntimeException("Failed to get current user", e);
    }
  }

  /**
   * Get the scheme of the filesystem.
   *
   * @return the scheme
   */
  @Nullable
  public String scheme() {
    return scheme;
  }

  /**
   * Get the authority of the filesystem.
   *
   * @return the authority
   */
  @Nullable
  public String authority() {
    return authority;
  }

  /**
   * Get the UserGroupInformation.
   *
   * @return the UserGroupInformation, or null if not set
   */
  @Nullable
  public UserGroupInformation ugi() {
    return ugi;
  }

  /**
   * Get the current user name.
   *
   * @return the current user name
   */
  public String currentUser() {
    return currentUser;
  }

  /**
   * Get the configuration map.
   *
   * @return the configuration map, or null if not set
   */
  @Nullable
  public Map<String, String> conf() {
    return conf;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FileSystemCacheKey)) {
      return false;
    }
    FileSystemCacheKey that = (FileSystemCacheKey) o;

    // When UGI is set, compare using UGI; otherwise, compare using conf
    if (ugi != null || that.ugi != null) {
      return Objects.equals(scheme, that.scheme)
          && Objects.equals(authority, that.authority)
          && Objects.equals(ugi, that.ugi);
    } else {
      return Objects.equals(scheme, that.scheme)
          && Objects.equals(authority, that.authority)
          && Objects.equals(conf, that.conf)
          && Objects.equals(currentUser, that.currentUser);
    }
  }

  @Override
  public int hashCode() {
    // When UGI is set, hash using UGI; otherwise, hash using conf
    if (ugi != null) {
      return Objects.hash(scheme, authority, ugi);
    } else {
      return Objects.hash(scheme, authority, conf, currentUser);
    }
  }

  @Override
  public String toString() {
    return "FileSystemCacheKey{"
        + "scheme='"
        + scheme
        + '\''
        + ", authority='"
        + authority
        + '\''
        + ", currentUser='"
        + currentUser
        + '\''
        + '}';
  }
}
