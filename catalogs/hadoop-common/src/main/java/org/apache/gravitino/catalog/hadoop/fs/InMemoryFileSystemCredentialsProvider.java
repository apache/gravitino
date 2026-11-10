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

import com.google.common.base.Preconditions;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.gravitino.credential.Credential;
import org.apache.hadoop.conf.Configuration;

/**
 * Supplies short-lived credentials to a FileSystem created in the same process.
 *
 * <p>The Hadoop configuration contains only an opaque handle. Credential values remain in memory
 * and are removed after the connection probe completes.
 */
public class InMemoryFileSystemCredentialsProvider
    implements GravitinoFileSystemCredentialsProvider {

  /** Hadoop configuration key containing the opaque credential handle. */
  public static final String CREDENTIAL_HANDLE = "fs.gvfs.credential.handle";

  private static final ConcurrentMap<String, Credential[]> CREDENTIALS = new ConcurrentHashMap<>();

  private Configuration conf;

  /**
   * Registers credentials and returns an opaque handle.
   *
   * @param credentials credentials used by one FileSystem probe.
   * @return an opaque handle for the registered credentials.
   */
  public static String register(Credential[] credentials) {
    Preconditions.checkArgument(credentials != null, "Credentials must not be null");
    String handle = UUID.randomUUID().toString();
    CREDENTIALS.put(handle, credentials.clone());
    return handle;
  }

  /**
   * Removes credentials associated with an opaque handle.
   *
   * @param handle the handle returned by {@link #register(Credential[])}.
   */
  public static void unregister(String handle) {
    if (handle != null) {
      CREDENTIALS.remove(handle);
    }
  }

  @Override
  public Credential[] getCredentials() {
    Preconditions.checkState(conf != null, "Configuration has not been set");
    String handle = conf.get(CREDENTIAL_HANDLE);
    Credential[] credentials = CREDENTIALS.get(handle);
    Preconditions.checkState(credentials != null, "Credentials are no longer available");
    return credentials.clone();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
