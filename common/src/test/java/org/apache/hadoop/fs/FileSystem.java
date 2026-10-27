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

package org.apache.hadoop.fs;

import com.google.errorprone.annotations.DoNotCall;
import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;

/**
 * Test stub of the Hadoop FileSystem recording which {@code get} overload the FileFetcher uses. In
 * real Hadoop, {@code get(Configuration)} resolves the filesystem from {@code fs.defaultFS} and
 * ignores the fetched URI's authority, while {@code get(URI, Configuration)} resolves it from the
 * URI — which is the contract the fetcher must uphold.
 */
public abstract class FileSystem {

  /** The URI passed to {@code get(URI, Configuration)} by the code under test, if any. */
  public static final AtomicReference<URI> uriOverloadUsed = new AtomicReference<>();

  /**
   * The single-arg {@code get(Configuration)} overload resolved from fs.defaultFS. Called only
   * reflectively by the code under test, so the {@code @DoNotCall} annotation affects no
   * compile-time caller.
   */
  @DoNotCall("Always throws: the default-FS overload must not be used to fetch a specific uri")
  public static FileSystem get(Configuration conf) {
    // Mirrors real Hadoop: the default filesystem, whatever the fetched URI points at.
    throw new IllegalArgumentException(
        "Wrong FS: the filesystem was resolved from fs.defaultFS, not from the fetched uri");
  }

  /** The two-arg {@code get(URI, Configuration)} overload resolving from the URI. */
  public static FileSystem get(URI uri, Configuration conf) {
    uriOverloadUsed.set(uri);
    return new LocalStub();
  }

  /** Copy that succeeds; the stub only records the call through the overload bookkeeping. */
  public void copyToLocalFile(Path src, Path dst) {}

  private static final class LocalStub extends FileSystem {}
}
