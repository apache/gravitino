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
package org.apache.gravitino.iceberg.common.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link FileIO} implementation that dispatches to per-scheme FileIO delegates based on the URI
 * scheme of each path. Enables a single Iceberg catalog to span multiple storage backends (e.g. S3
 * and GCS) by routing s3://, s3a://, s3n:// paths to S3FileIO and gs:// paths to GCSFileIO.
 *
 * <p>This is the server-side counterpart to the credential-scheme dispatch introduced in the
 * multi-backend credential vending spike (spike/multi-backend-credentials).
 */
public class MultiSchemeFileIO implements FileIO {

  private static final Logger LOG = LoggerFactory.getLogger(MultiSchemeFileIO.class);

  /** Maps URI scheme to FileIO delegate. Populated at initialize() time. */
  private Map<String, FileIO> delegatesByScheme;

  public MultiSchemeFileIO() {}

  @VisibleForTesting
  MultiSchemeFileIO(Map<String, FileIO> delegatesByScheme) {
    this.delegatesByScheme = ImmutableMap.copyOf(delegatesByScheme);
  }

  @Override
  public void initialize(Map<String, String> properties) {
    ImmutableMap.Builder<String, FileIO> builder = ImmutableMap.builder();

    // S3FileIO handles s3://, s3a://, s3n://
    // Translates Gravitino property keys (s3-access-key-id) to Iceberg keys (s3.access-key-id)
    try {
      Class<?> s3Class = Class.forName("org.apache.iceberg.aws.s3.S3FileIO");
      FileIO s3FileIO = (FileIO) s3Class.getDeclaredConstructor().newInstance();
      s3FileIO.initialize(toS3FileIOProperties(properties));
      builder.put("s3", s3FileIO);
      builder.put("s3a", s3FileIO);
      builder.put("s3n", s3FileIO);
      LOG.info("MultiSchemeFileIO: registered S3FileIO for s3/s3a/s3n schemes");
    } catch (Exception e) {
      LOG.warn("MultiSchemeFileIO: S3FileIO not available: {}", e.getMessage());
    }

    // GCSFileIO handles gs://
    // Uses GOOGLE_APPLICATION_CREDENTIALS env var for authentication (ADC)
    try {
      Class<?> gcsClass = Class.forName("org.apache.iceberg.gcp.gcs.GCSFileIO");
      FileIO gcsFileIO = (FileIO) gcsClass.getDeclaredConstructor().newInstance();
      gcsFileIO.initialize(toGCSFileIOProperties(properties));
      builder.put("gs", gcsFileIO);
      LOG.info("MultiSchemeFileIO: registered GCSFileIO for gs scheme");
    } catch (Exception e) {
      LOG.warn("MultiSchemeFileIO: GCSFileIO not available: {}", e.getMessage());
    }

    this.delegatesByScheme = builder.build();
    Preconditions.checkState(
        !delegatesByScheme.isEmpty(), "MultiSchemeFileIO: no FileIO delegates registered");
  }

  /**
   * Translates Gravitino S3 catalog properties to Iceberg S3FileIO property keys.
   *
   * <p>Gravitino uses: s3-access-key-id, s3-secret-access-key, s3-region Iceberg expects:
   * s3.access-key-id, s3.secret-access-key, s3.region
   */
  @VisibleForTesting
  static Map<String, String> toS3FileIOProperties(Map<String, String> properties) {
    Map<String, String> result = new HashMap<>(properties);
    translateKey(result, "s3-access-key-id", "s3.access-key-id");
    translateKey(result, "s3-secret-access-key", "s3.secret-access-key");
    translateKey(result, "s3-region", "s3.region");
    translateKey(result, "s3-endpoint", "s3.endpoint");
    return result;
  }

  /**
   * Translates Gravitino GCS catalog properties to Iceberg GCSFileIO property keys.
   *
   * <p>Gravitino uses: gcs-project-id Iceberg expects: gcs.project-id
   */
  @VisibleForTesting
  static Map<String, String> toGCSFileIOProperties(Map<String, String> properties) {
    Map<String, String> result = new HashMap<>(properties);
    translateKey(result, "gcs-project-id", "gcs.project-id");
    return result;
  }

  private static void translateKey(Map<String, String> props, String fromKey, String toKey) {
    if (props.containsKey(fromKey) && !props.containsKey(toKey)) {
      props.put(toKey, props.get(fromKey));
    }
  }

  @Override
  public InputFile newInputFile(String path) {
    return delegate(path).newInputFile(path);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return delegate(path).newInputFile(path, length);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return delegate(path).newOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    delegate(path).deleteFile(path);
  }

  @Override
  public Map<String, String> properties() {
    return delegatesByScheme.values().iterator().next().properties();
  }

  @Override
  public void close() {
    delegatesByScheme.values().forEach(FileIO::close);
  }

  private FileIO delegate(String path) {
    String scheme = extractScheme(path);
    FileIO delegate = delegatesByScheme.get(scheme);
    Preconditions.checkState(
        delegate != null,
        "MultiSchemeFileIO: no FileIO registered for scheme '%s' (path: %s)",
        scheme,
        path);
    return delegate;
  }

  @VisibleForTesting
  static String extractScheme(String path) {
    try {
      URI uri = URI.create(path);
      String scheme = uri.getScheme();
      Preconditions.checkNotNull(scheme, "Path has no scheme: %s", path);
      return scheme.toLowerCase();
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid path URI: " + path, e);
    }
  }
}
