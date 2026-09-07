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
package org.apache.gravitino.iceberg.common.io;

import com.google.auth.oauth2.AccessToken;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.storage.GCSProperties;
import org.apache.iceberg.gcp.gcs.GCSFileIO;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GCS {@link DelegateFileIO} that authenticates with {@code gcs-service-account-file}.
 *
 * <p>Iceberg's built-in {@link GCSFileIO} only understands a static {@code gcs.oauth2.token}. This
 * wrapper keeps a delegate {@link GCSFileIO} in sync with {@link GcsAccessTokenCache}, recreating
 * the delegate when the cached token is reminted. Token refresh does not depend on IRC catalog
 * cache eviction.
 */
public class GravitinoGCSFileIO implements DelegateFileIO, SupportsStorageCredentials {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoGCSFileIO.class);

  private Map<String, String> properties;
  private String serviceAccountFile;
  private volatile GCSFileIO delegate;
  private volatile String boundToken;

  @Override
  public void initialize(Map<String, String> props) {
    this.properties = new HashMap<>(props);
    this.serviceAccountFile = props.get(GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE);
    // Drop any stale delegate after re-initialize.
    closeDelegate();
    currentDelegate();
  }

  @Override
  public InputFile newInputFile(String path) {
    return currentDelegate().newInputFile(path);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return currentDelegate().newInputFile(path, length);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return currentDelegate().newOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    currentDelegate().deleteFile(path);
  }

  @Override
  public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
    currentDelegate().deleteFiles(pathsToDelete);
  }

  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    return currentDelegate().listPrefix(prefix);
  }

  @Override
  public void deletePrefix(String prefix) {
    currentDelegate().deletePrefix(prefix);
  }

  @Override
  public Map<String, String> properties() {
    return currentDelegate().properties();
  }

  @Override
  public void setCredentials(List<StorageCredential> credentials) {
    currentDelegate().setCredentials(credentials);
  }

  @Override
  public List<StorageCredential> credentials() {
    return currentDelegate().credentials();
  }

  @Override
  public void close() {
    closeDelegate();
  }

  private GCSFileIO currentDelegate() {
    if (StringUtils.isBlank(serviceAccountFile)) {
      return ensureDelegate(null);
    }
    AccessToken accessToken = GcsAccessTokenCache.get(serviceAccountFile);
    return ensureDelegate(accessToken);
  }

  private GCSFileIO ensureDelegate(AccessToken accessToken) {
    String tokenValue = accessToken == null ? null : accessToken.getTokenValue();
    GCSFileIO current = delegate;
    if (current != null && java.util.Objects.equals(boundToken, tokenValue)) {
      return current;
    }
    synchronized (this) {
      current = delegate;
      if (current != null && java.util.Objects.equals(boundToken, tokenValue)) {
        return current;
      }
      closeDelegate();
      Map<String, String> delegateProperties = new HashMap<>(properties);
      if (accessToken != null) {
        delegateProperties.put(
            IcebergConstants.ICEBERG_GCS_OAUTH2_TOKEN, accessToken.getTokenValue());
        Date expirationTime = accessToken.getExpirationTime();
        if (expirationTime != null) {
          delegateProperties.put(
              IcebergConstants.ICEBERG_GCS_OAUTH2_TOKEN_EXPIRES_AT,
              String.valueOf(expirationTime.toInstant().toEpochMilli()));
        }
        // Catalog bootstrap from a service-account file has no table credentials endpoint.
        delegateProperties.put(
            IcebergConstants.ICEBERG_GCS_OAUTH2_REFRESH_CREDENTIALS_ENABLED, "false");
        LOG.debug(
            "Binding GCSFileIO delegate to token from {}",
            GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE);
      }
      GCSFileIO created = new GCSFileIO();
      created.initialize(delegateProperties);
      this.delegate = created;
      this.boundToken = tokenValue;
      return created;
    }
  }

  private void closeDelegate() {
    GCSFileIO current = delegate;
    if (current == null) {
      return;
    }
    try {
      current.close();
    } catch (RuntimeException e) {
      LOG.warn("Failed to close GCSFileIO delegate", e);
    } finally {
      this.delegate = null;
      this.boundToken = null;
    }
  }
}
