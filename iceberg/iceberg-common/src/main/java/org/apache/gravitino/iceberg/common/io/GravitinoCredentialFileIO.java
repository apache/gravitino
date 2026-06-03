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

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.io.SupportsStorageCredentials;

/** FileIO wrapper that refreshes Gravitino storage credentials before path operations. */
public class GravitinoCredentialFileIO implements DelegateFileIO {

  /** Property for the wrapped Iceberg FileIO implementation. */
  public static final String DELEGATE_IO_IMPL = "gravitino.fileio.delegate-impl";

  /**
   * Property for the credential supplier registered in {@link GravitinoCredentialFileIORegistry}.
   */
  public static final String CREDENTIAL_SUPPLIER_ID = "gravitino.fileio.credential-supplier-id";

  private FileIO delegate;
  private GravitinoCredentialFileIORegistry.CredentialSupplier credentialSupplier;
  private Map<String, String> properties;

  /**
   * Wraps an Iceberg FileIO configuration with Gravitino credential refresh support.
   *
   * @param properties original Iceberg FileIO properties
   * @param credentialSupplierId registered credential supplier identifier
   * @return wrapped properties
   */
  public static Map<String, String> wrapProperties(
      Map<String, String> properties, String credentialSupplierId) {
    Map<String, String> wrappedProperties = new HashMap<>(properties);
    String delegateImpl = wrappedProperties.get(IcebergConstants.IO_IMPL);
    Preconditions.checkArgument(delegateImpl != null, "Iceberg FileIO implementation is not set");
    wrappedProperties.put(DELEGATE_IO_IMPL, delegateImpl);
    wrappedProperties.put(IcebergConstants.IO_IMPL, GravitinoCredentialFileIO.class.getName());
    wrappedProperties.put(CREDENTIAL_SUPPLIER_ID, credentialSupplierId);
    return wrappedProperties;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    this.properties = new HashMap<>(properties);
    String delegateImpl = properties.get(DELEGATE_IO_IMPL);
    String credentialSupplierId = properties.get(CREDENTIAL_SUPPLIER_ID);
    Preconditions.checkArgument(delegateImpl != null, "Delegate FileIO implementation is not set");
    Preconditions.checkArgument(
        credentialSupplierId != null, "Gravitino credential supplier id is not set");
    credentialSupplier = GravitinoCredentialFileIORegistry.get(credentialSupplierId);
    Preconditions.checkState(
        credentialSupplier != null,
        "Gravitino credential supplier %s is not registered",
        credentialSupplierId);

    Map<String, String> delegateProperties = new HashMap<>(properties);
    delegateProperties.put(IcebergConstants.IO_IMPL, delegateImpl);
    delegateProperties.remove(DELEGATE_IO_IMPL);
    delegateProperties.remove(CREDENTIAL_SUPPLIER_ID);
    delegate = CatalogUtil.loadFileIO(delegateImpl, delegateProperties, null);
  }

  @Override
  public InputFile newInputFile(String path) {
    refreshCredentials(path);
    return delegate.newInputFile(path);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    refreshCredentials(path);
    return delegate.newInputFile(path, length);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    refreshCredentials(path);
    return delegate.newOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    refreshCredentials(path);
    delegate.deleteFile(path);
  }

  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    refreshCredentials(prefix);
    if (delegate instanceof SupportsPrefixOperations) {
      return ((SupportsPrefixOperations) delegate).listPrefix(prefix);
    }
    throw new UnsupportedOperationException("Delegate FileIO does not support prefix operations");
  }

  @Override
  public void deletePrefix(String prefix) {
    refreshCredentials(prefix);
    if (delegate instanceof SupportsPrefixOperations) {
      ((SupportsPrefixOperations) delegate).deletePrefix(prefix);
      return;
    }
    throw new UnsupportedOperationException("Delegate FileIO does not support prefix operations");
  }

  @Override
  public void deleteFiles(Iterable<String> paths) throws BulkDeletionFailureException {
    String path = paths.iterator().hasNext() ? paths.iterator().next() : null;
    if (path != null) {
      refreshCredentials(path);
    }
    if (delegate instanceof SupportsBulkOperations) {
      ((SupportsBulkOperations) delegate).deleteFiles(paths);
      return;
    }

    int failures = 0;
    for (String deletePath : paths) {
      try {
        deleteFile(deletePath);
      } catch (RuntimeException e) {
        failures++;
      }
    }
    if (failures > 0) {
      throw new BulkDeletionFailureException(failures);
    }
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public void close() {
    if (delegate != null) {
      delegate.close();
    }
  }

  private void refreshCredentials(String path) {
    if (delegate instanceof SupportsStorageCredentials) {
      ((SupportsStorageCredentials) delegate).setCredentials(credentialSupplier.load(path));
    }
  }
}
