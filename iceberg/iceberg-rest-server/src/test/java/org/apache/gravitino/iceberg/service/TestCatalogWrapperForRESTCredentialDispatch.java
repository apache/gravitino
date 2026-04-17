/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.credential.CatalogCredentialManager;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialContext;
import org.apache.gravitino.credential.CredentialPrivilege;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCatalogWrapperForRESTCredentialDispatch {

  /** Minimal Credential impl used to verify which credential type was vended in each test. */
  private static class TestCredential implements Credential {
    private final String type;

    TestCredential(String type) {
      this.type = type;
    }

    @Override
    public String credentialType() {
      return type;
    }

    @Override
    public long expireTimeInMs() {
      return 0;
    }

    @Override
    public Map<String, String> credentialInfo() {
      return new HashMap<>();
    }

    @Override
    public void initialize(Map<String, String> credentialInfo, long expireTimeInMs) {}
  }

  private static TableMetadata mockTableMetadata(String location) {
    return mockTableMetadata(location, "", "");
  }

  private static TableMetadata mockTableMetadata(
      String location, String writeDataLocation, String writeMetadataLocation) {
    TableMetadata md = mock(TableMetadata.class);
    when(md.location()).thenReturn(location);
    when(md.property(eq(TableProperties.WRITE_DATA_LOCATION), eq("")))
        .thenReturn(writeDataLocation);
    when(md.property(eq(TableProperties.WRITE_METADATA_LOCATION), eq("")))
        .thenReturn(writeMetadataLocation);
    return md;
  }

  private static CatalogCredentialManager mockManager(
      ImmutableSet<String> registeredTypes, String returnForType) {
    CatalogCredentialManager manager = mock(CatalogCredentialManager.class);
    when(manager.getCredentialTypes()).thenReturn(registeredTypes);
    when(manager.getCredential(eq(returnForType), any(CredentialContext.class)))
        .thenReturn(new TestCredential(returnForType));
    return manager;
  }

  @Test
  void testSingleProviderS3DispatchesToS3() {
    CatalogCredentialManager manager = mockManager(ImmutableSet.of("s3-token"), "s3-token");
    TableMetadata md = mockTableMetadata("s3://bucket/ns/t/");

    List<Credential> result =
        CatalogWrapperForREST.getCredentials(manager, md, CredentialPrivilege.READ);

    Assertions.assertEquals(1, result.size());
    Assertions.assertEquals("s3-token", result.get(0).credentialType());
  }

  @Test
  void testMultiProviderGcsPathDispatchesToGcs() {
    CatalogCredentialManager manager =
        mockManager(ImmutableSet.of("s3-token", "gcs-token"), "gcs-token");
    TableMetadata md = mockTableMetadata("gs://bucket/ns/t/");

    List<Credential> result =
        CatalogWrapperForREST.getCredentials(manager, md, CredentialPrivilege.READ);

    Assertions.assertEquals(1, result.size());
    Assertions.assertEquals("gcs-token", result.get(0).credentialType());
  }

  @Test
  void testMultiProviderS3PathDispatchesToS3() {
    CatalogCredentialManager manager =
        mockManager(ImmutableSet.of("s3-token", "gcs-token"), "s3-token");
    TableMetadata md = mockTableMetadata("s3://bucket/ns/t/");

    List<Credential> result =
        CatalogWrapperForREST.getCredentials(manager, md, CredentialPrivilege.READ);

    Assertions.assertEquals(1, result.size());
    Assertions.assertEquals("s3-token", result.get(0).credentialType());
  }

  @Test
  void testS3aSchemeDispatchesToS3Provider() {
    CatalogCredentialManager manager = mockManager(ImmutableSet.of("s3-token"), "s3-token");
    TableMetadata md = mockTableMetadata("s3a://bucket/ns/t/");

    List<Credential> result =
        CatalogWrapperForREST.getCredentials(manager, md, CredentialPrivilege.READ);

    Assertions.assertEquals("s3-token", result.get(0).credentialType());
  }

  @Test
  void testAdlsSchemeDispatchesToAdlsProvider() {
    CatalogCredentialManager manager = mockManager(ImmutableSet.of("adls-token"), "adls-token");
    TableMetadata md = mockTableMetadata("abfs://container@acct.dfs.core.windows.net/t/");

    List<Credential> result =
        CatalogWrapperForREST.getCredentials(manager, md, CredentialPrivilege.READ);

    Assertions.assertEquals("adls-token", result.get(0).credentialType());
  }

  @Test
  void testBlobSchemeDispatchesToAzureProvider() {
    CatalogCredentialManager manager =
        mockManager(ImmutableSet.of("azure-account-key"), "azure-account-key");
    TableMetadata md = mockTableMetadata("wasb://container@acct.blob.core.windows.net/t/");

    List<Credential> result =
        CatalogWrapperForREST.getCredentials(manager, md, CredentialPrivilege.READ);

    Assertions.assertEquals("azure-account-key", result.get(0).credentialType());
  }

  @Test
  void testSecretKeyProviderMatchedByPrefix() {
    // Verify prefix matching works for non-token credential styles too.
    CatalogCredentialManager manager =
        mockManager(ImmutableSet.of("s3-secret-key"), "s3-secret-key");
    TableMetadata md = mockTableMetadata("s3://bucket/t/");

    List<Credential> result =
        CatalogWrapperForREST.getCredentials(manager, md, CredentialPrivilege.READ);

    Assertions.assertEquals("s3-secret-key", result.get(0).credentialType());
  }

  @Test
  void testWritePrivilegePopulatesWritePaths() {
    // Smoke test that WRITE vs READ paths both work - detailed path-content verification
    // is covered by the underlying CatalogCredentialManager tests.
    CatalogCredentialManager manager = mockManager(ImmutableSet.of("s3-token"), "s3-token");
    TableMetadata md = mockTableMetadata("s3://bucket/t/");

    List<Credential> result =
        CatalogWrapperForREST.getCredentials(manager, md, CredentialPrivilege.WRITE);

    Assertions.assertEquals("s3-token", result.get(0).credentialType());
  }

  @Test
  void testPathsSpanningSchemesThrowsIllegalState() {
    CatalogCredentialManager manager =
        mockManager(ImmutableSet.of("s3-token", "gcs-token"), "s3-token");
    // Main location on s3, WRITE_DATA_LOCATION on gcs — pathological cross-scheme configuration.
    TableMetadata md = mockTableMetadata("s3://bucket/t/", "gs://other/t/", "");

    Assertions.assertThrows(
        IllegalStateException.class,
        () -> CatalogWrapperForREST.getCredentials(manager, md, CredentialPrivilege.READ));
  }

  @Test
  void testNoRegisteredProviderForSchemeThrowsServiceUnavailable() {
    // Catalog has only gcs-token, but table is on s3. No matching provider.
    CatalogCredentialManager manager = mockManager(ImmutableSet.of("gcs-token"), "gcs-token");
    TableMetadata md = mockTableMetadata("s3://bucket/t/");

    Assertions.assertThrows(
        ServiceUnavailableException.class,
        () -> CatalogWrapperForREST.getCredentials(manager, md, CredentialPrivilege.READ));
  }

  @Test
  void testUnrecognizedSchemeThrowsServiceUnavailable() {
    CatalogCredentialManager manager = mockManager(ImmutableSet.of("s3-token"), "s3-token");
    TableMetadata md = mockTableMetadata("ftp://server/path/");

    Assertions.assertThrows(
        ServiceUnavailableException.class,
        () -> CatalogWrapperForREST.getCredentials(manager, md, CredentialPrivilege.READ));
  }
}
