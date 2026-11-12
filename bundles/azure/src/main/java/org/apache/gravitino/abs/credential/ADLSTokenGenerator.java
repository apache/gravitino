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
package org.apache.gravitino.abs.credential;

import com.azure.core.util.Context;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.implementation.util.DataLakeSasImplUtil;
import com.azure.storage.file.datalake.models.UserDelegationKey;
import com.azure.storage.file.datalake.sas.DataLakeServiceSasSignatureValues;
import com.azure.storage.file.datalake.sas.PathSasPermission;
import java.io.IOException;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.gravitino.credential.ADLSTokenCredential;
import org.apache.gravitino.credential.CredentialContext;
import org.apache.gravitino.credential.CredentialGenerator;
import org.apache.gravitino.credential.PathBasedCredentialContext;
import org.apache.gravitino.credential.config.AzureCredentialConfig;

/** Generates ADLS token to access ADLS data. */
public class ADLSTokenGenerator implements CredentialGenerator<ADLSTokenCredential> {

  private String storageAccountName;
  private String tenantId;
  private String clientId;
  private String clientSecret;
  private String endpoint;
  private Integer tokenExpireSecs;

  @Override
  public void initialize(Map<String, String> properties) {
    AzureCredentialConfig azureCredentialConfig = new AzureCredentialConfig(properties);
    this.storageAccountName = azureCredentialConfig.storageAccountName();
    this.tenantId = azureCredentialConfig.tenantId();
    this.clientId = azureCredentialConfig.clientId();
    this.clientSecret = azureCredentialConfig.clientSecret();
    this.endpoint =
        String.format("https://%s.%s", storageAccountName, ADLSTokenCredential.ADLS_DOMAIN);
    this.tokenExpireSecs = azureCredentialConfig.adlsTokenExpireInSecs();
  }

  @Override
  public ADLSTokenCredential generate(CredentialContext context) {
    if (!(context instanceof PathBasedCredentialContext)) {
      return null;
    }

    PathBasedCredentialContext pathContext = (PathBasedCredentialContext) context;

    Set<String> writePaths = pathContext.getWritePaths();
    Set<String> readPaths = pathContext.getReadPaths();
    Set<String> combinedPaths = new TreeSet<>(writePaths);
    combinedPaths.addAll(readPaths);

    ADLSLocationUtils.ADLSLocationParts locationParts = findCredentialLocation(combinedPaths);

    OffsetDateTime start = OffsetDateTime.now();
    OffsetDateTime expiry = OffsetDateTime.now().plusSeconds(tokenExpireSecs);
    UserDelegationKey userDelegationKey = getUserDelegationKey(start, expiry);

    PathSasPermission pathSasPermission =
        new PathSasPermission().setReadPermission(true).setListPermission(true);
    if (!writePaths.isEmpty()) {
      pathSasPermission
          .setWritePermission(true)
          .setDeletePermission(true)
          .setCreatePermission(true)
          .setAddPermission(true);
    }

    DataLakeServiceSasSignatureValues signatureValues =
        new DataLakeServiceSasSignatureValues(expiry, pathSasPermission);
    String sasToken =
        new DataLakeSasImplUtil(
                signatureValues,
                locationParts.getContainer(),
                ADLSLocationUtils.trimSlashes(locationParts.getPath()),
                true)
            .generateUserDelegationSas(
                userDelegationKey, locationParts.getAccountName(), Context.NONE);

    return new ADLSTokenCredential(
        locationParts.getAccountName(), sasToken, expiry.toInstant().toEpochMilli());
  }

  UserDelegationKey getUserDelegationKey(OffsetDateTime start, OffsetDateTime expiry) {
    ClientSecretCredential clientSecretCredential =
        new ClientSecretCredentialBuilder()
            .tenantId(tenantId)
            .clientId(clientId)
            .clientSecret(clientSecret)
            .build();

    DataLakeServiceClient dataLakeServiceClient =
        new DataLakeServiceClientBuilder()
            .endpoint(endpoint)
            .credential(clientSecretCredential)
            .buildClient();
    return dataLakeServiceClient.getUserDelegationKey(start, expiry);
  }

  private ADLSLocationUtils.ADLSLocationParts findCredentialLocation(Set<String> paths) {
    if (paths.isEmpty()) {
      throw new IllegalArgumentException(
          "ADLS credential context should contain at least one path");
    }

    List<ADLSLocationUtils.ADLSLocationParts> parsedLocations =
        paths.stream().map(ADLSTokenGenerator::parseSupportedLocation).collect(Collectors.toList());
    ADLSLocationUtils.ADLSLocationParts first = parsedLocations.get(0);
    boolean hasDifferentScope =
        parsedLocations.stream()
            .anyMatch(
                location ->
                    !storageAccountName.equalsIgnoreCase(location.getAccountName())
                        || !first.getAccountName().equalsIgnoreCase(location.getAccountName())
                        || !first.getContainer().equalsIgnoreCase(location.getContainer()));
    if (hasDifferentScope) {
      throw new IllegalArgumentException(
          "ADLS paths must belong to the configured storage account and a single container, but found: "
              + paths);
    }

    Map<String, ADLSLocationUtils.ADLSLocationParts> uniqueLocations = new LinkedHashMap<>();
    parsedLocations.forEach(
        location ->
            uniqueLocations.putIfAbsent(
                location.getContainer() + "\0" + ADLSLocationUtils.trimSlashes(location.getPath()),
                location));
    List<ADLSLocationUtils.ADLSLocationParts> locations = new ArrayList<>(uniqueLocations.values());

    List<ADLSLocationUtils.ADLSLocationParts> rootLocations =
        locations.stream()
            .filter(
                location ->
                    locations.stream()
                        .noneMatch(
                            possibleParent ->
                                possibleParent != location
                                    && isDescendantOf(location, possibleParent)))
            .collect(Collectors.toList());
    if (rootLocations.size() != 1) {
      throw new IllegalArgumentException(
          "ADLS credential paths must resolve to exactly one root location, but found: "
              + rootLocations.size()
              + " roots in paths: "
              + paths);
    }

    return rootLocations.get(0);
  }

  private static ADLSLocationUtils.ADLSLocationParts parseSupportedLocation(String path) {
    URI uri = URI.create(path);
    if (!"abfs".equalsIgnoreCase(uri.getScheme()) && !"abfss".equalsIgnoreCase(uri.getScheme())) {
      throw new IllegalArgumentException(
          "adls-token requires an abfs or abfss location; unsupported location: " + path);
    }
    ADLSLocationUtils.ADLSLocationParts location = ADLSLocationUtils.parseLocation(path);
    if (!(location.getAccountName() + "." + ADLSTokenCredential.ADLS_DOMAIN)
        .equalsIgnoreCase(uri.getHost())) {
      throw new IllegalArgumentException("Invalid ADLS storage account in location: " + path);
    }
    return location;
  }

  private static boolean isDescendantOf(
      ADLSLocationUtils.ADLSLocationParts location,
      ADLSLocationUtils.ADLSLocationParts possibleParent) {
    String path = ADLSLocationUtils.trimSlashes(location.getPath());
    String parentPath = ADLSLocationUtils.trimSlashes(possibleParent.getPath());
    if (parentPath.isEmpty()) {
      return !path.isEmpty();
    }
    return path.startsWith(parentPath + "/");
  }

  @Override
  public void close() throws IOException {}
}
