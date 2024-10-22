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

package org.apache.gravitino.gcs.credential;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.CredentialAccessBoundary;
import com.google.auth.oauth2.DownscopedCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.CredentialContext;
import org.apache.gravitino.credential.CredentialProvider;
import org.apache.gravitino.credential.GcsTokenCredential;
import org.apache.gravitino.credential.PathBasedCredentialContext;
import org.apache.gravitino.credential.config.GCSCredentialConfig;

// Refer from org/apache/polaris/core/storage/gcp/GcpCredentialsStorageIntegration
public class GcsTokenProvider implements CredentialProvider {

  private GoogleCredentials sourceCredentials;
  private static final String INITIAL_SCOPE = "https://www.googleapis.com/auth/cloud-platform";

  @Override
  public void initialize(Map<String, String> properties) {
    GCSCredentialConfig gcsCredentialConfig = new GCSCredentialConfig(properties);
    try {
      this.sourceCredentials =
          getSourceCredentials(gcsCredentialConfig).createScoped(INITIAL_SCOPE);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {}

  @Override
  public String credentialType() {
    return CredentialConstants.GCS_TOKEN_CREDENTIAL_TYPE;
  }

  @Override
  public Credential getCredential(CredentialContext context) {
    if (!(context instanceof PathBasedCredentialContext)) {
      return null;
    }
    PathBasedCredentialContext pathBasedCredentialContext = (PathBasedCredentialContext) context;
    try {
      AccessToken accessToken =
          getToken(
              pathBasedCredentialContext.getReadPaths(),
              pathBasedCredentialContext.getWritePaths());
      String tokenValue = accessToken.getTokenValue();
      long expireTime = accessToken.getExpirationTime().toInstant().toEpochMilli();
      return new GcsTokenCredential(tokenValue, expireTime);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private AccessToken getToken(Set<String> readLocations, Set<String> writeLocations)
      throws IOException {
    DownscopedCredentials downscopedCredentials =
        DownscopedCredentials.newBuilder()
            .setSourceCredential(sourceCredentials)
            .setCredentialAccessBoundary(getAccessBoundary(readLocations, writeLocations))
            .build();

    return downscopedCredentials.refreshAccessToken();
  }

  private CredentialAccessBoundary getAccessBoundary(
      Set<String> readLocations, Set<String> writeLocations) {
    Map<String, List<String>> readConditionsMap = new HashMap<>();
    Map<String, List<String>> writeConditionsMap = new HashMap<>();

    HashSet<String> readBuckets = new HashSet<>();
    HashSet<String> writeBuckets = new HashSet<>();
    Stream.concat(readLocations.stream(), writeLocations.stream())
        .distinct()
        .forEach(
            location -> {
              URI uri = URI.create(location);
              String bucket = getBucket(uri);
              readBuckets.add(bucket);
              String path = uri.getPath().substring(1);
              List<String> resourceExpressions =
                  readConditionsMap.computeIfAbsent(bucket, key -> new ArrayList<>());
              // add read privilege
              resourceExpressions.add(
                  String.format(
                      "resource.name.startsWith('projects/_/buckets/%s/objects/%s')",
                      bucket, path));
              // add list privilege
              resourceExpressions.add(
                  String.format(
                      "api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('%s')",
                      path));
              if (writeLocations.contains(location)) {
                writeBuckets.add(bucket);
                List<String> writeExpressions =
                    writeConditionsMap.computeIfAbsent(bucket, key -> new ArrayList<>());
                writeExpressions.add(
                    String.format(
                        "resource.name.startsWith('projects/_/buckets/%s/objects/%s')",
                        bucket, path));
              }
            });
    CredentialAccessBoundary.Builder accessBoundaryBuilder = CredentialAccessBoundary.newBuilder();
    readBuckets.forEach(
        bucket -> {
          List<String> readConditions = readConditionsMap.get(bucket);
          if (readConditions == null || readConditions.isEmpty()) {
            return;
          }
          CredentialAccessBoundary.AccessBoundaryRule.Builder builder =
              CredentialAccessBoundary.AccessBoundaryRule.newBuilder();
          builder.setAvailableResource(bucketResource(bucket));
          builder.setAvailabilityCondition(
              CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition.newBuilder()
                  .setExpression(String.join(" || ", readConditions))
                  .build());
          builder.setAvailablePermissions(Arrays.asList("inRole:roles/storage.legacyObjectReader"));
          builder.addAvailablePermission("inRole:roles/storage.objectViewer");
          accessBoundaryBuilder.addRule(builder.build());
        });
    writeBuckets.forEach(
        bucket -> {
          List<String> writeConditions = writeConditionsMap.get(bucket);
          if (writeConditions == null || writeConditions.isEmpty()) {
            return;
          }
          CredentialAccessBoundary.AccessBoundaryRule.Builder builder =
              CredentialAccessBoundary.AccessBoundaryRule.newBuilder();
          builder.setAvailableResource(bucketResource(bucket));
          builder.setAvailabilityCondition(
              CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition.newBuilder()
                  .setExpression(String.join(" || ", writeConditions))
                  .build());
          builder.setAvailablePermissions(Arrays.asList("inRole:roles/storage.legacyBucketWriter"));
          accessBoundaryBuilder.addRule(builder.build());
        });
    return accessBoundaryBuilder.build();
  }

  private static String bucketResource(String bucket) {
    return "//storage.googleapis.com/projects/_/buckets/" + bucket;
  }

  private static String getBucket(URI uri) {
    return uri.getHost();
  }

  private GoogleCredentials getSourceCredentials(GCSCredentialConfig gcsCredentialConfig)
      throws IOException {
    String gcsCredentialFilePath = gcsCredentialConfig.gcsCredentialFilePath();
    if (StringUtils.isBlank(gcsCredentialFilePath)) {
      return GoogleCredentials.getApplicationDefault();
    } else {
      File credentialsFile = new File(gcsCredentialFilePath);
      if (!credentialsFile.exists()) {
        throw new IOException("Gcs credential file does not exist." + gcsCredentialFilePath);
      }
      return GoogleCredentials.fromStream(new FileInputStream(credentialsFile));
    }
  }
}
