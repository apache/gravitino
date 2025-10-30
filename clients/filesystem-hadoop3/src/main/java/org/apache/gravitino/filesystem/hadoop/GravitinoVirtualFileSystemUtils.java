/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.filesystem.hadoop;

import static org.apache.gravitino.client.GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX;
import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_CONFIG_PREFIX;
import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_KERBEROS_PREFIX;
import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_PREFIX;
import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_REQUEST_HEADER_PREFIX;
import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration.NOT_GRAVITINO_CLIENT_CONFIG_LIST;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.DefaultOAuth2TokenProvider;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.KerberosTokenProvider;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.gravitino.storage.GCSProperties;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

/** Utility class for Gravitino Virtual File System. */
public class GravitinoVirtualFileSystemUtils {

  // The pattern is used to match gvfs path. The scheme prefix (gvfs://fileset) is optional.
  // The following path can be match:
  //     gvfs://fileset/fileset_catalog/fileset_schema/fileset1/file.txt
  //     /fileset_catalog/fileset_schema/fileset1/sub_dir/
  private static final Pattern IDENTIFIER_PATTERN =
      Pattern.compile("^(?:gvfs://fileset)?/([^/]+)/([^/]+)/([^/]+)(?>/[^/]+)*/?$");

  /**
   * Transform the Hadoop configuration to a map.
   *
   * @param configuration The Hadoop configuration.
   * @return The configuration map.
   */
  public static Map<String, String> getConfigMap(Configuration configuration) {
    Map<String, String> maps = Maps.newHashMap();
    // Don't use entry.getKey() directly in the lambda, because it cannot
    // handle variable expansion in the Configuration values.
    configuration.forEach(entry -> maps.put(entry.getKey(), configuration.get(entry.getKey())));
    return maps;
  }

  /**
   * Get Gravitino client by the configuration.
   *
   * @param configuration The configuration for the Gravitino client.
   * @return The Gravitino client.
   */
  public static GravitinoClient createClient(Configuration configuration) {
    return createClient(getConfigMap(configuration));
  }

  /**
   * Get Gravitino client by the configuration.
   *
   * @param configuration The configuration for the Gravitino client.
   * @return The Gravitino client.
   */
  public static GravitinoClient createClient(Map<String, String> configuration) {
    // initialize the Gravitino client
    String serverUri =
        configuration.get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY);
    String metalakeValue =
        configuration.get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(serverUri),
        "'%s' is not set in the configuration",
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY);

    Map<String, String> requestHeaders =
        configuration.entrySet().stream()
            .filter(e -> e.getKey().startsWith(FS_GRAVITINO_CLIENT_REQUEST_HEADER_PREFIX))
            .collect(
                Collectors.toMap(
                    e -> e.getKey().substring(FS_GRAVITINO_CLIENT_REQUEST_HEADER_PREFIX.length()),
                    Map.Entry::getValue));

    Map<String, String> clientConfig = extractClientConfig(configuration);

    String authType =
        configuration.getOrDefault(
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
            GravitinoVirtualFileSystemConfiguration.SIMPLE_AUTH_TYPE);
    if (authType.equalsIgnoreCase(GravitinoVirtualFileSystemConfiguration.SIMPLE_AUTH_TYPE)) {
      return GravitinoClient.builder(serverUri)
          .withMetalake(metalakeValue)
          .withSimpleAuth()
          .withHeaders(requestHeaders)
          .withClientConfig(clientConfig)
          .build();
    } else if (authType.equalsIgnoreCase(
        GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE)) {
      String authServerUri =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SERVER_URI_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SERVER_URI_KEY,
          authServerUri);

      String credential =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_CREDENTIAL_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_CREDENTIAL_KEY,
          credential);

      String path =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_PATH_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_PATH_KEY,
          path);

      String scope =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SCOPE_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SCOPE_KEY,
          scope);

      DefaultOAuth2TokenProvider authDataProvider =
          DefaultOAuth2TokenProvider.builder()
              .withUri(authServerUri)
              .withCredential(credential)
              .withPath(path)
              .withScope(scope)
              .build();

      return GravitinoClient.builder(serverUri)
          .withMetalake(metalakeValue)
          .withOAuth(authDataProvider)
          .withHeaders(requestHeaders)
          .withClientConfig(clientConfig)
          .build();
    } else if (authType.equalsIgnoreCase(
        GravitinoVirtualFileSystemConfiguration.KERBEROS_AUTH_TYPE)) {
      String principal =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_KERBEROS_PRINCIPAL_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.KERBEROS_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_KERBEROS_PRINCIPAL_KEY,
          principal);
      String keytabFilePath =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration
                  .FS_GRAVITINO_CLIENT_KERBEROS_KEYTAB_FILE_PATH_KEY);
      KerberosTokenProvider authDataProvider;
      if (StringUtils.isNotBlank(keytabFilePath)) {
        // Using principal and keytab to create auth provider
        authDataProvider =
            KerberosTokenProvider.builder()
                .withClientPrincipal(principal)
                .withKeyTabFile(new File(keytabFilePath))
                .build();
      } else {
        // Using ticket cache to create auth provider
        authDataProvider = KerberosTokenProvider.builder().withClientPrincipal(principal).build();
      }

      return GravitinoClient.builder(serverUri)
          .withMetalake(metalakeValue)
          .withKerberosAuth(authDataProvider)
          .withHeaders(requestHeaders)
          .withClientConfig(clientConfig)
          .build();
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported authentication type: %s for %s.",
              authType, GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY));
    }
  }

  @VisibleForTesting
  static Map<String, String> extractClientConfig(Map<String, String> configuration) {
    return configuration.entrySet().stream()
        .filter(e -> isClientConfigKey(e.getKey()))
        .collect(
            Collectors.toMap(
                e ->
                    e.getKey()
                        .replace(FS_GRAVITINO_CLIENT_CONFIG_PREFIX, GRAVITINO_CLIENT_CONFIG_PREFIX),
                Map.Entry::getValue,
                (oldVal, newVal) -> newVal));
  }

  private static boolean isClientConfigKey(String key) {
    return key.startsWith(FS_GRAVITINO_CLIENT_CONFIG_PREFIX)
        && !key.startsWith(FS_GRAVITINO_CLIENT_OAUTH2_PREFIX)
        && !key.startsWith(FS_GRAVITINO_CLIENT_KERBEROS_PREFIX)
        && !key.startsWith(FS_GRAVITINO_CLIENT_REQUEST_HEADER_PREFIX)
        && !NOT_GRAVITINO_CLIENT_CONFIG_LIST.contains(key);
  }

  /**
   * Extract the full identifier of the fileset from the virtual path.
   *
   * @param metalakeName The metalake name of the fileset.
   * @param gvfsPath The virtual path, format is
   *     [gvfs://fileset]/{fileset_catalog}/{fileset_schema}/{fileset_name}[/sub_path]
   * @return The full identifier of the fileset.
   * @throws IllegalArgumentException If the URI doesn't contain a valid identifier.
   */
  public static NameIdentifier extractIdentifier(String metalakeName, String gvfsPath)
      throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalakeName), "The metalake name cannot be null or empty.");

    Preconditions.checkArgument(
        StringUtils.isNotBlank(gvfsPath),
        "The gvfsPath which need be extracted cannot be null or empty.");

    Matcher matcher = IDENTIFIER_PATTERN.matcher(gvfsPath);
    Preconditions.checkArgument(
        matcher.matches() && matcher.groupCount() == 3,
        "URI %s doesn't contains valid identifier",
        gvfsPath);

    return NameIdentifier.of(metalakeName, matcher.group(1), matcher.group(2), matcher.group(3));
  }

  /**
   * Get the sub path from the virtual path.
   *
   * @param identifier The identifier of the fileset.
   * @param gvfsPath The virtual path, format is
   *     [gvfs://fileset]/{fileset_catalog}/{fileset_schema}/{fileset_name}[/sub_path]
   * @return The sub path.
   * @throws IllegalArgumentException If the virtual path doesn't match the identifier.
   */
  public static String getSubPathFromGvfsPath(NameIdentifier identifier, String gvfsPath)
      throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(gvfsPath), "Virtual path cannot be null or empty.");

    Preconditions.checkArgument(
        identifier.namespace().levels().length == 3,
        "The identifier should have 3 levels, but got %s",
        identifier.namespace().levels().length);

    Preconditions.checkArgument(
        StringUtils.isNotBlank(identifier.name()),
        "The identifier name should not be null or empty.");

    Preconditions.checkArgument(
        gvfsPath.startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX)
            || gvfsPath.startsWith("/"),
        "Virtual path should start with '%s' or '/'",
        GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX);

    String prefix =
        gvfsPath.startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX)
            ? String.format(
                "%s/%s/%s/%s",
                GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                identifier.namespace().level(1),
                identifier.namespace().level(2),
                identifier.name())
            : String.format(
                "/%s/%s/%s",
                identifier.namespace().level(1),
                identifier.namespace().level(2),
                identifier.name());

    Preconditions.checkArgument(
        gvfsPath.startsWith(prefix),
        "Virtual path '%s' doesn't match fileset identifier '%s'",
        gvfsPath,
        identifier);

    return gvfsPath.substring(prefix.length());
  }

  private static void checkAuthConfig(String authType, String configKey, String configValue) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(configValue),
        "%s should not be null if %s is set to %s.",
        configKey,
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
        authType);
  }

  /**
   * Create a secure hash fingerprint of credentials from the properties map.
   *
   * <p>This method extracts credential-related properties (access keys, secrets, tokens, etc.)
   * based on the storage scheme and creates a SHA-256 hash to uniquely identify the credential set.
   * This ensures that different credentials result in different cache entries, preventing
   * credential leakage between users.
   *
   * @param allProperties the properties map containing credentials
   * @param scheme the storage scheme (e.g., "s3a", "oss", "gs", "abfss", "hdfs")
   * @param ugi the UserGroupInformation for fallback when credentials are not available
   * @return a SHA-256 hex string fingerprint of the credentials
   */
  @VisibleForTesting
  public static String createCredentialFingerprint(
      Map<String, String> allProperties, String scheme, UserGroupInformation ugi) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      StringBuilder credentialContent = new StringBuilder();

      if ("s3a".equals(scheme)) {
        // S3 credentials: access key + secret key
        // Check both Hadoop format (fs.s3a.access.key) and Gravitino format (s3-access-key-id)
        String accessKey =
            allProperties.get("fs.s3a.access.key") != null
                ? allProperties.get("fs.s3a.access.key")
                : allProperties.get(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID);
        String secretKey =
            allProperties.get("fs.s3a.secret.key") != null
                ? allProperties.get("fs.s3a.secret.key")
                : allProperties.get(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY);
        String credProvider = allProperties.get("fs.s3a.aws.credentials.provider");
        if (accessKey != null && secretKey != null) {
          credentialContent.append(accessKey).append(":").append(secretKey);
        } else if (credProvider != null) {
          // Use credential provider class name as part of fingerprint
          credentialContent.append(credProvider);
        } else {
          // No explicit credentials, use UGI as fallback
          credentialContent.append(ugi.getShortUserName());
        }
      } else if ("oss".equals(scheme)) {
        // OSS credentials: access key id + access key secret
        // Check both Hadoop format and Gravitino format
        String accessKeyId =
            allProperties.get("fs.oss.access.key.id") != null
                ? allProperties.get("fs.oss.access.key.id")
                : allProperties.get(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID);
        String accessKeySecret =
            allProperties.get("fs.oss.access.key.secret") != null
                ? allProperties.get("fs.oss.access.key.secret")
                : allProperties.get(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET);
        String credProvider = allProperties.get("fs.oss.credentials.provider");
        if (accessKeyId != null && accessKeySecret != null) {
          credentialContent.append(accessKeyId).append(":").append(accessKeySecret);
        } else if (credProvider != null) {
          credentialContent.append(credProvider);
        } else {
          credentialContent.append(ugi.getShortUserName());
        }
      } else if ("gs".equals(scheme)) {
        // GCS credentials: service account key file path
        String keyFile =
            allProperties.get("fs.gs.auth.service.account.json.keyfile") != null
                ? allProperties.get("fs.gs.auth.service.account.json.keyfile")
                : allProperties.get(GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE);
        String tokenProvider = allProperties.get("fs.gs.auth.access.token.provider.impl");
        if (keyFile != null) {
          credentialContent.append(keyFile);
        } else if (tokenProvider != null) {
          credentialContent.append(tokenProvider);
        } else {
          credentialContent.append(ugi.getShortUserName());
        }
      } else if ("abfss".equals(scheme)) {
        // Azure Blob Storage credentials: account name + account key
        String accountName =
            allProperties.get(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME);
        String accountKey = allProperties.get(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY);
        // Also check for the Hadoop-formatted key
        if (accountName != null && accountKey == null) {
          String hadoopKey =
              allProperties.get("fs.azure.account.key." + accountName + ".dfs.core.windows.net");
          if (hadoopKey != null) {
            accountKey = hadoopKey;
          }
        }
        if (accountName != null && accountKey != null) {
          credentialContent.append(accountName).append(":").append(accountKey);
        } else {
          credentialContent.append(ugi.getShortUserName());
        }
      } else {
        // HDFS, Local, and other filesystems: use UGI as fingerprint
        // For HDFS, credentials are typically provided via environment/Kerberos
        credentialContent.append(ugi.getShortUserName());
      }

      // Hash the credential content
      digest.update(credentialContent.toString().getBytes(StandardCharsets.UTF_8));
      byte[] hashBytes = digest.digest();
      StringBuilder hexString = new StringBuilder();
      for (byte b : hashBytes) {
        hexString.append(String.format("%02x", b));
      }
      return hexString.toString();
    } catch (NoSuchAlgorithmException e) {
      // Fallback to UGI if SHA-256 is not available (should never happen)
      return ugi.getShortUserName();
    }
  }
}
