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
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.DefaultOAuth2TokenProvider;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.KerberosTokenProvider;
import org.apache.hadoop.conf.Configuration;

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
}
