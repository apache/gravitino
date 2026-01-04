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
package org.apache.gravitino.utils;

import com.google.common.base.Preconditions;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/** Utility class for fileset-related operations. */
public class FilesetUtil {

  private FilesetUtil() {}

  /**
   * Get user-defined configurations for a given path based on the path's base location
   * (scheme://authority).
   *
   * <p>The logic:
   *
   * <ol>
   *   <li>Extract baseLocation (scheme://authority) from the given path
   *   <li>Find config entries like "fs.path.config.&lt;name&gt; = &lt;base_location&gt;" where the
   *       base_location matches the extracted baseLocation
   *   <li>Extract the name from the matching entry
   *   <li>Then find all config entries with prefix "fs.path.config.&lt;name&gt;." and extract
   *       properties
   * </ol>
   *
   * <p>Example:
   *
   * <pre>
   *   fs.path.config.cluster1 = s3://bucket1
   *   fs.path.config.cluster1.aws-access-key = XXX1
   *   fs.path.config.cluster1.aws-secret-key = XXX2
   *   If path is "s3://bucket1/path/fileset1", then baseLocation is "s3://bucket1",
   *   cluster1 matches and we extract:
   *   - aws-access-key = XXX1
   *   - aws-secret-key = XXX2
   * </pre>
   *
   * @param path the path to extract configurations for
   * @param conf the configuration map containing path-based configurations
   * @param configPrefix the prefix for path-based configurations (e.g., "fs.path.config.")
   * @return a map of user-defined configurations for the given path
   */
  public static Map<String, String> getUserDefinedFileSystemConfigs(
      URI path, Map<String, String> conf, String configPrefix) {
    Preconditions.checkArgument(path != null, "Path should not be null");
    Preconditions.checkArgument(conf != null, "Configuration map should not be null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(configPrefix), "Config prefix should not be blank");

    Map<String, String> properties = new HashMap<>();
    String baseLocation = getBaseLocation(path);
    String locationName = null;

    // First pass: find the location name by matching baseLocation
    // Look for entries like "fs.path.config.<name> = <base_location>"
    // The key format should be exactly "fs.path.config.<name>" (no dot after name)
    for (Map.Entry<String, String> entry : conf.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();

      // Check if this is a location definition: "fs.path.config.<name> = <base_location>"
      // The key format should be exactly "fs.path.config.<name>" (no dot after name)
      if (key.startsWith(configPrefix)) {
        String suffix = key.substring(configPrefix.length());
        // Check if this is a location definition (no dot after the name)
        // Format: "fs.path.config.<name>" (not "fs.path.config.<name>.<property>")
        if (!suffix.contains(".")
            && StringUtils.isNotBlank(suffix)
            && StringUtils.isNotBlank(value)) {
          // This is a location definition: "fs.path.config.<name>"
          // Extract baseLocation from the value and compare with the path's baseLocation
          try {
            URI valueUri = new URI(value);
            String valueBaseLocation = getBaseLocation(valueUri);
            if (baseLocation.equals(valueBaseLocation)) {
              locationName = suffix;
              break;
            }
          } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format("Invalid URI in configuration of key %s: %s", key, value), e);
          }
        }
      }
    }

    // Second pass: extract all properties for the matched location name
    if (locationName != null) {
      String propertyPrefix = configPrefix + locationName + ".";
      for (Map.Entry<String, String> entry : conf.entrySet()) {
        String key = entry.getKey();
        // Check if this key is a property for the matched location
        // e.g., "fs.path.config.cluster1.aws-ak" matches prefix "fs.path.config.cluster1."
        if (key.startsWith(propertyPrefix)) {
          // Extract the property name after the location prefix
          // e.g., "fs.path.config.cluster1.aws-ak" -> "aws-ak"
          String propertyName = key.substring(propertyPrefix.length());
          if (!propertyName.isEmpty()) {
            properties.put(propertyName, entry.getValue());
          }
        }
      }
    }

    return properties;
  }

  /**
   * Extract base location (scheme://authority) from a URI.
   *
   * @param uri the URI to extract base location from
   * @return the base location in format "scheme://authority"
   */
  private static String getBaseLocation(URI uri) {
    Preconditions.checkArgument(uri.getAuthority() != null, "URI must have an authority: %s", uri);
    return uri.getScheme() + "://" + uri.getAuthority();
  }
}
