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
package org.apache.gravitino.hive.client;

import static org.apache.gravitino.catalog.hive.HiveConstants.HIVE_METASTORE_URIS;

import java.net.InetAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {

  private static final Logger LOG = LoggerFactory.getLogger(Util.class);

  public static final String HIVE_CONFIG_RESOURCES = "hive.config.resources";

  public static void updateConfigurationFromProperties(
      Properties properties, Configuration config) {
    try {
      String configResources = properties.getProperty(HIVE_CONFIG_RESOURCES);
      if (StringUtils.isNotBlank(configResources)) {
        for (String resource : configResources.split(",")) {
          resource = resource.trim();
          if (StringUtils.isNotBlank(resource)) {
            config.addResource(new Path(resource));
          }
        }
      }

      properties.forEach((k, v) -> config.set(k.toString(), v.toString()));
      resolveMetastoreUriHosts(config);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create configuration", e);
    }
  }

  // Pre-resolve to IP so Hive's resolveUris() doesn't receive Docker FQDNs with underscores.
  private static void resolveMetastoreUriHosts(Configuration config) {
    String urisValue = config.get(HIVE_METASTORE_URIS);
    if (StringUtils.isBlank(urisValue)) {
      return;
    }
    String resolved =
        Arrays.stream(urisValue.split(","))
            .map(uri -> resolveUriHost(uri.trim()))
            .collect(Collectors.joining(","));
    config.set(HIVE_METASTORE_URIS, resolved);
  }

  private static String resolveUriHost(String uriStr) {
    try {
      URI uri = new URI(uriStr);
      String host = uri.getHost();
      if (StringUtils.isBlank(host)) {
        return uriStr;
      }
      String ip = InetAddress.getByName(host).getHostAddress();
      return new URI(
              uri.getScheme(),
              uri.getUserInfo(),
              ip,
              uri.getPort(),
              uri.getPath(),
              uri.getQuery(),
              uri.getFragment())
          .toString();
    } catch (Exception e) {
      LOG.warn("Failed to resolve metastore URI host for '{}', using original", uriStr, e);
      return uriStr;
    }
  }
}
