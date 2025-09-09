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

package org.apache.gravitino.utils;

import com.google.common.base.Preconditions;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;

/**
 * Utility class for validating JDBC URLs and configurations. It checks for unsafe parameters
 * specific to MySQL, MariaDB, and PostgreSQL JDBC URLs. Unsafe parameters can lead to security
 * vulnerabilities or unexpected behavior, so this utility helps ensure that JDBC configurations are
 * safe to use.
 */
public class JdbcUrlUtils {

  // Unsafe parameters for MySQL and MariaDB, other parameters like
  // trustCertificateKeyStoreUrl, serverTimezone, characterEncoding,
  // useSSL are also risky, please use it with caution.
  private static final List<String> UNSAFE_MYSQL_PARAMETERS =
      Arrays.asList(
          "maxAllowedPacket",
          "autoDeserialize",
          "queryInterceptors",
          "statementInterceptors",
          "detectCustomCollations",
          "allowloadlocalinfile",
          "allowUrlInLocalInfile",
          "allowLoadLocalInfileInPath");

  private static final List<String> UNSAFE_POSTGRES_PARAMETERS =
      Arrays.asList(
          "socketFactory",
          "socketFactoryArg",
          "sslfactory",
          "sslhostnameverifier",
          "sslpasswordcallback",
          "authenticationPluginClassName");

  private JdbcUrlUtils() {
    // Utility class, no instantiation allowed
  }

  /**
   * Validates the JDBC configuration by checking the driver and URL for unsafe parameters.
   *
   * @param driver the JDBC driver class name
   * @param url the JDBC URL
   * @param all the JDBC configuration properties
   */
  public static void validateJdbcConfig(String driver, String url, Map<String, String> all) {
    Preconditions.checkArgument(StringUtils.isNotBlank(driver), "Driver class name can't be blank");
    Preconditions.checkArgument(StringUtils.isNotBlank(url), "JDBC URL can't be blank");

    String lowerUrl = url.toLowerCase();
    String decodedUrl = recursiveDecode(lowerUrl);

    // As H2 is only used for testing, we do not check unsafe parameters for H2.
    if (decodedUrl.startsWith("jdbc:mysql")) {
      checkUnsafeParameters(decodedUrl, all, UNSAFE_MYSQL_PARAMETERS, "MySQL");
    } else if (decodedUrl.startsWith("jdbc:mariadb")) {
      checkUnsafeParameters(decodedUrl, all, UNSAFE_MYSQL_PARAMETERS, "MariaDB");
    } else if (decodedUrl.startsWith("jdbc:postgresql")) {
      checkUnsafeParameters(decodedUrl, all, UNSAFE_POSTGRES_PARAMETERS, "PostgreSQL");
    }
  }

  private static void checkUnsafeParameters(
      String url, Map<String, String> config, List<String> unsafeParams, String dbType) {

    String lowerUrl = url.toLowerCase();

    for (String param : unsafeParams) {
      String lowerParam = param.toLowerCase();
      if (lowerUrl.contains(lowerParam) || containsValueIgnoreCase(config, param)) {
        throw new GravitinoRuntimeException(
            "Unsafe %s parameter '%s' detected in JDBC URL", dbType, param);
      }
    }
  }

  private static String recursiveDecode(String url) {
    String prev;
    String decoded = url;
    int max = 5;

    do {
      prev = decoded;
      try {
        decoded = URLDecoder.decode(prev, "UTF-8");
      } catch (Exception e) {
        throw new GravitinoRuntimeException("Unable to decode JDBC URL");
      }
    } while (!prev.equals(decoded) && --max > 0);

    return decoded;
  }

  private static boolean containsValueIgnoreCase(Map<String, String> map, String value) {
    for (String keyValue : map.values()) {
      if (keyValue != null && keyValue.equalsIgnoreCase(value)) {
        return true;
      }
    }
    return false;
  }
}
