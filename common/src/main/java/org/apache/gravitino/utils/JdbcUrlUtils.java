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
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;

/**
 * Utility class for validating JDBC URLs and configurations. It checks for unsafe parameters
 * specific to MySQL, MariaDB, and PostgreSQL JDBC URLs. Unsafe parameters can lead to security
 * vulnerabilities or unexpected behavior, so this utility helps ensure that JDBC configurations are
 * safe to use.
 */
public class JdbcUrlUtils {

  // The DBCP2 connection-pool property whose value is a list of connection properties forwarded
  // verbatim to the JDBC driver. Unsafe parameters smuggled here would otherwise evade a check
  // that only inspects the URL string and the raw config values.
  private static final String CONNECTION_PROPERTIES_KEY = "connectionProperties";

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

    String lowerUrl = url.toLowerCase(Locale.ROOT);
    List<String> decodedForms = decodedFormsForScan(lowerUrl);

    if (anyFormStartsWith(decodedForms, "jdbc:mysql")) {
      checkUnsafeParameters(decodedForms, all, UNSAFE_MYSQL_PARAMETERS, "MySQL");
    } else if (anyFormStartsWith(decodedForms, "jdbc:mariadb")) {
      checkUnsafeParameters(decodedForms, all, UNSAFE_MYSQL_PARAMETERS, "MariaDB");
    } else if (anyFormStartsWith(decodedForms, "jdbc:postgresql")) {
      checkUnsafeParameters(decodedForms, all, UNSAFE_POSTGRES_PARAMETERS, "PostgreSQL");
    }
  }

  /**
   * Returns the decoded forms of a JDBC URL that unsafe-parameter scans must cover. Drivers such as
   * MySQL Connector/J decode query tokens independently and ignore the URL fragment, so a malformed
   * percent escape in one part of the URL must not stop the scan from revealing parameter names
   * hidden behind valid encodings in another part. The returned forms are the URL decoded until the
   * first undecodable escape, plus the fully decoded form of the URL with malformed escapes treated
   * as literal '{@code %}' characters.
   *
   * @param url the JDBC URL, already lower-cased by the caller.
   * @return the candidate decoded forms, never empty.
   */
  public static List<String> decodedFormsForScan(String url) {
    // Percent-decoding can reintroduce upper-case characters (e.g. "%4a" -> 'J'), so the
    // returned forms are lower-cased for substring and prefix matching.
    String stoppedAtMalformed = recursiveDecode(url).toLowerCase(Locale.ROOT);
    String fullyDecoded =
        recursiveDecode(sanitizeMalformedPercentEscapes(url)).toLowerCase(Locale.ROOT);
    if (fullyDecoded.equals(stoppedAtMalformed)) {
      return Collections.singletonList(stoppedAtMalformed);
    }
    return Arrays.asList(stoppedAtMalformed, fullyDecoded);
  }

  /**
   * Replaces every '{@code %}' that is not followed by two hex digits with the escape for a literal
   * '{@code %}', making the URL decodable by {@link URLDecoder}.
   */
  private static String sanitizeMalformedPercentEscapes(String url) {
    return url.replaceAll("%(?![0-9a-f]{2})", "%25");
  }

  private static boolean anyFormStartsWith(List<String> forms, String prefix) {
    return forms.stream().anyMatch(form -> form.startsWith(prefix));
  }

  private static void checkUnsafeParameters(
      List<String> decodedForms,
      Map<String, String> config,
      List<String> unsafeParams,
      String dbType) {

    // Parameter names that reach the JDBC driver through the config map: the config keys
    // themselves (defense in depth) plus any names embedded in the DBCP2 "connectionProperties"
    // value, which is forwarded verbatim to the driver.
    Set<String> configParamNames = collectConfigParameterNames(config);

    for (String param : unsafeParams) {
      String lowerParam = param.toLowerCase(Locale.ROOT);
      boolean inUrl = decodedForms.stream().anyMatch(form -> form.contains(lowerParam));
      if (inUrl || configParamNames.contains(lowerParam)) {
        throw new GravitinoRuntimeException(
            "Unsafe %s parameter '%s' detected in JDBC configuration", dbType, param);
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
      } catch (UnsupportedEncodingException e) {
        // UTF-8 is guaranteed to be supported by the JVM specification.
        throw new RuntimeException(e);
      } catch (IllegalArgumentException e) {
        // The URL contains a literal '%' that is not part of a valid escape (e.g. a password
        // like "100%"). JDBC URLs are not required to be validly percent-encoded, so keep the
        // last fully decoded form instead of rejecting the URL: any parameter name hidden
        // behind valid encodings was already revealed by the previous passes, and a malformed
        // escape cannot additionally hide a readable name from this check.
        return prev;
      }
    } while (!prev.equals(decoded) && --max > 0);

    return decoded;
  }

  /**
   * Collects, in lower case, every parameter name that the JDBC driver could observe via the
   * configuration map. This includes each config key and each name embedded in the DBCP2 {@code
   * connectionProperties} value.
   */
  private static Set<String> collectConfigParameterNames(Map<String, String> config) {
    Set<String> names = new HashSet<>();
    if (config == null) {
      return names;
    }

    for (Map.Entry<String, String> entry : config.entrySet()) {
      String key = entry.getKey();
      if (key == null) {
        continue;
      }
      names.add(key.toLowerCase(Locale.ROOT));

      // DBCP2's BasicDataSourceFactory forwards every name in the "connectionProperties" value to
      // the driver. It parses that value by replacing ';' with '\n' and calling Properties.load,
      // so we parse it identically here to avoid a parser-differential bypass (e.g. names split by
      // embedded newlines or hidden behind '\' / '\\uXXXX' escapes that Properties.load resolves).
      if (CONNECTION_PROPERTIES_KEY.equalsIgnoreCase(key)) {
        for (String name : parseConnectionPropertyNames(entry.getValue())) {
          names.add(name.toLowerCase(Locale.ROOT));
        }
      }
    }
    return names;
  }

  /**
   * Parses the property names out of a DBCP2 {@code connectionProperties} value using the same
   * semantics DBCP2 itself uses: replace {@code ';'} with a newline and load as a {@link
   * Properties} document.
   */
  private static Set<String> parseConnectionPropertyNames(String value) {
    if (value == null) {
      return Collections.emptySet();
    }
    Properties properties = new Properties();
    try {
      properties.load(new StringReader(value.replace(';', '\n')));
    } catch (IOException | IllegalArgumentException e) {
      // A malformed value cannot be parsed into names to inspect; reject it defensively rather
      // than letting an un-inspectable value reach the driver. (DBCP2 parses it the same way and
      // would fail too.) The cause is chained for server-side diagnostics but kept out of the
      // message to avoid leaking internals to clients.
      throw new GravitinoRuntimeException(e, "Unable to parse JDBC connectionProperties");
    }
    return properties.stringPropertyNames();
  }
}
