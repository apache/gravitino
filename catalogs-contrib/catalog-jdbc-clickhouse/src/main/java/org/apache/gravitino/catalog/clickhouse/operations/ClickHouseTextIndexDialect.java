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

package org.apache.gravitino.catalog.clickhouse.operations;

import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.IndexConstants;

/**
 * Resolves the ClickHouse server version and selects the text data-skipping index DDL grammar that
 * version accepts.
 *
 * <p>ClickHouse changed the text index keyword and grammar several times: {@code inverted(N)} in
 * v23.2-v24.4, {@code full_text(N)} from v24.5 until the gin rename, {@code gin(N)} in v25.5, an
 * early {@code text} v2 key-value tokenizer grammar in v25.6-v25.8 and the v3 tokenizer grammar
 * from v25.9. From v25.12 an explicit tokenizer is required because {@code default} is no longer
 * accepted.
 *
 * <p>The DDL is generated here for both {@code CREATE TABLE} and {@code ALTER TABLE ADD INDEX};
 * {@code tokenizer} and {@code ngram_size} come from the existing index property plumbing.
 */
final class ClickHouseTextIndexDialect {

  /** The text index keyword and grammar a given server version accepts. */
  enum Grammar {
    /** {@code inverted(N)}: v23.2 up to (excluding) v24.5. */
    INVERTED,
    /** {@code full_text(N)}: v24.5 up to (excluding) the v25.5 gin rename. */
    FULL_TEXT,
    /** {@code gin(N)}: v25.5 up to (excluding) v25.6. */
    GIN,
    /** {@code text(N)} with the v2 key-value tokenizer grammar: v25.6 up to (excluding) v25.9. */
    TEXT_V2,
    /** {@code text(N)} with the v3 tokenizer grammar: v25.9 and later. */
    TEXT_V3
  }

  private static final Pattern VERSION_PATTERN = Pattern.compile("^(\\d+)\\.(\\d+)(?:\\.(\\d+))?");

  private ClickHouseTextIndexDialect() {}

  /**
   * Parses the connected ClickHouse server version from a version string.
   *
   * @param versionString the raw version reported by the JDBC driver (e.g. {@code 25.5.1.2782})
   * @return the parsed major/minor/patch version
   * @throws IllegalArgumentException when the version cannot be classified, so the caller fails
   *     clearly instead of guessing a grammar
   */
  static ServerVersion parseServerVersion(String versionString) {
    if (StringUtils.isBlank(versionString)) {
      throw new IllegalArgumentException(
          "Cannot determine the connected ClickHouse server version; the JDBC driver reported no "
              + "version, so the text index DDL grammar cannot be selected");
    }

    Matcher matcher = VERSION_PATTERN.matcher(versionString.trim());
    if (!matcher.find()) {
      throw new IllegalArgumentException(
          "Cannot classify the connected ClickHouse server version '%s'; expected a version like "
                  .formatted(versionString)
              + "'25.5.1.2782'. The text index DDL grammar cannot be selected");
    }

    int major = Integer.parseInt(matcher.group(1));
    int minor = Integer.parseInt(matcher.group(2));
    int patch = matcher.group(3) == null ? 0 : Integer.parseInt(matcher.group(3));
    return new ServerVersion(major, minor, patch);
  }

  /** Selects the text index DDL grammar for a server version. */
  static Grammar grammarFor(ServerVersion version) {
    if (version.isBefore(23, 2)) {
      throw new IllegalArgumentException(
          "ClickHouse %s does not support text data skipping indexes; text indexes require "
                  .formatted(version)
              + "v23.2 or later");
    }
    if (version.isBefore(24, 5)) {
      return Grammar.INVERTED;
    }
    if (version.isBefore(25, 5)) {
      return Grammar.FULL_TEXT;
    }
    if (version.isBefore(25, 6)) {
      return Grammar.GIN;
    }
    if (version.isBefore(25, 9)) {
      return Grammar.TEXT_V2;
    }
    return Grammar.TEXT_V3;
  }

  /**
   * Builds the complete {@code TYPE ...} clause for a text data-skipping index.
   *
   * @param version the connected server version
   * @param properties the index properties, carrying an optional {@code tokenizer} and {@code
   *     ngram_size}
   * @param indexName the index name, used in error messages
   * @return the type clause, e.g. {@code full_text(3)} or {@code text(tokenizer = 'ngram')}
   */
  static String buildTypeClause(
      ServerVersion version, Map<String, String> properties, String indexName) {
    Grammar grammar = grammarFor(version);
    switch (grammar) {
      case INVERTED:
        return "inverted(" + resolveTextNgramSize(properties, indexName) + ")";
      case FULL_TEXT:
        return "full_text(" + resolveTextNgramSize(properties, indexName) + ")";
      case GIN:
        return "gin(" + resolveTextNgramSize(properties, indexName) + ")";
      case TEXT_V2:
        // v25.6-v25.8 use the early key-value tokenizer grammar, where a bare positional grammar
        // (e.g. text(3)) is still accepted.
        return "text(" + buildTokenizerClause(properties, indexName, false) + ")";
      case TEXT_V3:
      default:
        // v25.9+ requires the v3 tokenizer; from v25.12 an explicit tokenizer must be emitted
        // because "default" is no longer accepted.
        return "text(" + buildTokenizerClause(properties, indexName, true) + ")";
    }
  }

  /**
   * The default granularity ClickHouse applies to the text v3 index. ClickHouse ignores an
   * explicitly supplied granularity for this dialect, so the value is only used to keep the
   * generated DDL faithful.
   */
  static int defaultGranularity(Grammar grammar) {
    return grammar == Grammar.TEXT_V3 ? 100000000 : 1;
  }

  private static String resolveTextNgramSize(Map<String, String> properties, String indexName) {
    String raw = properties == null ? null : properties.get(IndexConstants.TEXT_NGRAM_SIZE);
    if (StringUtils.isBlank(raw)) {
      throw new IllegalArgumentException(
          "ngram_size is required for text index '%s' on this ClickHouse version"
              .formatted(indexName));
    }
    int value;
    try {
      value = Integer.parseInt(raw.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "ngram_size must be a valid integer for text index '%s', but got '%s'"
              .formatted(indexName, raw),
          e);
    }
    if (value < IndexConstants.MIN_TEXT_NGRAM_SIZE || value > IndexConstants.MAX_TEXT_NGRAM_SIZE) {
      throw new IllegalArgumentException(
          "ngram_size must be between %s and %s for text index '%s', but got '%s'"
              .formatted(
                  IndexConstants.MIN_TEXT_NGRAM_SIZE,
                  IndexConstants.MAX_TEXT_NGRAM_SIZE,
                  indexName,
                  value));
    }
    return String.valueOf(value);
  }

  private static String buildTokenizerClause(
      Map<String, String> properties, String indexName, boolean tokenizerIsMandatory) {
    String tokenizer = properties == null ? null : properties.get(IndexConstants.TOKENIZER);
    tokenizer = StringUtils.trimToEmpty(tokenizer);

    if (StringUtils.isBlank(tokenizer)) {
      if (tokenizerIsMandatory) {
        throw new IllegalArgumentException(
            "tokenizer is required for text index '%s' on ClickHouse v25.12 and later; the server "
                    .formatted(indexName)
                + "no longer accepts the implicit 'default' tokenizer");
      }
      // v25.6-v25.8 still accept the implicit default tokenizer, but the n-gram size is positional
      // there, so emit the documented default explicitly to stay unambiguous.
      tokenizer = "default";
    }

    String lower = tokenizer.toLowerCase(Locale.ROOT);
    if ("default".equals(lower)) {
      return "tokenizer = 'default'";
    }
    if ("ngram".equals(lower)) {
      return "tokenizer = 'ngram', ngram_size = " + resolveTextNgramSize(properties, indexName);
    }
    if ("split".equals(lower)
        || "standard".equals(lower)
        || "no_op".equals(lower)
        || "ascii_cjk".equals(lower)) {
      return "tokenizer = '" + lower + "'";
    }
    throw new IllegalArgumentException(
        "Unsupported tokenizer '%s' for text index '%s'".formatted(tokenizer, indexName));
  }

  /** A parsed ClickHouse server version. */
  static final class ServerVersion {
    private final int major;
    private final int minor;
    private final int patch;

    ServerVersion(int major, int minor, int patch) {
      this.major = major;
      this.minor = minor;
      this.patch = patch;
    }

    /**
     * @param otherMajor the major version to compare against
     * @param otherMinor the minor version to compare against
     * @return true when this version precedes the given major.minor
     */
    boolean isBefore(int otherMajor, int otherMinor) {
      if (major != otherMajor) {
        return major < otherMajor;
      }
      return minor < otherMinor;
    }

    int major() {
      return major;
    }

    int minor() {
      return minor;
    }

    int patch() {
      return patch;
    }

    @Override
    public String toString() {
      return "%s.%s.%s".formatted(major, minor, patch);
    }
  }
}
