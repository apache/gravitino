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
package org.apache.gravitino.catalog.glue;

import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_ACCESS_KEY_ID;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_GLUE_API_CALL_ATTEMPT_TIMEOUT_MS;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_GLUE_API_CALL_TIMEOUT_MS;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_GLUE_CATALOG_ID;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_GLUE_ENDPOINT;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_GLUE_MAX_ATTEMPTS;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_REGION;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_SECRET_ACCESS_KEY;
import static org.apache.gravitino.catalog.glue.GlueConstants.DEFAULT_AWS_GLUE_API_CALL_ATTEMPT_TIMEOUT_MS;
import static org.apache.gravitino.catalog.glue.GlueConstants.DEFAULT_AWS_GLUE_API_CALL_TIMEOUT_MS;
import static org.apache.gravitino.catalog.glue.GlueConstants.DEFAULT_AWS_GLUE_MAX_ATTEMPTS;
import static org.apache.gravitino.catalog.glue.GlueConstants.DEFAULT_TABLE_FORMAT;
import static org.apache.gravitino.catalog.glue.GlueConstants.DEFAULT_TABLE_FORMAT_FILTER;
import static org.apache.gravitino.catalog.glue.GlueConstants.DEFAULT_TABLE_FORMAT_VALUE;
import static org.apache.gravitino.catalog.glue.GlueConstants.TABLE_FORMAT_FILTER;
import static org.apache.gravitino.catalog.glue.GlueConstants.WAREHOUSE;
import static org.apache.gravitino.connector.PropertyEntry.stringOptionalPropertyEntry;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.connector.BaseCatalogPropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;
import software.amazon.awssdk.regions.Region;

/** Properties metadata for the AWS Glue Data Catalog connector catalog-level configuration. */
public class GlueCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {

  /** Accepted values of {@code default-table-format}, compared case-insensitively. */
  static final Set<String> DEFAULT_TABLE_FORMATS = ImmutableSet.of("iceberg", "hive");

  /** Accepted entries of {@code table-format-filter}, compared case-insensitively. */
  static final Set<String> TABLE_FORMAT_FILTER_VALUES =
      ImmutableSet.of(DEFAULT_TABLE_FORMAT_FILTER, "hive", "iceberg", "delta", "parquet");

  private static final Pattern CATALOG_ID_PATTERN = Pattern.compile("\\d{12}");

  private static final Pattern HOST_PORT_PATTERN =
      Pattern.compile("[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?(:\\d{1,5})?");

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              AWS_REGION,
              stringEntry(
                  AWS_REGION,
                  "AWS region for the Glue Data Catalog (e.g. us-east-1)",
                  true /* required */,
                  true /* immutable */,
                  null /* defaultValue */,
                  GlueCatalogPropertiesMetadata::checkRegion))
          .put(
              AWS_GLUE_CATALOG_ID,
              stringEntry(
                  AWS_GLUE_CATALOG_ID,
                  "The 12-digit AWS account ID that owns the Glue catalog."
                      + " When omitted, defaults to the caller's AWS account ID.",
                  false /* required */,
                  true /* immutable */,
                  null /* defaultValue */,
                  GlueCatalogPropertiesMetadata::checkCatalogId))
          .put(
              AWS_ACCESS_KEY_ID,
              stringOptionalPropertyEntry(
                  AWS_ACCESS_KEY_ID,
                  "AWS access key ID for static credential authentication."
                      + " When omitted the default credential chain is used.",
                  false /* immutable */,
                  null /* defaultValue */,
                  true /* hidden */))
          .put(
              AWS_SECRET_ACCESS_KEY,
              stringOptionalPropertyEntry(
                  AWS_SECRET_ACCESS_KEY,
                  "AWS secret access key paired with aws-access-key-id."
                      + " When omitted the default credential chain is used.",
                  false /* immutable */,
                  null /* defaultValue */,
                  true /* hidden */))
          .put(
              AWS_GLUE_ENDPOINT,
              stringEntry(
                  AWS_GLUE_ENDPOINT,
                  "Custom Glue endpoint URL for VPC endpoints or LocalStack testing"
                      + " (e.g. http://localhost:4566)",
                  false /* required */,
                  false /* immutable */,
                  null /* defaultValue */,
                  GlueCatalogPropertiesMetadata::checkEndpoint))
          .put(
              AWS_GLUE_API_CALL_TIMEOUT_MS,
              positiveLongEntry(
                  AWS_GLUE_API_CALL_TIMEOUT_MS,
                  "Upper bound in milliseconds for one Glue API call made by the catalog,"
                      + " including retries. Also applied to the Glue calls of Iceberg tables, but"
                      + " not to S3 I/O.",
                  DEFAULT_AWS_GLUE_API_CALL_TIMEOUT_MS))
          .put(
              AWS_GLUE_API_CALL_ATTEMPT_TIMEOUT_MS,
              positiveLongEntry(
                  AWS_GLUE_API_CALL_ATTEMPT_TIMEOUT_MS,
                  "Upper bound in milliseconds for a single attempt of a Glue API call. Also used"
                      + " as the connection and socket timeout of the S3 client used for Iceberg"
                      + " tables.",
                  DEFAULT_AWS_GLUE_API_CALL_ATTEMPT_TIMEOUT_MS))
          .put(
              AWS_GLUE_MAX_ATTEMPTS,
              new PropertyEntry.Builder<Integer>()
                  .withName(AWS_GLUE_MAX_ATTEMPTS)
                  .withDescription(
                      "Maximum number of attempts of a Glue API call made by the catalog,"
                          + " including the first one. Also applied to the Glue calls of Iceberg"
                          + " tables, but not to S3 I/O.")
                  .withJavaType(Integer.class)
                  .withDefaultValue(DEFAULT_AWS_GLUE_MAX_ATTEMPTS)
                  .withDecoder(
                      value -> {
                        int attempts = Integer.parseInt(value.trim());
                        Preconditions.checkArgument(attempts > 0, "must be positive");
                        return attempts;
                      })
                  .withEncoder(String::valueOf)
                  .build())
          .put(
              DEFAULT_TABLE_FORMAT,
              stringEntry(
                  DEFAULT_TABLE_FORMAT,
                  "Default format for tables created via createTable(). Accepted: iceberg, hive.",
                  false /* required */,
                  false /* immutable */,
                  DEFAULT_TABLE_FORMAT_VALUE,
                  GlueCatalogPropertiesMetadata::checkDefaultTableFormat))
          .put(
              TABLE_FORMAT_FILTER,
              stringEntry(
                  TABLE_FORMAT_FILTER,
                  "Comma-separated table formats exposed by the catalog. Tables of other formats"
                      + " are absent from list, load, alter and drop, and cannot be created."
                      + " Accepted: all, hive, iceberg, delta, parquet.",
                  false /* required */,
                  false /* immutable */,
                  DEFAULT_TABLE_FORMAT_FILTER,
                  GlueCatalogPropertiesMetadata::checkTableFormatFilter))
          .put(
              WAREHOUSE,
              stringEntry(
                  WAREHOUSE,
                  "Base storage path used as warehouse when no explicit location is set"
                      + " at table creation time and the database declares no LocationUri"
                      + " (e.g. s3://my-bucket/warehouse)."
                      + " Table location is then derived as warehouse/database/table.",
                  true /* required */,
                  false /* immutable */,
                  null /* defaultValue */,
                  GlueCatalogPropertiesMetadata::checkWarehouse))
          .build();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }

  /**
   * Parses {@code aws-glue-endpoint} into a URI with an {@code http} or {@code https} scheme and a
   * host.
   *
   * @param endpoint the configured endpoint
   * @return the parsed endpoint URI
   * @throws IllegalArgumentException if the endpoint is not such a URI
   */
  static URI parseEndpoint(String endpoint) {
    URI uri;
    try {
      uri = new URI(endpoint);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format("Property '%s' is not a valid URI: %s", AWS_GLUE_ENDPOINT, e.getMessage()),
          e);
    }
    Preconditions.checkArgument(
        ("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
            && hasHost(uri),
        "Property '%s' must be an http or https URL with a host, got: %s",
        AWS_GLUE_ENDPOINT,
        endpoint);
    return uri;
  }

  private static boolean hasHost(URI uri) {
    // getHost() is null for host names with '_', such as Docker service names, so such an
    // authority is accepted when it is a plain host name with an optional port.
    return uri.getHost() != null
        || (uri.getRawAuthority() != null
            && HOST_PORT_PATTERN.matcher(uri.getRawAuthority()).matches());
  }

  private static String checkRegion(String region) {
    // Region.of() accepts any string, and an unknown or mis-cased region only fails later as an
    // unresolvable host or as a signature error that reads like rejected credentials.
    Preconditions.checkArgument(
        Region.regions().contains(Region.of(region)),
        "Property '%s' must be a known AWS region identifier in lower case, got: '%s'",
        AWS_REGION,
        region);
    return region;
  }

  private static String checkCatalogId(String catalogId) {
    Preconditions.checkArgument(
        CATALOG_ID_PATTERN.matcher(catalogId).matches(),
        "Property '%s' must be a 12-digit AWS account ID, got: '%s'",
        AWS_GLUE_CATALOG_ID,
        catalogId);
    return catalogId;
  }

  private static String checkEndpoint(String endpoint) {
    parseEndpoint(endpoint);
    return endpoint;
  }

  private static String checkWarehouse(String warehouse) {
    URI uri;
    try {
      uri = new URI(warehouse);
    } catch (URISyntaxException e) {
      uri = null;
    }
    // A path without a scheme would become a location on the Gravitino server's local disk. The
    // S3 requirement of Iceberg tables is checked when such a table is created, because Hive
    // tables may live on other file systems.
    Preconditions.checkArgument(
        uri != null
            && uri.getScheme() != null
            && StringUtils.isNotBlank(uri.getRawSchemeSpecificPart())
            && (!uri.getScheme().toLowerCase(Locale.ROOT).startsWith("s3")
                || GlueIcebergTableHelper.isS3Location(warehouse)),
        "Property '%s' must be a URI with a scheme, such as s3://bucket/path, got: '%s'",
        WAREHOUSE,
        warehouse);
    return warehouse;
  }

  private static String checkDefaultTableFormat(String format) {
    Preconditions.checkArgument(
        DEFAULT_TABLE_FORMATS.contains(format.toLowerCase(Locale.ROOT)),
        "Property '%s' must be one of %s, got: '%s'",
        DEFAULT_TABLE_FORMAT,
        DEFAULT_TABLE_FORMATS,
        format);
    return format;
  }

  private static String checkTableFormatFilter(String filter) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(filter)
            && Arrays.stream(filter.split(",", -1))
                .map(s -> s.trim().toLowerCase(Locale.ROOT))
                .allMatch(TABLE_FORMAT_FILTER_VALUES::contains),
        "Property '%s' must be a comma-separated list of %s, got: '%s'",
        TABLE_FORMAT_FILTER,
        TABLE_FORMAT_FILTER_VALUES,
        filter);
    return filter;
  }

  private static PropertyEntry<String> stringEntry(
      String name,
      String description,
      boolean required,
      boolean immutable,
      String defaultValue,
      Function<String, String> decoder) {
    return new PropertyEntry.Builder<String>()
        .withName(name)
        .withDescription(description)
        .withRequired(required)
        .withImmutable(immutable)
        .withJavaType(String.class)
        .withDefaultValue(defaultValue)
        .withDecoder(decoder)
        .withEncoder(Function.identity())
        .build();
  }

  private static PropertyEntry<Long> positiveLongEntry(
      String name, String description, long defaultValue) {
    return new PropertyEntry.Builder<Long>()
        .withName(name)
        .withDescription(description)
        .withJavaType(Long.class)
        .withDefaultValue(defaultValue)
        .withDecoder(
            value -> {
              long parsed = Long.parseLong(value.trim());
              Preconditions.checkArgument(parsed > 0, "must be positive");
              return parsed;
            })
        .withEncoder(String::valueOf)
        .build();
  }
}
