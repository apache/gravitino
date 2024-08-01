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
package org.apache.gravitino.trino.connector.metadata;

import static org.apache.gravitino.Catalog.CLOUD_REGION_CODE;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.trino.spi.TrinoException;
import java.time.Instant;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;

/** Help Apache Gravitino connector access CatalogMetadata from Gravitino client. */
public class GravitinoCatalog {

  private static ObjectMapper objectMapper = new ObjectMapper();

  private final String metalake;
  private final String provider;
  private final String name;
  private final Map<String, String> properties;
  private final long lastModifiedTime;

  static {
    objectMapper =
        JsonMapper.builder()
            .disable(MapperFeature.AUTO_DETECT_CREATORS)
            .disable(MapperFeature.AUTO_DETECT_FIELDS)
            .disable(MapperFeature.AUTO_DETECT_SETTERS)
            .disable(MapperFeature.AUTO_DETECT_GETTERS)
            .build();
  }

  public GravitinoCatalog(String metalake, Catalog catalog) {
    this.metalake = metalake;
    this.provider = catalog.provider();
    this.name = catalog.name();
    this.properties = catalog.properties();
    Instant time =
        catalog.auditInfo().lastModifiedTime() == null
            ? catalog.auditInfo().createTime()
            : catalog.auditInfo().lastModifiedTime();
    lastModifiedTime = time.toEpochMilli();
  }

  @JsonCreator
  public GravitinoCatalog(
      @JsonProperty("metalake") String metalake,
      @JsonProperty("provider") String provider,
      @JsonProperty("name") String name,
      @JsonProperty("properties") Map<String, String> properties,
      @JsonProperty("lastModifiedTime") long lastModifiedTime) {
    this.metalake = metalake;
    this.provider = provider;
    this.name = name;
    this.properties = properties;
    this.lastModifiedTime = lastModifiedTime;
  }

  @JsonProperty
  public String getProvider() {
    return provider;
  }

  @JsonProperty
  public String getName() {
    return name;
  }

  @JsonProperty
  public String getMetalake() {
    return metalake;
  }

  public NameIdentifier geNameIdentifier() {
    return NameIdentifier.of(metalake, name);
  }

  public String getProperty(String name, String defaultValue) {
    return properties.getOrDefault(name, defaultValue);
  }

  public String getRequiredProperty(String name) throws Exception {
    String value = properties.getOrDefault(name, "");
    if (StringUtils.isBlank(value)) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_MISSING_CONFIG, "Missing required config: " + name);
    }
    return value;
  }

  @JsonProperty
  public Map<String, String> getProperties() {
    return properties;
  }

  @JsonProperty
  public long getLastModifiedTime() {
    return lastModifiedTime;
  }

  public static String toJson(GravitinoCatalog catalog) throws JsonProcessingException {
    return objectMapper.writeValueAsString(catalog);
  }

  public static GravitinoCatalog fromJson(String jsonString) throws JsonProcessingException {
    return objectMapper.readValue(jsonString, GravitinoCatalog.class);
  }

  public String getRegion() {
    return properties.getOrDefault(CLOUD_REGION_CODE, "");
  }

  public boolean isSameRegion(String region) {
    // When the Gravitino connector has not configured the cloud.region-code,
    // or the catalog has not configured the cloud.region-code,
    // or the catalog cluster name is equal to the connector-configured region code,
    // the catalog is belong to the region
    return StringUtils.isEmpty(region)
        || StringUtils.isEmpty(getRegion())
        || region.equals(getRegion());
  }
}
