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

package org.apache.gravitino.secret;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/** Safe metadata for a configured secrets-provider instance. */
public final class SecretProviderInfo {

  private final String name;
  private final String type;
  @Nullable private final String uri;

  /**
   * Creates provider metadata.
   *
   * @param name the configured provider instance name
   * @param type the provider type identifier from {@link SecretProvider#type()}
   * @param uri optional non-secret provider endpoint; may be {@code null}
   */
  @JsonCreator
  public SecretProviderInfo(
      @JsonProperty("name") String name,
      @JsonProperty("type") String type,
      @JsonProperty("uri") @Nullable String uri) {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "name must not be blank");
    Preconditions.checkArgument(StringUtils.isNotBlank(type), "type must not be blank");
    this.name = name;
    this.type = type;
    this.uri = StringUtils.isBlank(uri) ? null : uri;
  }

  /**
   * Returns the configured provider instance name.
   *
   * @return the provider name
   */
  @JsonProperty("name")
  public String name() {
    return name;
  }

  /**
   * Returns the provider type identifier.
   *
   * @return the provider type
   */
  @JsonProperty("type")
  public String type() {
    return type;
  }

  /**
   * Returns the optional non-secret provider endpoint.
   *
   * @return the URI, or {@code null} when not configured
   */
  @Nullable
  @JsonProperty("uri")
  public String uri() {
    return uri;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SecretProviderInfo)) {
      return false;
    }
    SecretProviderInfo that = (SecretProviderInfo) o;
    return Objects.equals(name, that.name)
        && Objects.equals(type, that.type)
        && Objects.equals(uri, that.uri);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, uri);
  }

  @Override
  public String toString() {
    return "SecretProviderInfo{name='" + name + "', type='" + type + "', uri='" + uri + "'}";
  }
}
