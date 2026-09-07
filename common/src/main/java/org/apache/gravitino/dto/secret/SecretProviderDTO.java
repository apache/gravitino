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
package org.apache.gravitino.dto.secret;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

/** Discovery metadata for a configured secrets-provider instance. */
public final class SecretProviderDTO {

  @JsonProperty("name")
  private String name;

  @JsonProperty("type")
  private String type;

  /** Default constructor for Jackson deserialization. */
  public SecretProviderDTO() {}

  /**
   * Creates provider discovery metadata.
   *
   * @param name the configured provider instance name
   * @param type the provider type identifier
   */
  public SecretProviderDTO(String name, String type) {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "name must not be blank");
    Preconditions.checkArgument(StringUtils.isNotBlank(type), "type must not be blank");
    this.name = name;
    this.type = type;
  }

  /**
   * Returns the configured provider instance name.
   *
   * @return the provider name
   */
  public String name() {
    return name;
  }

  /**
   * Returns the provider type identifier.
   *
   * @return the provider type
   */
  public String type() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SecretProviderDTO)) {
      return false;
    }
    SecretProviderDTO that = (SecretProviderDTO) o;
    return Objects.equals(name, that.name) && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type);
  }

  @Override
  public String toString() {
    return "SecretProviderDTO{name='" + name + "', type='" + type + "'}";
  }
}
