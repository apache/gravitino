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
package org.apache.gravitino.dto.semantic;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.gravitino.semantic.CustomExtension;

/** DTO for a vendor-specific Semantic Model extension. */
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(setterPrefix = "with")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CustomExtensionDTO {

  @JsonProperty("vendorName")
  private String vendorName;

  @JsonProperty("data")
  private String data;

  /**
   * Creates a custom extension DTO from an API model.
   *
   * @param extension The API custom extension.
   * @return The custom extension DTO.
   */
  public static CustomExtensionDTO fromCustomExtension(CustomExtension extension) {
    return builder().withVendorName(extension.vendorName()).withData(extension.data()).build();
  }

  /**
   * Converts this DTO to an API custom extension.
   *
   * @return The API custom extension.
   */
  public CustomExtension toCustomExtension() {
    return CustomExtension.builder().withVendorName(vendorName).withData(data).build();
  }
}
