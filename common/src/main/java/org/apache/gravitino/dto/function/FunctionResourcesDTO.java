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
package org.apache.gravitino.dto.function;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.function.FunctionResources;

/** DTO for {@link FunctionResources}. */
@Getter
@EqualsAndHashCode
@ToString
public class FunctionResourcesDTO {

  @JsonProperty("jars")
  private String[] jars;

  @JsonProperty("files")
  private String[] files;

  @JsonProperty("archives")
  private String[] archives;

  private FunctionResourcesDTO() {
    this(FunctionResources.empty());
  }

  /**
   * Creates a DTO from {@link FunctionResources}.
   *
   * @param resources Source resources.
   */
  public FunctionResourcesDTO(FunctionResources resources) {
    this(resources.jars(), resources.files(), resources.archives());
  }

  /**
   * Creates a DTO with explicit resources.
   *
   * @param jars Jar URIs.
   * @param files File URIs.
   * @param archives Archive URIs.
   */
  public FunctionResourcesDTO(String[] jars, String[] files, String[] archives) {
    this.jars = jars;
    this.files = files;
    this.archives = archives;
  }

  /**
   * Converts this DTO to {@link FunctionResources}.
   *
   * @return Converted resources.
   */
  public FunctionResources toResources() {
    if ((jars == null || jars.length == 0)
        && (files == null || files.length == 0)
        && (archives == null || archives.length == 0)) {
      return FunctionResources.empty();
    }
    return FunctionResources.of(
        jars == null ? FunctionResources.empty().jars() : Arrays.copyOf(jars, jars.length),
        files == null ? FunctionResources.empty().files() : Arrays.copyOf(files, files.length),
        archives == null
            ? FunctionResources.empty().archives()
            : Arrays.copyOf(archives, archives.length));
  }
}
