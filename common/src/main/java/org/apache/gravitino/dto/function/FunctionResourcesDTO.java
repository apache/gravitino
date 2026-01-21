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
import org.apache.gravitino.function.FunctionResources;

/** DTO for function resources. */
@Getter
@EqualsAndHashCode
public class FunctionResourcesDTO {

  @JsonProperty("jars")
  private String[] jars;

  @JsonProperty("files")
  private String[] files;

  @JsonProperty("archives")
  private String[] archives;

  private FunctionResourcesDTO() {}

  private FunctionResourcesDTO(String[] jars, String[] files, String[] archives) {
    this.jars = jars;
    this.files = files;
    this.archives = archives;
  }

  /**
   * Convert this DTO to a {@link FunctionResources} instance.
   *
   * @return The function resources.
   */
  public FunctionResources toFunctionResources() {
    return FunctionResources.of(jars, files, archives);
  }

  /**
   * Create a {@link FunctionResourcesDTO} from a {@link FunctionResources} instance.
   *
   * @param resources The function resources.
   * @return The function resources DTO.
   */
  public static FunctionResourcesDTO fromFunctionResources(FunctionResources resources) {
    if (resources == null) {
      return null;
    }
    return new FunctionResourcesDTO(resources.jars(), resources.files(), resources.archives());
  }

  @Override
  public String toString() {
    return "FunctionResourcesDTO{"
        + "jars="
        + Arrays.toString(jars)
        + ", files="
        + Arrays.toString(files)
        + ", archives="
        + Arrays.toString(archives)
        + '}';
  }

  /** Builder for {@link FunctionResourcesDTO}. */
  public static class Builder {
    private String[] jars;
    private String[] files;
    private String[] archives;

    /**
     * Set the jar resources.
     *
     * @param jars The jar resources.
     * @return This builder.
     */
    public Builder withJars(String[] jars) {
      this.jars = jars;
      return this;
    }

    /**
     * Set the file resources.
     *
     * @param files The file resources.
     * @return This builder.
     */
    public Builder withFiles(String[] files) {
      this.files = files;
      return this;
    }

    /**
     * Set the archive resources.
     *
     * @param archives The archive resources.
     * @return This builder.
     */
    public Builder withArchives(String[] archives) {
      this.archives = archives;
      return this;
    }

    /**
     * Build the {@link FunctionResourcesDTO}.
     *
     * @return The function resources DTO.
     */
    public FunctionResourcesDTO build() {
      return new FunctionResourcesDTO(jars, files, archives);
    }
  }

  /**
   * Create a new builder.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }
}
