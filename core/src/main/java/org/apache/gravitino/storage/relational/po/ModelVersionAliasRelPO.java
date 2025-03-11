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
package org.apache.gravitino.storage.relational.po;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

@EqualsAndHashCode
@Getter
public class ModelVersionAliasRelPO {

  private Long modelId;

  private Integer modelVersion;

  private String modelVersionAlias;

  private Long deletedAt;

  private ModelVersionAliasRelPO() {}

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private final ModelVersionAliasRelPO modelVersionAliasRelPO;

    private Builder() {
      modelVersionAliasRelPO = new ModelVersionAliasRelPO();
    }

    public Builder withModelId(Long modelId) {
      modelVersionAliasRelPO.modelId = modelId;
      return this;
    }

    public Builder withModelVersion(Integer modelVersion) {
      modelVersionAliasRelPO.modelVersion = modelVersion;
      return this;
    }

    public Builder withModelVersionAlias(String modelVersionAlias) {
      modelVersionAliasRelPO.modelVersionAlias = modelVersionAlias;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      modelVersionAliasRelPO.deletedAt = deletedAt;
      return this;
    }

    public ModelVersionAliasRelPO build() {
      Preconditions.checkArgument(modelVersionAliasRelPO.modelId != null, "modelId is required");
      Preconditions.checkArgument(
          modelVersionAliasRelPO.modelVersion != null, "modelVersion is required");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(modelVersionAliasRelPO.modelVersionAlias),
          "modelVersionAlias is required");
      Preconditions.checkArgument(
          modelVersionAliasRelPO.deletedAt != null, "deletedAt is required");
      return modelVersionAliasRelPO;
    }
  }
}
