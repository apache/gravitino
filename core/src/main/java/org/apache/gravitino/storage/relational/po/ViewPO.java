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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import lombok.Getter;

@Getter
public class ViewPO {
  private Long viewId;
  private String viewName;
  private Long metalakeId;
  private Long catalogId;
  private Long schemaId;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ViewPO)) {
      return false;
    }
    ViewPO viewPO = (ViewPO) o;
    return Objects.equal(getViewId(), viewPO.getViewId())
        && Objects.equal(getViewName(), viewPO.getViewName())
        && Objects.equal(getMetalakeId(), viewPO.getMetalakeId())
        && Objects.equal(getCatalogId(), viewPO.getCatalogId())
        && Objects.equal(getSchemaId(), viewPO.getSchemaId())
        && Objects.equal(getCurrentVersion(), viewPO.getCurrentVersion())
        && Objects.equal(getLastVersion(), viewPO.getLastVersion())
        && Objects.equal(getDeletedAt(), viewPO.getDeletedAt());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getViewId(),
        getViewName(),
        getMetalakeId(),
        getCatalogId(),
        getSchemaId(),
        getCurrentVersion(),
        getLastVersion(),
        getDeletedAt());
  }

  public static class Builder {
    private final ViewPO viewPO;

    private Builder() {
      viewPO = new ViewPO();
    }

    public Builder withViewId(Long viewId) {
      viewPO.viewId = viewId;
      return this;
    }

    public Builder withViewName(String viewName) {
      viewPO.viewName = viewName;
      return this;
    }

    public Builder withMetalakeId(Long metalakeId) {
      viewPO.metalakeId = metalakeId;
      return this;
    }

    public Builder withCatalogId(Long catalogId) {
      viewPO.catalogId = catalogId;
      return this;
    }

    public Builder withSchemaId(Long schemaId) {
      viewPO.schemaId = schemaId;
      return this;
    }

    public Builder withCurrentVersion(Long currentVersion) {
      viewPO.currentVersion = currentVersion;
      return this;
    }

    public Builder withLastVersion(Long lastVersion) {
      viewPO.lastVersion = lastVersion;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      viewPO.deletedAt = deletedAt;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(viewPO.viewId != null, "View id is required");
      Preconditions.checkArgument(viewPO.viewName != null, "View name is required");
      Preconditions.checkArgument(viewPO.metalakeId != null, "Metalake id is required");
      Preconditions.checkArgument(viewPO.catalogId != null, "Catalog id is required");
      Preconditions.checkArgument(viewPO.schemaId != null, "Schema id is required");
      Preconditions.checkArgument(viewPO.currentVersion != null, "Current version is required");
      Preconditions.checkArgument(viewPO.lastVersion != null, "Last version is required");
      Preconditions.checkArgument(viewPO.deletedAt != null, "Deleted at is required");
    }

    public ViewPO build() {
      validate();
      return viewPO;
    }
  }

  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
  }
}
