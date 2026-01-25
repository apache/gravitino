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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.View;
import org.apache.iceberg.rest.responses.LoadViewResponse;

/** Represents an Apache Iceberg View entity in the Iceberg catalog. */
@ToString
@Getter
public class IcebergView implements View {

  private String name;
  private Map<String, String> properties;
  private AuditInfo auditInfo;

  private IcebergView() {}

  /**
   * Converts an Iceberg LoadViewResponse to a Gravitino IcebergView.
   *
   * @param response The Iceberg LoadViewResponse.
   * @param viewName The name of the view.
   * @return A new IcebergView instance.
   */
  public static IcebergView fromLoadViewResponse(LoadViewResponse response, String viewName) {
    Map<String, String> properties =
        response.metadata() != null && response.metadata().properties() != null
            ? Maps.newHashMap(response.metadata().properties())
            : Maps.newHashMap();

    return IcebergView.builder()
        .withName(viewName)
        .withProperties(properties)
        .withAuditInfo(
            AuditInfo.builder().withCreator("iceberg").withCreateTime(Instant.now()).build())
        .build();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Map<String, String> properties() {
    return properties != null ? properties : Collections.emptyMap();
  }

  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /** Returns a new builder for constructing an IcebergView. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for IcebergView. */
  public static class Builder {
    private final IcebergView view;

    private Builder() {
      this.view = new IcebergView();
    }

    public Builder withName(String name) {
      view.name = name;
      return this;
    }

    public Builder withProperties(Map<String, String> properties) {
      view.properties = properties != null ? Maps.newHashMap(properties) : Maps.newHashMap();
      return this;
    }

    public Builder withAuditInfo(AuditInfo auditInfo) {
      view.auditInfo = auditInfo;
      return this;
    }

    public IcebergView build() {
      return view;
    }
  }
}
