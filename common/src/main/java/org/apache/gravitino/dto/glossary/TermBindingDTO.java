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
package org.apache.gravitino.dto.glossary;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;
import org.apache.gravitino.dto.tag.MetadataObjectDTO;
import org.apache.gravitino.glossary.TermBinding;

/** A visible glossary binding. A metric binding identifies its containing semantic model. */
public final class TermBindingDTO {
  @JsonProperty("kind")
  private final TermBinding.Kind kind;

  @JsonProperty("object")
  private final MetadataObjectDTO object;

  @JsonProperty("metric")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  private final String metric;

  /** Creates the wire representation of a metadata object or metric binding. */
  @JsonCreator
  public TermBindingDTO(
      @JsonProperty("kind") TermBinding.Kind kind,
      @JsonProperty("object") MetadataObjectDTO object,
      @JsonProperty("metric") @Nullable String metric) {
    this.kind = kind;
    this.object = object;
    this.metric = metric;
  }
  /** Returns the relationship of the term to the object. */
  public TermBinding.Kind getKind() {
    return kind;
  }
  /** Returns the bound object, or the containing semantic model for a metric. */
  public MetadataObjectDTO getObject() {
    return object;
  }
  /** Returns the metric name for a metric binding. */
  @Nullable
  public String getMetric() {
    return metric;
  }
}
