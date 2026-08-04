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
package org.apache.gravitino.rel;

import com.google.common.base.Preconditions;
import java.util.Objects;
import org.apache.gravitino.annotation.Unstable;
import org.apache.gravitino.rel.metric.SemanticModel;

/** A structured OSI-compatible {@link Representation} of a Metric View. */
@Unstable
public final class MetricRepresentation implements Representation {

  private final SemanticModel semanticModel;

  private MetricRepresentation(SemanticModel semanticModel) {
    this.semanticModel = semanticModel;
  }

  /**
   * Creates a builder for a metric representation.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String type() {
    return Representation.TYPE_METRIC;
  }

  /**
   * Returns the structured semantic model.
   *
   * @return The semantic model.
   */
  public SemanticModel semanticModel() {
    return semanticModel;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof MetricRepresentation)) {
      return false;
    }
    MetricRepresentation that = (MetricRepresentation) other;
    return Objects.equals(semanticModel, that.semanticModel);
  }

  @Override
  public int hashCode() {
    return Objects.hash(semanticModel);
  }

  @Override
  public String toString() {
    return "MetricRepresentation{" + "semanticModel=" + semanticModel + '}';
  }

  /** Builder for {@link MetricRepresentation}. */
  public static final class Builder {
    private SemanticModel semanticModel;

    private Builder() {}

    /**
     * Sets the structured semantic model.
     *
     * @param semanticModel The semantic model.
     * @return This builder.
     */
    public Builder withSemanticModel(SemanticModel semanticModel) {
      this.semanticModel = semanticModel;
      return this;
    }

    /**
     * Builds the metric representation.
     *
     * @return The metric representation.
     */
    public MetricRepresentation build() {
      Preconditions.checkArgument(semanticModel != null, "semanticModel must not be null");
      return new MetricRepresentation(semanticModel);
    }
  }
}
