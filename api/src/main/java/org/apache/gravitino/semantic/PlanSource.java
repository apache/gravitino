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
package org.apache.gravitino.semantic;

import com.google.common.base.Preconditions;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.Evolving;

/** A dataset required by a {@link SemanticQueryPlan} and the physical source that provides it. */
@Evolving
public final class PlanSource {

  private final String dataset;
  private final NameIdentifier source;

  private PlanSource(String dataset, NameIdentifier source) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(dataset), "dataset must not be null or blank");
    Preconditions.checkArgument(source != null, "source must not be null");
    this.dataset = dataset;
    this.source = source;
  }

  /**
   * Creates a plan source.
   *
   * @param dataset The dataset name declared by the Semantic Model.
   * @param source The physical source identifier.
   * @return The plan source.
   */
  public static PlanSource of(String dataset, NameIdentifier source) {
    return new PlanSource(dataset, source);
  }

  /**
   * Returns the dataset name declared by the Semantic Model.
   *
   * @return The dataset name.
   */
  public String dataset() {
    return dataset;
  }

  /**
   * Returns the physical source identifier.
   *
   * @return The source identifier.
   */
  public NameIdentifier source() {
    return source;
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof PlanSource)) {
      return false;
    }
    PlanSource that = (PlanSource) other;
    return dataset.equals(that.dataset) && source.equals(that.source);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataset, source);
  }

  @Override
  public String toString() {
    return "PlanSource{" + "dataset='" + dataset + '\'' + ", source=" + source + '}';
  }
}
