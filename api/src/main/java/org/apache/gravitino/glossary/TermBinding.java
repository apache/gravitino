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
package org.apache.gravitino.glossary;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.Evolving;

/**
 * A binding links a glossary term to the data behind it. A binding is direct only: binding a table
 * does not bind its columns.
 *
 * <p>This public value identifies targets by their names. The server resolves each target to its
 * own stable ID for persistence, including a metric ID rather than its containing model's ID. The
 * storage key does not include the metric name. A binding grants no access to its target, and a
 * {@link Kind#USED_BY} binding does not by itself establish that the metric computes the term.
 */
@Evolving
public final class TermBinding {

  /** The kind of a binding. */
  public enum Kind {
    /** The object stores or computes the term, for example a table or a column. */
    IMPLEMENTED_BY,
    /** A metric of a semantic model is built from the term. */
    USED_BY
  }

  /** The types of the metadata objects that can implement a term. */
  public static final Set<MetadataObject.Type> IMPLEMENTED_BY_TYPES =
      ImmutableSet.of(
          MetadataObject.Type.CATALOG,
          MetadataObject.Type.SCHEMA,
          MetadataObject.Type.TABLE,
          MetadataObject.Type.VIEW,
          MetadataObject.Type.COLUMN,
          MetadataObject.Type.FILESET,
          MetadataObject.Type.TOPIC,
          MetadataObject.Type.MODEL);

  private final Kind kind;
  private final MetadataObject object;
  @Nullable private final String metric;

  private TermBinding(Kind kind, MetadataObject object, @Nullable String metric) {
    this.kind = kind;
    this.object = object;
    this.metric = metric;
  }

  /**
   * Creates a binding to an object that implements the term.
   *
   * @param object The object, of a type in {@link #IMPLEMENTED_BY_TYPES}. In Step 1, {@code COLUMN}
   *     means a table column; view output columns are not supported. The server validates the
   *     containing table, which cannot be determined from this name-only value.
   * @return The binding.
   * @throws IllegalArgumentException If the object is null or of another type.
   */
  public static TermBinding implementedBy(MetadataObject object) {
    Preconditions.checkArgument(object != null, "Bound object cannot be null");
    Preconditions.checkArgument(
        IMPLEMENTED_BY_TYPES.contains(object.type()),
        "A term cannot be implemented by an object of type %s",
        object.type());
    return new TermBinding(Kind.IMPLEMENTED_BY, object, null);
  }

  /**
   * Creates a binding to a metric of a semantic model that uses the term.
   *
   * @param semanticModel The semantic model that defines the metric.
   * @param metric The name of the metric.
   * @return The binding.
   * @throws IllegalArgumentException If the object is not a semantic model, or the metric is blank.
   */
  public static TermBinding usedBy(MetadataObject semanticModel, String metric) {
    Preconditions.checkArgument(semanticModel != null, "Semantic model cannot be null");
    Preconditions.checkArgument(
        semanticModel.type() == MetadataObject.Type.SEMANTIC_MODEL,
        "A term can be used only by a metric of a semantic model, not by an object of type %s",
        semanticModel.type());
    Preconditions.checkArgument(StringUtils.isNotBlank(metric), "Metric name cannot be blank");
    return new TermBinding(Kind.USED_BY, semanticModel, metric);
  }

  /**
   * Returns the kind of the binding.
   *
   * @return The kind of the binding.
   */
  public Kind kind() {
    return kind;
  }

  /**
   * Returns the bound object. For a {@link Kind#USED_BY} binding, it is the semantic model that
   * defines the metric.
   *
   * @return The bound object.
   */
  public MetadataObject object() {
    return object;
  }

  /**
   * Returns the name of the bound metric.
   *
   * @return The name of the metric for a {@link Kind#USED_BY} binding, or empty for a {@link
   *     Kind#IMPLEMENTED_BY} binding.
   */
  public Optional<String> metric() {
    return Optional.ofNullable(metric);
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TermBinding)) {
      return false;
    }
    TermBinding that = (TermBinding) o;
    return kind == that.kind
        && Objects.equals(object, that.object)
        && Objects.equals(metric, that.metric);
  }

  @Override
  public int hashCode() {
    return Objects.hash(kind, object, metric);
  }

  @Override
  public String toString() {
    return "TermBinding{kind="
        + kind
        + ", object="
        + object.fullName()
        + ", type="
        + object.type()
        + (metric == null ? "" : ", metric=" + metric)
        + "}";
  }
}
