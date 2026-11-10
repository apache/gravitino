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

package org.apache.gravitino.listener.api.info;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.View;

/**
 * ViewInfo exposes view information for event listeners; it is read-only. Most fields are shallow
 * copies internally, not deep copies, for performance.
 */
@DeveloperApi
public final class ViewInfo {
  private final String name;
  private final Column[] columns;
  @Nullable private final String comment;
  private final Representation[] representations;
  @Nullable private final String defaultCatalog;
  @Nullable private final String defaultSchema;
  private final Map<String, String> properties;
  @Nullable private final Audit auditInfo;

  /** Constructs a ViewInfo from a {@link View}. */
  public ViewInfo(View view) {
    this(
        view.name(),
        view.columns(),
        view.comment(),
        view.representations(),
        view.defaultCatalog(),
        view.defaultSchema(),
        view.properties(),
        view.auditInfo());
  }

  /**
   * Constructs ViewInfo with the given fields.
   *
   * @param name View name
   * @param columns Output columns
   * @param comment Optional comment
   * @param representations View representations (at least one in a valid create request)
   * @param defaultCatalog Optional default catalog for the definition
   * @param defaultSchema Optional default schema for the definition
   * @param properties View properties; copied defensively
   * @param auditInfo Optional audit information
   */
  public ViewInfo(
      String name,
      Column[] columns,
      @Nullable String comment,
      Representation[] representations,
      @Nullable String defaultCatalog,
      @Nullable String defaultSchema,
      Map<String, String> properties,
      @Nullable Audit auditInfo) {
    this.name = name;
    this.columns = columns == null ? new Column[0] : columns.clone();
    this.comment = comment;
    this.representations =
        representations == null ? new Representation[0] : representations.clone();
    this.defaultCatalog = defaultCatalog;
    this.defaultSchema = defaultSchema;
    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
    this.auditInfo = auditInfo;
  }

  /** Returns the view name. */
  public String name() {
    return name;
  }

  /** Returns the output columns of the view. */
  public Column[] columns() {
    return columns;
  }

  /** Returns the optional comment for the view. */
  @Nullable
  public String comment() {
    return comment;
  }

  /** Returns the view representations. */
  public Representation[] representations() {
    return representations;
  }

  /** Returns the optional default catalog used by the view definition. */
  @Nullable
  public String defaultCatalog() {
    return defaultCatalog;
  }

  /** Returns the optional default schema used by the view definition. */
  @Nullable
  public String defaultSchema() {
    return defaultSchema;
  }

  /** Returns the view properties. */
  public Map<String, String> properties() {
    return properties;
  }

  /** Returns the optional audit information for the view. */
  @Nullable
  public Audit auditInfo() {
    return auditInfo;
  }
}
