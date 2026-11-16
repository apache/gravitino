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
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.tag.TagValue;

/** A domain snapshot for event listeners, without storage identifiers. */
@DeveloperApi
public final class DomainInfo {
  private final String name;
  @Nullable private final String comment;
  private final Map<String, String> properties;
  private final Domain.Status status;
  @Nullable private final String parent;
  @Nullable private final TagValue backingTag;
  @Nullable private final Audit audit;

  /**
   * Creates domain information for a request or result.
   *
   * @param name The domain name.
   * @param comment The optional comment.
   * @param properties The properties, or null for none.
   * @param status The domain status.
   * @param parent The parent name, or null for a top-level domain.
   * @param backingTag The backing tag, or null if unbound.
   * @param audit The audit information, or null for a creation request.
   */
  public DomainInfo(
      String name,
      @Nullable String comment,
      @Nullable Map<String, String> properties,
      Domain.Status status,
      @Nullable String parent,
      @Nullable TagValue backingTag,
      @Nullable Audit audit) {
    this.name = name;
    this.comment = comment;
    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
    this.status = status;
    this.parent = parent;
    this.backingTag = backingTag;
    this.audit = audit;
  }
  /**
   * @return The domain name.
   */
  public String name() {
    return name;
  }
  /**
   * @return The optional comment.
   */
  @Nullable
  public String comment() {
    return comment;
  }
  /**
   * @return The immutable properties.
   */
  public Map<String, String> properties() {
    return properties;
  }
  /**
   * @return The visibility status.
   */
  public Domain.Status status() {
    return status;
  }
  /**
   * @return The parent name, or empty for a top-level domain.
   */
  public Optional<String> parent() {
    return Optional.ofNullable(parent);
  }
  /**
   * @return The backing tag, or empty if unbound.
   */
  public Optional<TagValue> backingTag() {
    return Optional.ofNullable(backingTag);
  }
  /**
   * @return The audit information, or null for a creation request.
   */
  @Nullable
  public Audit auditInfo() {
    return audit;
  }
}
