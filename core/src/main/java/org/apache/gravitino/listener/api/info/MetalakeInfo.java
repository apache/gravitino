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
import org.apache.gravitino.Metalake;
import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Provides access to metadata about a Metalake instance, designed for use by event listeners. This
 * class encapsulates the essential attributes of a Metalake, including its name, optional
 * description, properties, and audit information.
 */
@DeveloperApi
public final class MetalakeInfo {
  private final String name;
  @Nullable private final String comment;
  private final Map<String, String> properties;
  @Nullable private final Audit audit;

  /**
   * Constructs MetalakeInfo from an existing Metalake object.
   *
   * @param metalake The Metalake instance to extract information from.
   */
  public MetalakeInfo(Metalake metalake) {
    this(metalake.name(), metalake.comment(), metalake.properties(), metalake.auditInfo());
  }

  /**
   * Directly constructs MetalakeInfo with specified details.
   *
   * @param name The name of the Metalake.
   * @param comment An optional description for the Metalake.
   * @param properties A map of properties associated with the Metalake.
   * @param audit Optional audit details for the Metalake.
   */
  public MetalakeInfo(String name, String comment, Map<String, String> properties, Audit audit) {
    this.name = name;
    this.comment = comment;
    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
    this.audit = audit;
  }

  /**
   * Returns the audit information of the Metalake.
   *
   * @return Audit details, or null if not available.
   */
  @Nullable
  public Audit auditInfo() {
    return audit;
  }

  /**
   * Returns the name of the Metalake.
   *
   * @return The Metalake's name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the optional comment describing the Metalake.
   *
   * @return The comment, or null if not provided.
   */
  @Nullable
  public String comment() {
    return comment;
  }

  /**
   * Returns the properties of the Metalake.
   *
   * @return A map of Metalake properties.
   */
  public Map<String, String> properties() {
    return properties;
  }
}
