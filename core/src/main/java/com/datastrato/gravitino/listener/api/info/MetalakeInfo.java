/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.info;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.Metalake;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nullable;

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
