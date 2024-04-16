/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.MetalakeInfo;

/** Represents an event that is generated when a Metalake is successfully loaded. */
@DeveloperApi
public final class LoadMetalakeEvent extends MetalakeEvent {
  private final MetalakeInfo loadedMetalakeInfo;

  /**
   * Constructs an instance of {@code LoadMetalakeEvent}.
   *
   * @param user The username of the individual who initiated the metalake loading.
   * @param identifier The unique identifier of the metalake that was loaded.
   * @param metalakeInfo The state of the metalake post-loading.
   */
  public LoadMetalakeEvent(String user, NameIdentifier identifier, MetalakeInfo metalakeInfo) {
    super(user, identifier);
    this.loadedMetalakeInfo = metalakeInfo;
  }

  /**
   * Retrieves detailed information about the Metalake that was successfully loaded.
   *
   * @return A {@link MetalakeInfo} instance containing comprehensive details of the Metalake,
   *     including its configuration, properties, and state at the time of loading.
   */
  public MetalakeInfo loadedMetalakeInfo() {
    return loadedMetalakeInfo;
  }
}
