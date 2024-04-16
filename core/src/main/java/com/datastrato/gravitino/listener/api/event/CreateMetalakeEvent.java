/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.MetalakeInfo;

/** Represents an event triggered upon the successful creation of a Metalake. */
@DeveloperApi
public final class CreateMetalakeEvent extends MetalakeEvent {
  private final MetalakeInfo createdMetalakeInfo;
  /**
   * Constructs an instance of {@code CreateMetalakeEvent}, capturing essential details about the
   * successful creation of a metalake.
   *
   * @param user The username of the individual who initiated the metalake creation.
   * @param identifier The unique identifier of the metalake that was created.
   * @param createdMetalakeInfo The final state of the metalake post-creation.
   */
  public CreateMetalakeEvent(
      String user, NameIdentifier identifier, MetalakeInfo createdMetalakeInfo) {
    super(user, identifier);
    this.createdMetalakeInfo = createdMetalakeInfo;
  }

  /**
   * Retrieves the final state of the Metalake as it was returned to the user after successful
   * creation.
   *
   * @return A {@link MetalakeInfo} instance encapsulating the comprehensive details of the newly
   *     created Metalake.
   */
  public MetalakeInfo createdMetalakeInfo() {
    return createdMetalakeInfo;
  }
}
