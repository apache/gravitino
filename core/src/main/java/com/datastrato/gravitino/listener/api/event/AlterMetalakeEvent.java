/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.MetalakeChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.MetalakeInfo;

/** Represents an event fired when a metalake is successfully altered. */
@DeveloperApi
public final class AlterMetalakeEvent extends MetalakeEvent {
  private final MetalakeInfo updatedMetalakeInfo;
  private final MetalakeChange[] metalakeChanges;

  /**
   * Constructs an instance of {@code AlterMetalakeEvent}, encapsulating the key details about the
   * successful alteration of a metalake.
   *
   * @param user The username of the individual responsible for initiating the metalake alteration.
   * @param identifier The unique identifier of the altered metalake, serving as a clear reference
   *     point for the metalake in question.
   * @param metalakeChanges An array of {@link MetalakeChange} objects representing the specific
   *     changes applied to the metalake during the alteration process.
   * @param updatedMetalakeInfo The post-alteration state of the metalake.
   */
  public AlterMetalakeEvent(
      String user,
      NameIdentifier identifier,
      MetalakeChange[] metalakeChanges,
      MetalakeInfo updatedMetalakeInfo) {
    super(user, identifier);
    this.metalakeChanges = metalakeChanges.clone();
    this.updatedMetalakeInfo = updatedMetalakeInfo;
  }

  /**
   * Retrieves the updated state of the metalake after the successful alteration.
   *
   * @return A {@link MetalakeInfo} instance encapsulating the details of the altered metalake.
   */
  public MetalakeInfo updatedMetalakeInfo() {
    return updatedMetalakeInfo;
  }

  /**
   * Retrieves the specific changes that were made to the metalake during the alteration process.
   *
   * @return An array of {@link MetalakeChange} objects detailing each modification applied to the
   *     metalake.
   */
  public MetalakeChange[] metalakeChanges() {
    return metalakeChanges;
  }
}
