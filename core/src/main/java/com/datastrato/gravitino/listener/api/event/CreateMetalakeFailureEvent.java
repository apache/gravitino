/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.MetalakeInfo;

/**
 * Represents an event that is generated when an attempt to create a Metalake fails due to an
 * exception.
 */
@DeveloperApi
public final class CreateMetalakeFailureEvent extends MetalakeFailureEvent {
  private final MetalakeInfo createMetalakeRequest;

  public CreateMetalakeFailureEvent(
      String user,
      NameIdentifier identifier,
      Exception exception,
      MetalakeInfo createMetalakeRequest) {
    super(user, identifier, exception);
    this.createMetalakeRequest = createMetalakeRequest;
  }

  /**
   * Retrieves the original request information for the attempted Metalake creation.
   *
   * @return The {@link MetalakeInfo} instance representing the request information for the failed
   *     Metalake creation attempt.
   */
  public MetalakeInfo createMetalakeRequest() {
    return createMetalakeRequest;
  }
}
