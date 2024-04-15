/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.CatalogInfo;

/**
 * Represents an event that is generated when an attempt to create a catalog fails due to an
 * exception.
 */
@DeveloperApi
public final class CreateCatalogFailureEvent extends CatalogFailureEvent {
  private final CatalogInfo createCatalogRequest;

  public CreateCatalogFailureEvent(
      String user,
      NameIdentifier identifier,
      Exception exception,
      CatalogInfo createCatalogRequest) {
    super(user, identifier, exception);
    this.createCatalogRequest = createCatalogRequest;
  }

  /**
   * Retrieves the original request information for the attempted catalog creation.
   *
   * @return The {@link CatalogInfo} instance representing the request information for the failed
   *     catalog creation attempt.
   */
  public CatalogInfo createCatalogRequest() {
    return createCatalogRequest;
  }
}
