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

  /**
   * Constructs a {@code CreateCatalogFailureEvent} instance, capturing detailed information about
   * the failed catalog creation attempt.
   *
   * @param user The user who initiated the catalog creation operation.
   * @param identifier The identifier of the catalog that was attempted to be created.
   * @param exception The exception that was thrown during the catalog creation operation, providing
   *     insight into what went wrong.
   * @param createCatalogRequest The original request information used to attempt to create the
   *     catalog. This includes details such as the intended catalog schema, properties, and other
   *     configuration options that were specified.
   */
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
