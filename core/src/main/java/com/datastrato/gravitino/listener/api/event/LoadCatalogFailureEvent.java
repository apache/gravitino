/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/** Represents an event that occurs when an attempt to load a catalog fails due to an exception. */
@DeveloperApi
public final class LoadCatalogFailureEvent extends CatalogFailureEvent {
  /**
   * Constructs a {@code LoadCatalogFailureEvent} instance.
   *
   * @param user The user who initiated the catalog loading operation.
   * @param identifier The identifier of the catalog that the loading attempt was made for.
   * @param exception The exception that was thrown during the catalog loading operation, offering
   *     insight into the issues encountered.
   */
  public LoadCatalogFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }
}
