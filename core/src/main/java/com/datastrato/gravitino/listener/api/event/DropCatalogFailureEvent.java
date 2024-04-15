/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is generated when an attempt to drop a catalog fails due to an
 * exception.
 */
@DeveloperApi
public final class DropCatalogFailureEvent extends CatalogFailureEvent {
  public DropCatalogFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }
}
