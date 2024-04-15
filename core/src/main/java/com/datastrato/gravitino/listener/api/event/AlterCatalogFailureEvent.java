/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.CatalogChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is generated when an attempt to create a catalog fails due to an
 * exception.
 */
@DeveloperApi
public final class AlterCatalogFailureEvent extends CatalogFailureEvent {
  private final CatalogChange[] catalogChanges;

  public AlterCatalogFailureEvent(
      String user, NameIdentifier identifier, Exception exception, CatalogChange[] catalogChanges) {
    super(user, identifier, exception);
    this.catalogChanges = catalogChanges.clone();
  }

  public CatalogChange[] catalogChanges() {
    return catalogChanges;
  }
}
