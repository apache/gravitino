/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/** Represents an event that is generated after a catalog is successfully dropped. */
@DeveloperApi
public final class DropCatalogEvent extends CatalogEvent {
  private final boolean isExists;

  public DropCatalogEvent(String user, NameIdentifier identifier, boolean isExists) {
    super(user, identifier);
    this.isExists = isExists;
  }

  /**
   * Retrieves the existence status of the catalog at the time of the drop operation.
   *
   * @return A boolean value indicating whether the catalog existed. {@code true} if the catalog
   *     existed, otherwise {@code false}.
   */
  public boolean isExists() {
    return isExists;
  }
}
