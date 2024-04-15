/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.CatalogChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.CatalogInfo;

/** Represents an event triggered upon the successful creation of a catalog. */
@DeveloperApi
public final class AlterCatalogEvent extends CatalogEvent {
  private final CatalogInfo updatedCatalogInfo;
  private final CatalogChange[] catalogChanges;

  public AlterCatalogEvent(
      String user,
      NameIdentifier identifier,
      CatalogChange[] catalogChanges,
      CatalogInfo updatedCatalogInfo) {
    super(user, identifier);
    this.catalogChanges = catalogChanges.clone();
    this.updatedCatalogInfo = updatedCatalogInfo;
  }

  /**
   * Retrieves the final state of the catalog as it was returned to the user after successful
   * creation.
   *
   * @return A {@link CatalogInfo} instance encapsulating the comprehensive details of the newly
   *     created catalog.
   */
  public CatalogInfo updatedCatalogInfo() {
    return updatedCatalogInfo;
  }

  public CatalogChange[] catalogChanges() {
    return catalogChanges;
  }
}
