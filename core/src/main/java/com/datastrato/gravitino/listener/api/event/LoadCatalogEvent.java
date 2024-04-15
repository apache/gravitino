/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.CatalogInfo;

/** Represents an event triggered upon the successful loading of a catalog. */
@DeveloperApi
public final class LoadCatalogEvent extends CatalogEvent {
  private final CatalogInfo loadedCatalogInfo;

  public LoadCatalogEvent(String user, NameIdentifier identifier, CatalogInfo loadedCatalogInfo) {
    super(user, identifier);
    this.loadedCatalogInfo = loadedCatalogInfo;
  }

  /**
   * Retrieves the state of the catalog as it was made available to the user after successful
   * loading.
   *
   * @return A {@link CatalogInfo} instance encapsulating the details of the catalog as loaded.
   */
  public CatalogInfo loadedCatalogInfo() {
    return loadedCatalogInfo;
  }
}
