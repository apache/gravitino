/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.CatalogInfo;

/** Represents an event that is activated upon the successful creation of a catalog. */
@DeveloperApi
public class CreateCatalogEvent extends CatalogEvent {
  private final CatalogInfo createdCatalogInfo;

  public CreateCatalogEvent(String user, NameIdentifier identifier, CatalogInfo catalogInfo) {
    super(user, identifier);
    this.createdCatalogInfo = catalogInfo;
  }

  /**
   * Provides the final state of the catalog as it is presented to the user following the successful
   * creation.
   *
   * @return A {@link CatalogInfo} object that encapsulates the detailed characteristics of the
   *     newly created catalog.
   */
  public CatalogInfo createdCatalogInfo() {
    return createdCatalogInfo;
  }
}
