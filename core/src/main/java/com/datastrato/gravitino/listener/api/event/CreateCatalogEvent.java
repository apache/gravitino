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

  /**
   * Constructs an instance of {@code CreateCatalogEvent}, capturing essential details about the
   * successful creation of a catalog.
   *
   * @param user The username of the individual who initiated the catalog creation.
   * @param identifier The unique identifier of the catalog that was created.
   * @param createdCatalogInfo The final state of the catalog post-creation.
   */
  public CreateCatalogEvent(
      String user, NameIdentifier identifier, CatalogInfo createdCatalogInfo) {
    super(user, identifier);
    this.createdCatalogInfo = createdCatalogInfo;
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
