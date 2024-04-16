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

  /**
   * Constructs an instance of {@code AlterCatalogEvent}, encapsulating the key details about the
   * successful alteration of a catalog.
   *
   * @param user The username of the individual responsible for initiating the catalog alteration.
   * @param identifier The unique identifier of the altered catalog, serving as a clear reference
   *     point for the catalog in question.
   * @param catalogChanges An array of {@link CatalogChange} objects representing the specific
   *     changes applied to the catalog during the alteration process.
   * @param updatedCatalogInfo The post-alteration state of the catalog.
   */
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

  /**
   * Retrieves the specific changes that were made to the catalog during the alteration process.
   *
   * @return An array of {@link CatalogChange} objects detailing each modification applied to the
   *     catalog.
   */
  public CatalogChange[] catalogChanges() {
    return catalogChanges;
  }
}
