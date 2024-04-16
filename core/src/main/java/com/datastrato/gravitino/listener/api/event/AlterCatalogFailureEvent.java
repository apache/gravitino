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

  /**
   * Constructs an {@code AlterCatalogFailureEvent} instance, capturing detailed information about
   * the failed catalog alteration attempt.
   *
   * @param user The user who initiated the catalog alteration operation.
   * @param identifier The identifier of the catalog that was attempted to be altered.
   * @param exception The exception that was thrown during the catalog alteration operation.
   * @param catalogChanges The changes that were attempted on the catalog.
   */
  public AlterCatalogFailureEvent(
      String user, NameIdentifier identifier, Exception exception, CatalogChange[] catalogChanges) {
    super(user, identifier, exception);
    this.catalogChanges = catalogChanges.clone();
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
