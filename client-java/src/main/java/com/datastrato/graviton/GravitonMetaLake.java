/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton;

import com.datastrato.graviton.client.RESTClient;
import com.datastrato.graviton.dto.AuditDTO;
import com.datastrato.graviton.dto.MetalakeDTO;
import com.datastrato.graviton.exceptions.CatalogAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import com.google.common.base.Preconditions;
import java.util.Map;

/**
 * GravitonMetaLake is a specific implementation of MetalakeDTO that also supports Catalog
 * operations.
 */
public class GravitonMetaLake extends MetalakeDTO implements SupportsCatalogs {

  private final RESTClient restClient;

  /**
   * Constructs a new Graviton MetaLake instance with the provided details.
   *
   * @param name The name of the Graviton MetaLake.
   * @param comment The comment for the Graviton MetaLake.
   * @param properties The properties of the Graviton MetaLake.
   * @param auditDTO The audit information for the Graviton MetaLake.
   * @param restClient The REST client used for API communication.
   */
  GravitonMetaLake(
      String name,
      String comment,
      Map<String, String> properties,
      AuditDTO auditDTO,
      RESTClient restClient) {
    super(name, comment, properties, auditDTO);
    this.restClient = restClient;
  }

  /**
   * Retrieves an array of Catalog names associated with this Graviton MetaLake.
   *
   * @param namespace The namespace to filter Catalogs.
   * @return An array of name identifier's representing the Catalogs.
   * @throws NoSuchMetalakeException If the Graviton MetaLake does not exist.
   */
  @Override
  public NameIdentifier[] listCatalogs(Namespace namespace) throws NoSuchMetalakeException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /**
   * Loads a specific Catalog associated with this Graviton MetaLake.
   *
   * @param ident The identifier of the Catalog to be loaded.
   * @return A Catalog instance representing the loaded Catalog.
   * @throws NoSuchCatalogException If the specified Catalog does not exist.
   */
  @Override
  public Catalog loadCatalog(NameIdentifier ident) throws NoSuchCatalogException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /**
   * Creates a new Catalog associated with this Graviton MetaLake.
   *
   * @param ident The identifier of the new Catalog.
   * @param type The type of the new Catalog.
   * @param comment The comment for the new Catalog.
   * @param properties The properties of the new Catalog.
   * @return A Catalog instance representing the newly created Catalog.
   * @throws NoSuchMetalakeException If the Graviton MetaLake does not exist.
   * @throws CatalogAlreadyExistsException If a Catalog with the specified identifier already
   *     exists.
   */
  @Override
  public Catalog createCatalog(
      NameIdentifier ident, Catalog.Type type, String comment, Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /**
   * Alters a specific Catalog associated with this GravitonMetaLake.
   *
   * @param ident The identifier of the Catalog to be altered.
   * @param changes The changes to be applied to the Catalog.
   * @return A Catalog instance representing the updated Catalog.
   * @throws NoSuchCatalogException If the specified Catalog does not exist.
   * @throws IllegalArgumentException If the provided changes are invalid.
   */
  @Override
  public Catalog alterCatalog(NameIdentifier ident, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /**
   * Drops a specific Catalog associated with this Graviton MetaLake.
   *
   * @param ident The identifier of the Catalog to be dropped.
   * @return True if the drop operation was successful, false otherwise.
   */
  @Override
  public boolean dropCatalog(NameIdentifier ident) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /** The Builder class for GravitonMetaLake. */
  static class Builder extends MetalakeDTO.Builder<Builder> {
    private RESTClient restClient;

    /**
     * Sets the RESTClient for the GravitonMetaLake being built.
     *
     * @param restClient The REST client for API communication.
     * @return The Builder instance with the updated REST client.
     */
    Builder withRestClient(RESTClient restClient) {
      this.restClient = restClient;
      return this;
    }

    /**
     * Builds the GravitonMetaLake instance.
     *
     * @return A new Graviton MetaLake instance with the specified details.
     * @throws NullPointerException If the REST client is not set.
     * @throws IllegalArgumentException If the name is null or empty, or the audit is null.
     */
    @Override
    public GravitonMetaLake build() {
      Preconditions.checkNotNull(restClient, "restClient must be set");
      Preconditions.checkArgument(
          name != null && !name.isEmpty(), "name must not be null or empty");
      Preconditions.checkArgument(audit != null, "audit must be non-null");
      return new GravitonMetaLake(name, comment, properties, audit, restClient);
    }
  }
}
