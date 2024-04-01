/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.CatalogChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.SupportsCatalogs;
import com.datastrato.gravitino.exceptions.CatalogAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.google.common.base.Preconditions;
import java.util.Map;

/**
 * Gravitino Client for an user to interact with the Gravitino API, allowing the client to list,
 * load, create, and alter Catalog.
 *
 * <p>It uses an underlying {@link RESTClient} to send HTTP requests and receive responses from the
 * API.
 */
public class GravitinoClient extends GravitinoClientBase implements SupportsCatalogs {

  private final GravitinoMetalake metalake;

  /**
   * Constructs a new GravitinoClient with the given URI, authenticator and AuthDataProvider.
   *
   * @param uri The base URI for the Gravitino API.
   * @param metalakeName The specified metalake name.
   * @param authDataProvider The provider of the data which is used for authentication.
   * @throws NoSuchMetalakeException if the metalake with specified name does not exist.
   */
  private GravitinoClient(String uri, String metalakeName, AuthDataProvider authDataProvider) {
    super(uri, authDataProvider);
    this.metalake = loadMetalake(NameIdentifier.of(metalakeName));
  }

  /**
   * Get the current metalake object
   *
   * @return the {@link GravitinoMetalake} object
   * @throws NoSuchMetalakeException if the metalake with specified name does not exist.
   */
  private GravitinoMetalake getMetalake() {
    return metalake;
  }

  @Override
  public NameIdentifier[] listCatalogs(Namespace namespace) throws NoSuchMetalakeException {
    return getMetalake().listCatalogs(namespace);
  }

  @Override
  public Catalog[] listCatalogsInfo(Namespace namespace) throws NoSuchMetalakeException {
    return getMetalake().listCatalogsInfo(namespace);
  }

  @Override
  public Catalog loadCatalog(NameIdentifier ident) throws NoSuchCatalogException {
    return getMetalake().loadCatalog(ident);
  }

  @Override
  public Catalog createCatalog(
      NameIdentifier ident,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException {
    return getMetalake().createCatalog(ident, type, provider, comment, properties);
  }

  @Override
  public Catalog alterCatalog(NameIdentifier ident, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException {
    return getMetalake().alterCatalog(ident, changes);
  }

  @Override
  public boolean dropCatalog(NameIdentifier ident) {
    return getMetalake().dropCatalog(ident);
  }

  /**
   * List all catalogs in the current metalake.
   *
   * @return The list of catalog's name identifiers.
   * @throws NoSuchMetalakeException If the metalake with namespace does not exist.
   */
  public NameIdentifier[] listCatalogs() throws NoSuchMetalakeException {
    return getMetalake().listCatalogs();
  }

  /**
   * Load a catalog by its name.
   *
   * @param catalogName the name of the catalog.
   * @return The catalog.
   * @throws NoSuchCatalogException If the catalog does not exist.
   */
  public Catalog loadCatalog(String catalogName) throws NoSuchCatalogException {
    return getMetalake().loadCatalog(catalogName);
  }

  /**
   * Check if a catalog exists.
   *
   * @param catalogName The name of the catalog.
   * @return True if the catalog exists, false otherwise.
   */
  public boolean catalogExists(String catalogName) {
    return getMetalake().catalogExists(catalogName);
  }

  /**
   * Create a catalog with specified name.
   *
   * <p>The parameter "provider" is a short name of the catalog, used to tell Gravitino which
   * catalog should be created.
   *
   * @param catalogName the name of the catalog.
   * @param type the type of the catalog.
   * @param comment the comment of the catalog.
   * @param provider the provider of the catalog.
   * @param properties the properties of the catalog.
   * @return The created catalog.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   * @throws CatalogAlreadyExistsException If the catalog already exists.
   */
  public Catalog createCatalog(
      String catalogName,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException {
    return getMetalake().createCatalog(catalogName, type, provider, comment, properties);
  }

  /**
   * Alter a catalog with specified name.
   *
   * @param catalogName the name of the catalog.
   * @param changes the changes to apply to the catalog.
   * @return The altered catalog.
   * @throws NoSuchCatalogException If the catalog does not exist.
   * @throws IllegalArgumentException If the changes cannot be applied to the catalog.
   */
  public Catalog alterCatalog(String catalogName, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException {
    return getMetalake().alterCatalog(catalogName, changes);
  }

  /**
   * Drop a catalog with specified name.
   *
   * @param catalogName the identifier of the catalog.
   * @return True if the catalog was dropped, false otherwise.
   */
  public boolean dropCatalog(String catalogName) {
    return getMetalake().dropCatalog(catalogName);
  }

  /**
   * Creates a new builder for constructing a GravitinoClient.
   *
   * @param uri The base URI for the Gravitino API.
   * @return A new instance of the Builder class for constructing a GravitinoClient.
   */
  public static ClientBuilder builder(String uri) {
    return new ClientBuilder(uri);
  }

  /** Builder class for constructing a GravitinoClient. */
  public static class ClientBuilder extends GravitinoClientBase.Builder<GravitinoClient> {

    /** The name of the metalake that the client is working on. */
    protected String metalakeName;

    /**
     * The private constructor for the Builder class.
     *
     * @param uri The base URI for the Gravitino API.
     */
    protected ClientBuilder(String uri) {
      super(uri);
    }

    /**
     * Optional, set the metalake name for this client.
     *
     * @param metalakeName The name of the metalake that the client is working on.
     * @return This Builder instance for method chaining.
     */
    public ClientBuilder withMetalake(String metalakeName) {
      this.metalakeName = metalakeName;
      return this;
    }

    /**
     * Builds a new GravitinoClient instance.
     *
     * @return A new instance of GravitinoClient with the specified base URI.
     * @throws IllegalArgumentException If the base URI is null or empty.
     * @throws NoSuchMetalakeException if the metalake with specified name does not exist.
     */
    @Override
    public GravitinoClient build() {
      Preconditions.checkArgument(
          uri != null && !uri.isEmpty(), "The argument 'uri' must be a valid URI");
      Preconditions.checkArgument(
          metalakeName != null && !metalakeName.isEmpty(),
          "The argument 'metalakeName' must be a valid name");

      return new GravitinoClient(uri, metalakeName, authDataProvider);
    }
  }
}
