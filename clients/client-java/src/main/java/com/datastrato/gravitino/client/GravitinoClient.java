/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.CatalogChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.CatalogAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.google.common.base.Preconditions;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gravitino Client for an user to interact with the Gravitino API, allowing the client to list,
 * load, create, and alter Catalog.
 *
 * <p>It uses an underlying {@link RESTClient} to send HTTP requests and receive responses from the
 * API.
 */
public class GravitinoClient extends GravitinoClientBase {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoClient.class);

  private final GravitinoMetaLake metaLake;

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
    this.metaLake = loadMetalake(NameIdentifier.of(metalakeName));
  }

  /**
   * Get the current metalake object
   *
   * @return the {@link GravitinoMetaLake} object
   * @throws NoSuchMetalakeException if the metalake with specified name does not exist.
   */
  public GravitinoMetaLake getMetaLake() {
    return metaLake;
  }

  /**
   * List all catalogs in the metalake.
   *
   * @return The list of catalog's name identifiers.
   * @throws NoSuchMetalakeException If the metalake with namespace does not exist.
   */
  public NameIdentifier[] listCatalogs() throws NoSuchMetalakeException {
    return getMetaLake().listCatalogs(Namespace.ofCatalog(this.getMetaLake().name()));
  }

  /**
   * Load a catalog by its identifier.
   *
   * @param catalogName the name of the catalog.
   * @return The catalog.
   * @throws NoSuchCatalogException If the catalog does not exist.
   */
  public Catalog loadCatalog(String catalogName) throws NoSuchCatalogException {
    return getMetaLake().loadCatalog(ofCatalogIdent(catalogName));
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
    return getMetaLake()
        .createCatalog(ofCatalogIdent(catalogName), type, provider, comment, properties);
  }

  /**
   * Alter a catalog with specified identifier.
   *
   * @param catalogName the name of the catalog.
   * @param changes the changes to apply to the catalog.
   * @return The altered catalog.
   * @throws NoSuchCatalogException If the catalog does not exist.
   * @throws IllegalArgumentException If the changes cannot be applied to the catalog.
   */
  public Catalog alterCatalog(String catalogName, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException {
    return getMetaLake().alterCatalog(ofCatalogIdent(catalogName), changes);
  }

  /**
   * Drop a catalog with specified identifier.
   *
   * @param catalogName the name of the catalog.
   * @return True if the catalog was dropped, false otherwise.
   */
  public boolean dropCatalog(String catalogName) {
    return getMetaLake().dropCatalog(ofCatalogIdent(catalogName));
  }

  /**
   * Get a catalog identifier from its name.
   *
   * @param catalogName the name of the catalog.
   * @return The {@link NameIdentifier} of the catalog.
   */
  public NameIdentifier ofCatalogIdent(String catalogName) {
    return NameIdentifier.ofCatalog(this.getMetaLake().name(), catalogName);
  }

  /**
   * Creates a new builder for constructing a GravitinoClient.
   *
   * @param uri The base URI for the Gravitino API.
   * @return A new instance of the Builder class for constructing a GravitinoClient.
   */
  public static Builder<GravitinoClient> builder(String uri) {
    return new ClientBuilder(uri);
  }

  /** Builder class for constructing a GravitinoClient. */
  static class ClientBuilder extends GravitinoClientBase.Builder<GravitinoClient> {

    /**
     * The private constructor for the Builder class.
     *
     * @param uri The base URI for the Gravitino API.
     */
    protected ClientBuilder(String uri) {
      super(uri);
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
