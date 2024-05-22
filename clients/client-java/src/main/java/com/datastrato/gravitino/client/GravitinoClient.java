/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.CatalogChange;
import com.datastrato.gravitino.NameIdentifier;
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
   * @param checkVersion Whether to check the version of the Gravitino server. Gravitino does not
   *     support the case that the client-side version is higher than the server-side version.
   * @param headers The base header for Gravitino API.
   * @throws NoSuchMetalakeException if the metalake with specified name does not exist.
   */
  private GravitinoClient(
      String uri,
      String metalakeName,
      AuthDataProvider authDataProvider,
      boolean checkVersion,
      Map<String, String> headers) {
    super(uri, authDataProvider, checkVersion, headers);
    this.metalake = loadMetalake(metalakeName);
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
  public NameIdentifier[] listCatalogs() throws NoSuchMetalakeException {
    return getMetalake().listCatalogs();
  }

  @Override
  public Catalog[] listCatalogsInfo() throws NoSuchMetalakeException {
    return getMetalake().listCatalogsInfo();
  }

  @Override
  public Catalog loadCatalog(String catalogName) throws NoSuchCatalogException {
    return getMetalake().loadCatalog(catalogName);
  }

  @Override
  public Catalog createCatalog(
      String catalogName,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException {
    return getMetalake().createCatalog(catalogName, type, provider, comment, properties);
  }

  @Override
  public Catalog alterCatalog(String catalogName, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException {
    return getMetalake().alterCatalog(catalogName, changes);
  }

  @Override
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

      return new GravitinoClient(uri, metalakeName, authDataProvider, checkVersion, headers);
    }
  }
}
