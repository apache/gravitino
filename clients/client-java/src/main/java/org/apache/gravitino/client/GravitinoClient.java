/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.client;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.SupportsCatalogs;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;

/**
 * Apache Gravitino Client for an user to interact with the Gravitino API, allowing the client to
 * list, load, create, and alter Catalog.
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
  public String[] listCatalogs() throws NoSuchMetalakeException {
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

  /**
   * Test whether a catalog can be created successfully with the specified parameters, without
   * actually creating it.
   *
   * @param catalogName the name of the catalog.
   * @param type the type of the catalog.
   * @param provider the provider of the catalog.
   * @param comment the comment of the catalog.
   * @param properties the properties of the catalog.
   * @throws Exception if the test failed.
   */
  @Override
  public void testConnection(
      String catalogName,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws Exception {
    getMetalake().testConnection(catalogName, type, provider, comment, properties);
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
