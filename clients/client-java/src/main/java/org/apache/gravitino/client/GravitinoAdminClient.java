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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.SupportsMetalakes;
import org.apache.gravitino.dto.requests.MetalakeCreateRequest;
import org.apache.gravitino.dto.requests.MetalakeSetRequest;
import org.apache.gravitino.dto.requests.MetalakeUpdateRequest;
import org.apache.gravitino.dto.requests.MetalakeUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.MetalakeListResponse;
import org.apache.gravitino.dto.responses.MetalakeResponse;
import org.apache.gravitino.exceptions.MetalakeAlreadyExistsException;
import org.apache.gravitino.exceptions.MetalakeInUseException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;

/**
 * Apache Gravitino Client for the administrator to interact with the Gravitino API, allowing the
 * client to list, load, create, and alter Metalakes.
 *
 * <p>Normal users should use {@link GravitinoClient} to connect with the Gravitino server.
 */
public class GravitinoAdminClient extends GravitinoClientBase implements SupportsMetalakes {

  /**
   * Constructs a new GravitinoClient with the given URI, authenticator and AuthDataProvider.
   *
   * @param uri The base URI for the Gravitino API.
   * @param authDataProvider The provider of the data which is used for authentication.
   * @param checkVersion Whether to check the version of the Gravitino server. Gravitino does not
   *     support the case that the client-side version is higher than the server-side version.
   * @param headers The base header for Gravitino API.
   * @param properties A map of properties (key-value pairs) used to configure the Gravitino client.
   */
  private GravitinoAdminClient(
      String uri,
      AuthDataProvider authDataProvider,
      boolean checkVersion,
      Map<String, String> headers,
      Map<String, String> properties) {
    super(uri, authDataProvider, checkVersion, headers, properties);
  }

  /**
   * Retrieves a list of Metalakes from the Gravitino API.
   *
   * @return An array of GravitinoMetalake objects representing the Metalakes.
   */
  @Override
  public GravitinoMetalake[] listMetalakes() {
    MetalakeListResponse resp =
        restClient.get(
            API_METALAKES_LIST_PATH,
            MetalakeListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.metalakeErrorHandler());
    resp.validate();

    return Arrays.stream(resp.getMetalakes())
        .map(o -> DTOConverters.toMetaLake(o, restClient))
        .toArray(GravitinoMetalake[]::new);
  }

  /**
   * Creates a new Metalake using the Gravitino API.
   *
   * @param name The name of the new Metalake.
   * @param comment The comment for the new Metalake.
   * @param properties The properties of the new Metalake.
   * @return A GravitinoMetalake instance representing the newly created Metalake.
   * @throws MetalakeAlreadyExistsException If a Metalake with the specified identifier already
   *     exists.
   */
  @Override
  public GravitinoMetalake createMetalake(
      String name, String comment, Map<String, String> properties)
      throws MetalakeAlreadyExistsException {
    checkMetalakeName(name);

    MetalakeCreateRequest req = new MetalakeCreateRequest(name, comment, properties);
    req.validate();

    MetalakeResponse resp =
        restClient.post(
            API_METALAKES_LIST_PATH,
            req,
            MetalakeResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.metalakeErrorHandler());
    resp.validate();

    return DTOConverters.toMetaLake(resp.getMetalake(), restClient);
  }

  /**
   * Alters a specific Metalake using the Gravitino API.
   *
   * @param name The name of the Metalake to be altered.
   * @param changes The changes to be applied to the Metalake.
   * @return A GravitinoMetalake instance representing the updated Metalake.
   * @throws NoSuchMetalakeException If the specified Metalake does not exist.
   * @throws IllegalArgumentException If the provided changes are invalid or not applicable.
   */
  @Override
  public GravitinoMetalake alterMetalake(String name, MetalakeChange... changes)
      throws NoSuchMetalakeException, IllegalArgumentException {
    checkMetalakeName(name);
    List<MetalakeUpdateRequest> reqs =
        Arrays.stream(changes)
            .map(DTOConverters::toMetalakeUpdateRequest)
            .collect(Collectors.toList());
    MetalakeUpdatesRequest updatesRequest = new MetalakeUpdatesRequest(reqs);
    updatesRequest.validate();

    MetalakeResponse resp =
        restClient.put(
            API_METALAKES_IDENTIFIER_PATH + name,
            updatesRequest,
            MetalakeResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.metalakeErrorHandler());
    resp.validate();

    return DTOConverters.toMetaLake(resp.getMetalake(), restClient);
  }

  /**
   * Drop a metalake with specified name. If the force flag is true, it will:
   *
   * <ul>
   *   <li>Cascade drop all sub-entities (tags, catalogs, schemas, tables, etc.) of the metalake in
   *       Gravitino store.
   *   <li>Drop the metalake even if it is in use.
   *   <li>External resources (e.g. database, table, etc.) associated with sub-entities will not be
   *       deleted unless it is managed (such as managed fileset).
   * </ul>
   *
   * If the force flag is false, it is equivalent to calling {@link #dropMetalake(String)}.
   *
   * @param name The name of the metalake.
   * @param force Whether to force the drop.
   * @return True if the metalake was dropped, false if the metalake does not exist.
   * @throws NonEmptyEntityException If the metalake is not empty and force is false.
   * @throws MetalakeInUseException If the metalake is in use and force is false.
   */
  @Override
  public boolean dropMetalake(String name, boolean force)
      throws NonEmptyEntityException, MetalakeInUseException {
    checkMetalakeName(name);
    Map<String, String> params = new HashMap<>();
    params.put("force", String.valueOf(force));

    DropResponse resp =
        restClient.delete(
            API_METALAKES_IDENTIFIER_PATH + name,
            params,
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.metalakeErrorHandler());
    resp.validate();
    return resp.dropped();
  }

  @Override
  public void enableMetalake(String name) throws NoSuchMetalakeException {
    MetalakeSetRequest req = new MetalakeSetRequest(true);

    ErrorResponse resp =
        restClient.patch(
            API_METALAKES_IDENTIFIER_PATH + name,
            req,
            ErrorResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.metalakeErrorHandler());

    if (resp.getCode() == 0) {
      return;
    }

    ErrorHandlers.metalakeErrorHandler().accept(resp);
  }

  @Override
  public void disableMetalake(String name) throws NoSuchMetalakeException {
    MetalakeSetRequest req = new MetalakeSetRequest(false);

    ErrorResponse resp =
        restClient.patch(
            API_METALAKES_IDENTIFIER_PATH + name,
            req,
            ErrorResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.metalakeErrorHandler());

    if (resp.getCode() == 0) {
      return;
    }

    ErrorHandlers.metalakeErrorHandler().accept(resp);
  }

  /**
   * Creates a new builder for constructing a GravitinoClient.
   *
   * @param uri The base URI for the Gravitino API.
   * @return A new instance of the Builder class for constructing a GravitinoClient.
   */
  public static AdminClientBuilder builder(String uri) {
    return new AdminClientBuilder(uri);
  }

  /** Builder class for constructing a GravitinoAdminClient. */
  public static class AdminClientBuilder extends GravitinoClientBase.Builder<GravitinoAdminClient> {

    /**
     * The private constructor for the Builder class.
     *
     * @param uri The base URI for the Gravitino API.
     */
    protected AdminClientBuilder(String uri) {
      super(uri);
    }

    /**
     * Builds a new GravitinoClient instance.
     *
     * @return A new instance of GravitinoClient with the specified base URI.
     * @throws IllegalArgumentException If the base URI is null or empty.
     */
    @Override
    public GravitinoAdminClient build() {
      Preconditions.checkArgument(
          uri != null && !uri.isEmpty(), "The argument 'uri' must be a valid URI");
      return new GravitinoAdminClient(uri, authDataProvider, checkVersion, headers, properties);
    }
  }
}
