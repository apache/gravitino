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
package org.apache.gravitino.server.web;

import com.google.common.collect.Maps;
import java.lang.reflect.Parameter;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.Version;
import org.apache.gravitino.audit.FilesetAuditConstants;
import org.apache.gravitino.audit.FilesetDataOperation;
import org.apache.gravitino.audit.InternalClientType;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auxiliary.AuxiliaryServiceManager;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.dto.HealthCheckDTO;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.HealthResponse;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
  private static final String REMOTE_USER = "gravitino";

  // Matches gravitino.auxService.names / AuxiliaryServiceManager's registration key.
  private static final String ICEBERG_REST_SERVICE_NAME = "iceberg-rest";
  // Keys below are read from AuxiliaryServiceManager.getAuxServiceConfig, which already strips
  // the gravitino.iceberg-rest. (or deprecated gravitino.auxService.iceberg-rest.) prefix, so
  // they must NOT be re-prefixed here.
  // The provider name used by the Iceberg REST server itself; see
  // IcebergConstants.ICEBERG_REST_CATALOG_CONFIG_PROVIDER and DynamicIcebergConfigProvider. The
  // shared server utilities do not depend on Iceberg implementation modules.
  private static final String ICEBERG_CATALOG_CONFIG_PROVIDER_KEY = "catalog-config-provider";
  private static final String ICEBERG_DYNAMIC_CONFIG_PROVIDER_NAME = "dynamic-config-provider";
  // The post-strip key used by the Iceberg REST server itself; see
  // IcebergConstants.GRAVITINO_METALAKE and DynamicIcebergConfigProvider.
  private static final String ICEBERG_SERVED_METALAKE_KEY = "gravitino-metalake";
  // Overrides the listener-derived endpoint; see docs/iceberg-rest-service.md.
  private static final String ICEBERG_ADVERTISED_URI_KEY = "advertised-uri";
  private static final String ICEBERG_HOST_KEY = "host";
  private static final String ICEBERG_HTTP_PORT_KEY = "httpPort";
  private static final String ICEBERG_HTTPS_PORT_KEY = "httpsPort";
  private static final String ICEBERG_ENABLE_HTTPS_KEY = "enableHttps";
  // Match IcebergConfig.DEFAULT_ICEBERG_REST_SERVICE_HTTP_PORT/HTTPS_PORT. JettyServerConfig's
  // defaults belong to the Gravitino server (8090/8433), not the Iceberg REST server.
  private static final int ICEBERG_DEFAULT_HTTP_PORT = 9001;
  private static final int ICEBERG_DEFAULT_HTTPS_PORT = 9433;
  private static final String ICEBERG_DEFAULT_HOST = "0.0.0.0";

  private Utils() {}

  public static String remoteUser(HttpServletRequest httpRequest) {
    return Optional.ofNullable(httpRequest.getRemoteUser()).orElse(REMOTE_USER);
  }

  public static <T> Response ok(T t) {
    return Response.status(Response.Status.OK).entity(t).type(MediaType.APPLICATION_JSON).build();
  }

  public static Response created() {
    return Response.status(Response.Status.CREATED).type(MediaType.APPLICATION_JSON).build();
  }

  public static Response tooManyRequests() {
    return Response.status(Status.TOO_MANY_REQUESTS).type(MediaType.APPLICATION_JSON).build();
  }

  public static Response ok() {
    return Response.status(Response.Status.NO_CONTENT).type(MediaType.APPLICATION_JSON).build();
  }

  public static Response illegalArguments(String message) {
    return illegalArguments(IllegalArgumentException.class.getSimpleName(), message, null);
  }

  public static Response illegalArguments(String message, Throwable throwable) {
    return illegalArguments(throwable.getClass().getSimpleName(), message, throwable);
  }

  public static Response illegalArguments(String type, String message, Throwable throwable) {
    return Response.status(Response.Status.BAD_REQUEST)
        .entity(ErrorResponse.illegalArguments(type, message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  public static Response connectionFailed(String message) {
    return Response.status(Response.Status.BAD_GATEWAY)
        .entity(ErrorResponse.connectionFailed(message))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  public static Response connectionFailed(String message, Throwable throwable) {
    return Response.status(Response.Status.BAD_GATEWAY)
        .entity(ErrorResponse.connectionFailed(message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  public static Response internalError(String message) {
    return internalError(message, null);
  }

  public static Response internalError(String message, Throwable throwable) {
    ServerHealth.getInstance().recordFailure(throwable);
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(ErrorResponse.internalError(message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  public static Response notFound(String type, String message) {
    return notFound(type, message, null);
  }

  public static Response notFound(String message, Throwable throwable) {
    return notFound(throwable.getClass().getSimpleName(), message, throwable);
  }

  public static Response notFound(String type, String message, Throwable throwable) {
    return Response.status(Response.Status.NOT_FOUND)
        .entity(ErrorResponse.notFound(type, message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  public static Response alreadyExists(String type, String message) {
    return alreadyExists(type, message, null);
  }

  public static Response alreadyExists(String message, Throwable throwable) {
    return alreadyExists(throwable.getClass().getSimpleName(), message, throwable);
  }

  public static Response alreadyExists(String type, String message, Throwable throwable) {
    return Response.status(Response.Status.CONFLICT)
        .entity(ErrorResponse.alreadyExists(type, message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  public static Response notInUse(String message, Throwable throwable) {
    return notInUse(throwable.getClass().getSimpleName(), message, throwable);
  }

  public static Response notInUse(String type, String message, Throwable throwable) {
    return Response.status(Response.Status.CONFLICT)
        .entity(ErrorResponse.notInUse(type, message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  public static Response inUse(String message, Throwable throwable) {
    return inUse(throwable.getClass().getSimpleName(), message, throwable);
  }

  public static Response inUse(String type, String message, Throwable throwable) {
    return Response.status(Response.Status.CONFLICT)
        .entity(ErrorResponse.inUse(type, message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  public static Response nonEmpty(String type, String message) {
    return nonEmpty(type, message, null);
  }

  public static Response nonEmpty(String message, Throwable throwable) {
    return nonEmpty(throwable.getClass().getSimpleName(), message, throwable);
  }

  public static Response nonEmpty(String type, String message, Throwable throwable) {
    return Response.status(Response.Status.CONFLICT)
        .entity(ErrorResponse.nonEmpty(type, message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  /**
   * Returns an HTTP 501 response for functionality that the server does not implement.
   *
   * @param message the error message
   * @return the HTTP response
   */
  public static Response unsupportedOperation(String message) {
    return unsupportedOperation(message, null);
  }

  /**
   * Returns an HTTP 501 response for functionality that the server does not implement.
   *
   * @param message the error message
   * @param throwable the exception that caused the error
   * @return the HTTP response
   */
  public static Response unsupportedOperation(String message, Throwable throwable) {
    return Response.status(Response.Status.NOT_IMPLEMENTED)
        .entity(ErrorResponse.unsupportedOperation(message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  /**
   * Returns an HTTP 409 response when an operation conflicts with the target object's state.
   *
   * <p>The unsupported-operation error payload is retained so existing clients can reconstruct
   * domain exceptions such as {@code UnmodifiableStatisticException}.
   *
   * @param message the error message
   * @param throwable the exception that caused the error
   * @return the HTTP response
   */
  public static Response operationConflict(String message, Throwable throwable) {
    return Response.status(Response.Status.CONFLICT)
        .entity(ErrorResponse.unsupportedOperation(message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  /**
   * Returns an HTTP 405 response when the target resource does not allow the request method.
   *
   * <p>The unsupported-operation error payload is retained for compatibility with clients that
   * identify this response by its application error code.
   *
   * @param message the error message
   * @return the HTTP response
   */
  public static Response methodNotAllowed(String message) {
    return Response.status(Response.Status.METHOD_NOT_ALLOWED)
        .entity(ErrorResponse.unsupportedOperation(message))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  public static Response forbidden(String message, Throwable throwable) {
    return Response.status(Response.Status.FORBIDDEN)
        .entity(ErrorResponse.forbidden(message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  public static <T> Response serviceUnavailable(T t) {
    return Response.status(Response.Status.SERVICE_UNAVAILABLE)
        .entity(t)
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  /**
   * Returns the health response used after an observed out-of-memory failure.
   *
   * @return HTTP 503 with a JVM failure requiring process restart
   */
  public static Response outOfMemoryResponse() {
    HealthCheckDTO check =
        new HealthCheckDTO(
            "jvm",
            HealthCheckDTO.Status.DOWN,
            Collections.singletonMap("reason", "OutOfMemoryError; restart required"));
    return serviceUnavailable(
        new HealthResponse(HealthCheckDTO.Status.DOWN, Collections.singletonList(check)));
  }

  public static Response doAs(
      HttpServletRequest httpRequest, PrivilegedExceptionAction<Response> action) throws Exception {
    UserPrincipal principal =
        (UserPrincipal)
            httpRequest.getAttribute(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME);
    if (principal == null) {
      principal = new UserPrincipal(AuthConstants.ANONYMOUS_USER);
    }
    try {
      return PrincipalUtils.doAs(principal, action);
    } catch (Exception | Error failure) {
      // Record before a resource converts a wrapped failure into an ordinary error response.
      ServerHealth.getInstance().recordFailure(failure);
      throw failure;
    }
  }

  public static Map<String, String> filterFilesetAuditHeaders(HttpServletRequest httpRequest) {
    Map<String, String> filteredHeaders = Maps.newHashMap();

    String internalClientType =
        httpRequest.getHeader(FilesetAuditConstants.HTTP_HEADER_INTERNAL_CLIENT_TYPE);
    if (StringUtils.isNotBlank(internalClientType)) {
      filteredHeaders.put(
          FilesetAuditConstants.HTTP_HEADER_INTERNAL_CLIENT_TYPE,
          InternalClientType.checkValid(internalClientType)
              ? internalClientType
              : InternalClientType.UNKNOWN.name());
    }

    String dataOperation =
        httpRequest.getHeader(FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION);
    if (StringUtils.isNotBlank(dataOperation)) {
      filteredHeaders.put(
          FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION,
          FilesetDataOperation.checkValid(dataOperation)
              ? dataOperation
              : FilesetDataOperation.UNKNOWN.name());
    }
    return filteredHeaders;
  }

  public static Map<String, String> filterFilesetCredentialHeaders(HttpServletRequest httpRequest) {
    Map<String, String> filteredHeaders = Maps.newHashMap();

    String currentLocationName =
        httpRequest.getHeader(CredentialConstants.HTTP_HEADER_CURRENT_LOCATION_NAME);
    if (StringUtils.isNotBlank(currentLocationName)) {
      filteredHeaders.put(
          CredentialConstants.HTTP_HEADER_CURRENT_LOCATION_NAME, currentLocationName);
    }
    return filteredHeaders;
  }

  public static int[] getClientVersion(HttpServletRequest request) {
    String clientVersion = request.getHeader(Version.CLIENT_VERSION_HEADER);
    if (StringUtils.isBlank(clientVersion)) {
      // If the client does not send the version, we assume it is the current version.
      clientVersion = Version.getCurrentVersion().version;
    }

    return StringUtils.isBlank(clientVersion) ? null : Version.parseVersionNumber(clientVersion);
  }

  public static Map<String, Object> extractPathParamsFromParameters(
      Parameter[] parameters, Object[] args) {
    Map<String, Object> pathParams = new HashMap<>();
    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];
      PathParam pathParam = parameter.getAnnotation(PathParam.class);
      if (pathParam == null) {
        continue;
      }
      pathParams.put("p_" + pathParam.value(), args[i]);
    }
    return pathParams;
  }

  /**
   * Resolves the Iceberg REST service URI from the supplied service registration and configuration.
   *
   * <p>A valid advertised URI takes precedence over the listener configuration. Otherwise, the URI
   * uses the configured host, protocol and port, with Iceberg REST defaults for omitted values. A
   * wildcard listener host is replaced with {@code requestHost}, assuming both services share a
   * host. Reverse proxies or split deployments should configure {@code advertised-uri} explicitly.
   *
   * @param serviceManager the auxiliary service manager used to check service registration
   * @param serviceConfig the effective Iceberg REST service configuration returned by {@link
   *     AuxiliaryServiceManager#getAuxServiceConfig}, with the service prefix stripped
   * @param metalake the requested metalake, or null/blank to skip the metalake check
   * @param requestHost the hostname from the current request, used for wildcard listener hosts
   * @return the service URI, or null if the service is not registered, does not use the dynamic
   *     catalog config provider, or serves a different metalake
   * @throws IllegalStateException if the advertised URI is invalid
   */
  @Nullable
  public static String resolveIcebergRestServiceUri(
      AuxiliaryServiceManager serviceManager,
      Map<String, String> serviceConfig,
      @Nullable String metalake,
      String requestHost) {
    if (!serviceManager.isAuxServiceRegistered(ICEBERG_REST_SERVICE_NAME)) {
      return null;
    }

    String provider = serviceConfig.getOrDefault(ICEBERG_CATALOG_CONFIG_PROVIDER_KEY, "");
    if (!ICEBERG_DYNAMIC_CONFIG_PROVIDER_NAME.equals(provider)) {
      // Only the dynamic catalog config provider maps Iceberg REST catalog names onto Gravitino
      // catalogs; the default static provider serves statically-declared catalogs unrelated to
      // Gravitino catalog names, so routing at it would 404 on every request.
      LOG.debug(
          "Iceberg REST service does not use the dynamic catalog config provider "
              + "(catalog-config-provider={}); not reporting its endpoint for auto-discovery.",
          provider);
      return null;
    }

    String servedMetalake = serviceConfig.getOrDefault(ICEBERG_SERVED_METALAKE_KEY, "");
    if (StringUtils.isNotBlank(metalake)
        && StringUtils.isNotBlank(servedMetalake)
        && !servedMetalake.equals(metalake)) {
      // The Iceberg REST server serves exactly one metalake. Routing a different metalake's
      // catalogs at it would 404 on every request, so report it as unavailable instead.
      LOG.debug(
          "Iceberg REST service serves metalake {}, not the requested metalake {}; not "
              + "reporting its endpoint for auto-discovery.",
          servedMetalake,
          metalake);
      return null;
    }

    String advertisedUri = StringUtils.trimToNull(serviceConfig.get(ICEBERG_ADVERTISED_URI_KEY));
    if (advertisedUri != null) {
      return checkIcebergRestAdvertisedUri(advertisedUri);
    }

    String host = serviceConfig.getOrDefault(ICEBERG_HOST_KEY, ICEBERG_DEFAULT_HOST);
    if (isWildcardHost(host)) {
      host = requestHost;
    }
    boolean enableHttps =
        Boolean.parseBoolean(serviceConfig.getOrDefault(ICEBERG_ENABLE_HTTPS_KEY, "false"));
    String scheme = enableHttps ? "https" : "http";
    int port =
        parseIcebergRestPort(
            serviceConfig,
            enableHttps ? ICEBERG_HTTPS_PORT_KEY : ICEBERG_HTTP_PORT_KEY,
            enableHttps ? ICEBERG_DEFAULT_HTTPS_PORT : ICEBERG_DEFAULT_HTTP_PORT);
    return String.format("%s://%s:%d/iceberg", scheme, bracketIfIPv6(host), port);
  }

  private static String checkIcebergRestAdvertisedUri(String value) {
    boolean valid;
    try {
      URI uri = new URI(value);
      // URI accepts any non-negative integer as a port; -1 means no explicit port.
      int port = uri.getPort();
      valid =
          StringUtils.equalsAnyIgnoreCase(uri.getScheme(), "http", "https")
              && StringUtils.isNotBlank(uri.getHost())
              && (port == -1 || (port >= 1 && port <= 65535))
              && uri.getQuery() == null
              && uri.getFragment() == null;
    } catch (URISyntaxException e) {
      valid = false;
    }
    if (!valid) {
      throw new IllegalStateException(
          String.format(
              "Invalid Iceberg REST service %s '%s': expected an absolute http(s) URI with a "
                  + "host, a port in 1-65535 if present, and no query or fragment",
              ICEBERG_ADVERTISED_URI_KEY, value));
    }
    return value;
  }

  // An IPv6 literal host (e.g. "::1", from an explicit config value or from
  // HttpServletRequest#getServerName()) must be bracketed to form a valid URI authority;
  // otherwise its colons are parsed as the port separator. A hostname or IPv4 address never
  // contains a colon, so this only ever fires for IPv6.
  private static String bracketIfIPv6(String host) {
    if (host.contains(":") && !host.startsWith("[")) {
      return "[" + host + "]";
    }
    return host;
  }

  private static int parseIcebergRestPort(Map<String, String> config, String key, int defaultPort) {
    String value = config.getOrDefault(key, "");
    if (StringUtils.isBlank(value)) {
      return defaultPort;
    }
    try {
      return Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      return defaultPort;
    }
  }

  private static boolean isWildcardHost(String host) {
    return StringUtils.isBlank(host)
        || "0.0.0.0".equals(host)
        || "::".equals(host)
        || "[::]".equals(host)
        || "0:0:0:0:0:0:0:0".equals(host);
  }
}
