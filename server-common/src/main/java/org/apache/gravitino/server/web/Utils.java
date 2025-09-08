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
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
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
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.utils.PrincipalUtils;

public class Utils {

  private static final String REMOTE_USER = "gravitino";

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

  public static Response unsupportedOperation(String message) {
    return unsupportedOperation(message, null);
  }

  public static Response unsupportedOperation(String message, Throwable throwable) {
    return Response.status(Response.Status.METHOD_NOT_ALLOWED)
        .entity(ErrorResponse.unsupportedOperation(message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  public static Response forbidden(String message, Throwable throwable) {
    return Response.status(Response.Status.FORBIDDEN)
        .entity(ErrorResponse.forbidden(message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  public static Response doAs(
      HttpServletRequest httpRequest, PrivilegedExceptionAction<Response> action) throws Exception {
    UserPrincipal principal =
        (UserPrincipal)
            httpRequest.getAttribute(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME);
    if (principal == null) {
      principal = new UserPrincipal(AuthConstants.ANONYMOUS_USER);
    }
    return PrincipalUtils.doAs(principal, action);
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
}
