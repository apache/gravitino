package com.datastrato.graviton.server.web;

import com.datastrato.graviton.dto.responses.BaseResponse;
import com.datastrato.graviton.dto.responses.ErrorType;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class Utils {

  private static final String REMOTE_USER = "graviton";

  private Utils() {}

  public static String remoteUser(HttpServletRequest httpRequest) {
    return Optional.ofNullable(httpRequest.getRemoteUser()).orElse(REMOTE_USER);
  }

  public static <T> Response ok(T t) {
    return Response.status(Response.Status.OK).entity(t).type(MediaType.APPLICATION_JSON).build();
  }

  public static Response ok() {
    return Response.status(Response.Status.NO_CONTENT).type(MediaType.APPLICATION_JSON).build();
  }

  public static Response illegalArguments(String message) {
    return Response.status(Response.Status.BAD_REQUEST)
        .entity(BaseResponse.error(ErrorType.INVALID_ARGUMENTS, message))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  public static Response internalError(String message) {
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(BaseResponse.error(ErrorType.INTERNAL_ERROR, message))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  public static Response notFound(String message) {
    return Response.status(Response.Status.NOT_FOUND)
        .entity(BaseResponse.error(ErrorType.NOT_FOUND, message))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }
}
