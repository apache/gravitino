/*
* Copyright 2023 Datastrato.
* This software is licensed under the Apache License version 2.
*/

package com.datastrato.graviton;

import com.datastrato.graviton.client.HTTPClient;
import com.datastrato.graviton.client.RESTClient;
import com.datastrato.graviton.dto.requests.MetalakeCreateRequest;
import com.datastrato.graviton.dto.requests.MetalakeUpdateRequest;
import com.datastrato.graviton.dto.requests.MetalakeUpdatesRequest;
import com.datastrato.graviton.dto.responses.DropResponse;
import com.datastrato.graviton.dto.responses.MetalakeListResponse;
import com.datastrato.graviton.dto.responses.MetalakeResponse;
import com.datastrato.graviton.exceptions.MetalakeAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import com.datastrato.graviton.json.JsonUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitonClient implements SupportMetalakes, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(GravitonClient.class);

  private static final ObjectMapper MAPPER = JsonUtils.objectMapper();

  private final RESTClient restClient;

  private GravitonClient(String uri) {
    this.restClient =
        HTTPClient.builder(Collections.emptyMap()).uri(uri).withObjectMapper(MAPPER).build();
  }

  @Override
  public GravitonMetaLake[] listMetalakes() {
    MetalakeListResponse resp =
        restClient.get(
            "api/metalakes",
            MetalakeListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.metalakeErrorHandler());
    resp.validate();

    return Arrays.stream(resp.getMetalakes())
        .map(o -> DTOConverters.toMetaLake(o, restClient))
        .toArray(GravitonMetaLake[]::new);
  }

  @Override
  public GravitonMetaLake loadMetalake(NameIdentifier ident) throws NoSuchMetalakeException {
    Preconditions.checkArgument(ident != null, "Metalake name identifier cannot be null");
    Preconditions.checkArgument(
        !ident.hasNamespace(),
        "Metalake name identifier is a top-level identifier, namespace should be empty");

    MetalakeResponse resp =
        restClient.get(
            "api/metalakes/" + ident.toString(),
            MetalakeResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.metalakeErrorHandler());
    resp.validate();

    return DTOConverters.toMetaLake(resp.getMetalake(), restClient);
  }

  @Override
  public GravitonMetaLake createMetalake(
      NameIdentifier ident, String comment, Map<String, String> properties)
      throws MetalakeAlreadyExistsException {
    Preconditions.checkArgument(ident != null, "Metalake name identifier cannot be null");
    Preconditions.checkArgument(
        !ident.hasNamespace(),
        "Metalake name identifier is a top-level identifier, namespace should be empty");

    MetalakeCreateRequest req = new MetalakeCreateRequest(ident.toString(), comment, properties);
    req.validate();

    MetalakeResponse resp =
        restClient.post(
            "api/metalakes",
            req,
            MetalakeResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.metalakeErrorHandler());
    resp.validate();

    return DTOConverters.toMetaLake(resp.getMetalake(), restClient);
  }

  @Override
  public GravitonMetaLake alterMetalake(NameIdentifier ident, MetalakeChange... changes)
      throws NoSuchMetalakeException, IllegalArgumentException {
    Preconditions.checkArgument(ident != null, "Metalake name identifier cannot be null");
    Preconditions.checkArgument(
        !ident.hasNamespace(),
        "Metalake name identifier is a top-level identifier, namespace should be empty");

    List<MetalakeUpdateRequest> reqs =
        Arrays.stream(changes)
            .map(DTOConverters::toMetalakeUpdateRequest)
            .collect(Collectors.toList());
    MetalakeUpdatesRequest updatesRequest = new MetalakeUpdatesRequest(reqs);
    updatesRequest.validate();

    MetalakeResponse resp =
        restClient.put(
            "api/metalakes/" + ident.toString(),
            updatesRequest,
            MetalakeResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.metalakeErrorHandler());
    resp.validate();

    return DTOConverters.toMetaLake(resp.getMetalake(), restClient);
  }

  @Override
  public boolean dropMetalake(NameIdentifier ident) {
    Preconditions.checkArgument(ident != null, "Metalake name identifier cannot be null");
    Preconditions.checkArgument(
        !ident.hasNamespace(),
        "Metalake name identifier is a top-level identifier, namespace should be empty");

    try {
      DropResponse resp =
          restClient.delete(
              "api/metalakes/" + ident.toString(),
              DropResponse.class,
              Collections.emptyMap(),
              ErrorHandlers.metalakeErrorHandler());
      resp.validate();
      return resp.dropped();

    } catch (Exception e) {
      LOG.warn("Failed to drop metadata {}", ident, e);
      return false;
    }
  }

  @Override
  public void close() {
    if (restClient != null) {
      try {
        restClient.close();
      } catch (Exception e) {
        // Swallow the exception
        LOG.warn("Failed to close the HTTP REST client", e);
      }
    }
  }

  public static Builder builder(String uri) {
    return new Builder(uri);
  }

  public static class Builder {

    private String uri;

    private Builder(String uri) {
      this.uri = uri;
    }

    public GravitonClient build() {
      Preconditions.checkArgument(
          uri != null && !uri.isEmpty(), "The argument 'uri' must be a valid URI");

      return new GravitonClient(uri);
    }
  }
}
