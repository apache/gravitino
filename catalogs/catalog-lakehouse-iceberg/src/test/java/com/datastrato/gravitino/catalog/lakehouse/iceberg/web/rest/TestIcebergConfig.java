/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg.web.rest;

import java.util.Optional;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestIcebergConfig extends IcebergTestBase {

  @Override
  protected Application configure() {
    return IcebergRestTestUtil.getIcebergResourceConfig(IcebergConfigOperations.class, false);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testConfig(boolean withPrefix) {
    setUrlPathWithPrefix(withPrefix);
    Response resp = getConfigClientBuilder().get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    ConfigResponse response = resp.readEntity(ConfigResponse.class);
    Assertions.assertEquals(0, response.defaults().size());
    Assertions.assertEquals(0, response.overrides().size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"PREFIX", "", "\\\n\t\\\'", "\u0024", "\100", "[_~"})
  void testIcebergRestValidPrefix(String prefix) {
    String path = injectPrefixToPath(IcebergRestTestUtil.CONFIG_PATH, prefix);
    Response response = getIcebergClientBuilder(path, Optional.empty()).get();
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }

  @ParameterizedTest
  @ValueSource(strings = {"/", "hello/"})
  void testIcebergRestInvalidPrefix(String prefix) {
    String path = injectPrefixToPath(IcebergRestTestUtil.CONFIG_PATH, prefix);
    Response response = getIcebergClientBuilder(path, Optional.empty()).get();
    Assertions.assertEquals(500, response.getStatus());
  }
}
