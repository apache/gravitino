/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog.lakehouse.iceberg.web.rest;

import com.datastrato.graviton.catalog.lakehouse.iceberg.web.IcebergObjectMapperProvider;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergConfig extends JerseyTest {

  @Override
  protected Application configure() {
    return IcebergRestTestUtil.getIcebergResourceConfig(IcebergConfig.class);
  }

  @Test
  public void testConfig() {
    Response resp =
        target(IcebergRestTestUtil.CONFIG_PATH)
            .register(IcebergObjectMapperProvider.class)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept(MediaType.APPLICATION_JSON_TYPE)
            .get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    ConfigResponse response = resp.readEntity(ConfigResponse.class);
    Assertions.assertEquals(response.defaults().size(), 0);
    Assertions.assertEquals(response.overrides().size(), 0);
  }
}
