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
package org.apache.gravitino.server.web.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import org.apache.gravitino.auxiliary.AuxiliaryServiceManager;
import org.apache.gravitino.dto.responses.IcebergRESTServiceResponse;
import org.junit.jupiter.api.Test;

public class TestIcebergRESTServiceOperations {

  private static final String DYNAMIC_PROVIDER = "dynamic-config-provider";

  @Test
  public void testDiscoveryEndpointProducesVersionedJson() throws Exception {
    Produces produces =
        IcebergRESTServiceOperations.class
            .getMethod("getIcebergRestServiceUri", String.class)
            .getAnnotation(Produces.class);

    assertEquals("application/vnd.gravitino.v1+json", produces.value()[0]);
  }

  private static Map<String, String> withDynamicProvider(Map<String, String> extra) {
    return ImmutableMap.<String, String>builder()
        .put("catalog-config-provider", DYNAMIC_PROVIDER)
        .putAll(extra)
        .buildKeepingLast();
  }

  private IcebergRESTServiceOperations newOps(
      boolean registered, Map<String, String> icebergConfig, String requestServerName) {
    AuxiliaryServiceManager auxServiceManager = mock(AuxiliaryServiceManager.class);
    when(auxServiceManager.isAuxServiceRegistered("iceberg-rest")).thenReturn(registered);
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getServerName()).thenReturn(requestServerName);

    return new IcebergRESTServiceOperations() {
      @Override
      AuxiliaryServiceManager getAuxServiceManager() {
        return auxServiceManager;
      }

      @Override
      Map<String, String> getIcebergRestServiceConfig() {
        return icebergConfig;
      }

      @Override
      HttpServletRequest getHttpRequest() {
        return request;
      }
    };
  }

  private String uriOf(Response response) {
    return ((IcebergRESTServiceResponse) response.getEntity()).getUri();
  }

  @Test
  public void testReturnsNullWhenAuxServiceNotRegistered() {
    IcebergRESTServiceOperations ops = newOps(false, ImmutableMap.of(), "gravitino-host");
    assertNull(uriOf(ops.getIcebergRestServiceUri("test")));
  }

  @Test
  public void testReturnsNullWhenNotUsingDynamicConfigProvider() {
    // Regression test: the default (static) catalog config provider serves statically-declared
    // catalogs unrelated to Gravitino catalog names, so it must never be reported for
    // auto-discovery — even with a blank/absent catalog-config-provider, which is what the
    // common no-provider-configured deployment looks like.
    IcebergRESTServiceOperations ops =
        newOps(true, ImmutableMap.of("host", "irc-host"), "gravitino-host");
    assertNull(uriOf(ops.getIcebergRestServiceUri("test")));
  }

  @Test
  public void testReturnsNullWhenStaticConfigProviderExplicit() {
    IcebergRESTServiceOperations ops =
        newOps(
            true,
            ImmutableMap.of("catalog-config-provider", "static-config-provider", "host", "h"),
            "gravitino-host");
    assertNull(uriOf(ops.getIcebergRestServiceUri("test")));
  }

  @Test
  public void testDefaultPortIsTheIcebergRestDefaultNotTheGravitinoServerDefault() {
    // Regression test: without an explicit httpPort, the reported port must be the Iceberg REST
    // server's own default (9001), not the Gravitino webserver's default (8090) that
    // JettyServerConfig would otherwise fall back to.
    IcebergRESTServiceOperations ops =
        newOps(true, withDynamicProvider(ImmutableMap.of("host", "irc-host")), "gravitino-host");
    assertEquals("http://irc-host:9001/iceberg", uriOf(ops.getIcebergRestServiceUri("")));
  }

  @Test
  public void testHttpsUsesTheHttpsPortNotTheHttpPort() {
    IcebergRESTServiceOperations ops =
        newOps(
            true,
            withDynamicProvider(
                ImmutableMap.of(
                    "host", "irc-host",
                    "enableHttps", "true")),
            "gravitino-host");
    assertEquals("https://irc-host:9433/iceberg", uriOf(ops.getIcebergRestServiceUri("")));
  }

  @Test
  public void testExplicitPortIsHonored() {
    IcebergRESTServiceOperations ops =
        newOps(
            true,
            withDynamicProvider(
                ImmutableMap.of(
                    "host", "irc-host",
                    "httpPort", "19001")),
            "gravitino-host");
    assertEquals("http://irc-host:19001/iceberg", uriOf(ops.getIcebergRestServiceUri("")));
  }

  @Test
  public void testMalformedPortFallsBackToDefaultPort() {
    IcebergRESTServiceOperations ops =
        newOps(
            true,
            withDynamicProvider(ImmutableMap.of("host", "irc-host", "httpPort", "not-a-number")),
            "gravitino-host");
    assertEquals("http://irc-host:9001/iceberg", uriOf(ops.getIcebergRestServiceUri("")));
  }

  @Test
  public void testWildcardHostFallsBackToRequestServerName() {
    IcebergRESTServiceOperations ops =
        newOps(
            true, withDynamicProvider(ImmutableMap.of("host", "0.0.0.0")), "host.docker.internal");
    assertEquals(
        "http://host.docker.internal:9001/iceberg", uriOf(ops.getIcebergRestServiceUri("")));
  }

  @Test
  public void testBlankHostIsTreatedAsWildcard() {
    IcebergRESTServiceOperations ops =
        newOps(true, withDynamicProvider(ImmutableMap.of()), "gravitino-host");
    assertEquals("http://gravitino-host:9001/iceberg", uriOf(ops.getIcebergRestServiceUri("")));
  }

  @Test
  public void testMismatchedMetalakeReturnsNull() {
    IcebergRESTServiceOperations ops =
        newOps(
            true,
            withDynamicProvider(
                ImmutableMap.of(
                    "host", "irc-host",
                    "gravitino-metalake", "prod")),
            "gravitino-host");
    assertNull(uriOf(ops.getIcebergRestServiceUri("test")));
  }

  @Test
  public void testMatchingMetalakeIsReported() {
    IcebergRESTServiceOperations ops =
        newOps(
            true,
            withDynamicProvider(
                ImmutableMap.of(
                    "host", "irc-host",
                    "gravitino-metalake", "test")),
            "gravitino-host");
    assertEquals("http://irc-host:9001/iceberg", uriOf(ops.getIcebergRestServiceUri("test")));
  }

  @Test
  public void testBlankRequestedMetalakeSkipsTheMetalakeCheck() {
    IcebergRESTServiceOperations ops =
        newOps(
            true,
            withDynamicProvider(
                ImmutableMap.of(
                    "host", "irc-host",
                    "gravitino-metalake", "prod")),
            "gravitino-host");
    assertEquals("http://irc-host:9001/iceberg", uriOf(ops.getIcebergRestServiceUri("")));
  }

  @Test
  public void testResponseIsNotCacheable() {
    IcebergRESTServiceOperations ops =
        newOps(true, withDynamicProvider(ImmutableMap.of()), "gravitino-host");
    Response response = ops.getIcebergRestServiceUri("");
    assertEquals("no-store", response.getHeaderString("Cache-Control"));
  }
}
