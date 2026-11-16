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
package org.apache.gravitino.authorization.ranger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.gravitino.authorization.ranger.reference.VXGroup;
import org.apache.gravitino.authorization.ranger.reference.VXGroupList;
import org.apache.gravitino.authorization.ranger.reference.VXUser;
import org.apache.gravitino.authorization.ranger.reference.VXUserList;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Exercises {@link RangerClientExtension} over HTTP against a stub Ranger Admin. */
public class TestRangerClientExtension {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  // The stub server and the client use the same IPv4 address, also when IPv6 is preferred.
  private static final String LOOPBACK = "127.0.0.1";

  private final List<String> requests = new CopyOnWriteArrayList<>();
  private final Map<String, String> requestBodies = new ConcurrentHashMap<>();
  private HttpServer server;
  private RangerClientExtension client;

  @BeforeEach
  public void setUp() throws IOException {
    server = HttpServer.create(new InetSocketAddress(LOOPBACK, 0), 0);
    server.createContext(
        "/service/xusers/users/external",
        exchange -> respond(exchange, 200, userJson(7L, readBody(exchange))));
    server.createContext(
        "/service/xusers/users",
        exchange -> {
          if ("DELETE".equals(exchange.getRequestMethod())) {
            respond(exchange, 204, null);
          } else {
            respond(
                exchange,
                200,
                "{\"startIndex\":0,\"pageSize\":200,\"totalCount\":1,\"resultSize\":1,"
                    + "\"sortType\":\"asc\",\"sortBy\":\"id\",\"vXUsers\":["
                    + userJson(7L, "{\"name\":\"user1\"}")
                    + "]}");
          }
        });
    server.createContext(
        "/service/xusers/groups",
        exchange -> {
          String method = exchange.getRequestMethod();
          if ("DELETE".equals(method)) {
            respond(exchange, 204, null);
          } else if ("POST".equals(method)) {
            readBody(exchange);
            respond(exchange, 200, "{\"id\":3,\"name\":\"group1\",\"groupSource\":0,\"x\":1}");
          } else {
            respond(
                exchange,
                200,
                "{\"totalCount\":1,\"vXGroups\":[{\"id\":3,\"name\":\"group1\",\"x\":1}]}");
          }
        });
    server.createContext(
        "/service/public/v2/api/service/name/",
        exchange -> respond(exchange, 404, "{\"statusCode\":1,\"msgDesc\":\"not found\"}"));
    server.createContext(
        "/service/public/v2/api/policy",
        exchange -> {
          readBody(exchange);
          respond(exchange, 200, "{\"id\":11,\"name\":\"p1\",\"createTime\":1714557600000}");
        });
    server.start();

    String url = "http://" + LOOPBACK + ":" + server.getAddress().getPort();
    client = new RangerClientExtension(url, "simple", "admin", "password");
  }

  @AfterEach
  public void tearDown() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  public void testUserApis() throws Exception {
    Assertions.assertTrue(
        client.createUser(VXUser.builder().withName("user1").withDescription("d").build()));
    JsonNode body = MAPPER.readTree(requestBodies.get("POST /service/xusers/users/external"));
    Assertions.assertEquals("user1", body.get("name").asText());
    Assertions.assertEquals("d", body.get("description").asText());
    Assertions.assertTrue(body.has("password"));
    Assertions.assertFalse(body.has("id"));

    VXUserList users = client.searchUser(ImmutableMap.of("name", "user1"));
    Assertions.assertEquals(1, users.getListSize());
    Assertions.assertEquals(7L, users.getList().get(0).getId());
    Assertions.assertTrue(requests.contains("GET /service/xusers/users?name=user1"));

    Assertions.assertTrue(client.deleteUser(7L));
    Assertions.assertTrue(requests.contains("DELETE /service/xusers/users/7?forceDelete=true"));
  }

  @Test
  public void testGroupApis() {
    Assertions.assertTrue(client.createGroup(VXGroup.builder().withName("group1").build()));

    VXGroupList groups = client.searchGroup(ImmutableMap.of("name", "group1"));
    Assertions.assertEquals(1, groups.getListSize());
    Assertions.assertEquals(3L, groups.getList().get(0).getId());

    Assertions.assertTrue(client.deleteGroup(3L));
    Assertions.assertTrue(requests.contains("DELETE /service/xusers/groups/3?forceDelete=true"));
  }

  @Test
  public void testPolicyRequestsOmitServerManagedTimes() throws Exception {
    Date createTime = Date.from(Instant.ofEpochMilli(1714557600000L));
    Date updateTime = Date.from(Instant.ofEpochMilli(1714557601000L));
    RangerPolicy policy = new RangerPolicy();
    policy.setId(11L);
    policy.setName("p1");
    policy.setResource("database", new RangerPolicy.RangerPolicyResource("db1"));
    policy.setCreateTime(createTime);
    policy.setUpdateTime(updateTime);

    RangerPolicy updated = client.updatePolicy(11L, policy);
    RangerPolicy created = client.createPolicy(policy);

    for (String request :
        ImmutableList.of(
            "PUT /service/public/v2/api/policy/11", "POST /service/public/v2/api/policy")) {
      JsonNode body = MAPPER.readTree(requestBodies.get(request));
      Assertions.assertEquals("p1", body.get("name").asText(), request);
      Assertions.assertTrue(body.has("resources"), request);
      Assertions.assertFalse(body.has("createTime"), request);
      Assertions.assertFalse(body.has("updateTime"), request);
    }
    // The caller's policy keeps its values, and responses are parsed as before.
    Assertions.assertEquals(createTime, policy.getCreateTime());
    Assertions.assertEquals(updateTime, policy.getUpdateTime());
    Assertions.assertEquals(createTime, updated.getCreateTime());
    Assertions.assertEquals(11L, created.getId());
  }

  /** Non-JSON responses are contextualized without changing service-not-found errors. */
  @Test
  public void testNonJsonServiceResponseIsActionable() throws Exception {
    for (int status : new int[] {200, 404}) {
      server.removeContext("/service/public/v2/api/service/name/");
      server.createContext(
          "/service/public/v2/api/service/name/",
          exchange -> {
            byte[] body = "upstream unavailable".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            exchange.sendResponseHeaders(status, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
              out.write(body);
            }
          });
      if (status == 200) {
        AuthorizationPluginException error =
            Assertions.assertThrows(
                AuthorizationPluginException.class, () -> client.getService("hive-service"));
        Assertions.assertTrue(error.getMessage().contains("hive-service"));
        Assertions.assertTrue(error.getMessage().contains("must return JSON"));
        Assertions.assertInstanceOf(ClientHandlerException.class, error.getCause());
      } else {
        RangerServiceException error =
            Assertions.assertThrows(
                RangerServiceException.class, () -> client.getService("hive-service"));
        Assertions.assertEquals(status, error.getStatus().getStatusCode());
        Assertions.assertTrue(error.getMessage().contains("upstream unavailable"));
      }
    }
  }

  @Test
  public void testServiceNotFoundStatus() {
    RangerServiceException e =
        Assertions.assertThrows(RangerServiceException.class, () -> client.getService("absent"));
    Assertions.assertEquals(ClientResponse.Status.NOT_FOUND, e.getStatus());
  }

  private static String userJson(long id, String requestBody) throws IOException {
    JsonNode request = MAPPER.readTree(requestBody);
    return "{\"id\":"
        + id
        + ",\"createDate\":\"2024-05-01T10:00:00Z\",\"name\":\""
        + request.get("name").asText()
        + "\",\"status\":1,\"isVisible\":1,\"userSource\":1,\"groupIdList\":[],"
        + "\"userRoleList\":[\"ROLE_USER\"],\"fieldAddedByNewerRangerAdmin\":true}";
  }

  private String readBody(HttpExchange exchange) throws IOException {
    try (InputStream in = exchange.getRequestBody()) {
      String body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      requestBodies.put(exchange.getRequestMethod() + " " + exchange.getRequestURI(), body);
      return body;
    }
  }

  private void respond(HttpExchange exchange, int status, String body) throws IOException {
    requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI());
    if (body == null) {
      exchange.sendResponseHeaders(status, -1);
      exchange.close();
      return;
    }
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }
}
