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
package org.apache.gravitino.authorization.ranger.reference;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.TimeZone;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Pins the JSON exchanged with Ranger Admin for the user and group APIs. Since Ranger 2.8 the
 * client serializes requests with {@link JsonUtils} (Jackson 2) and parses responses with {@link
 * JsonUtilsV2} (Jackson 2), whereas Ranger 2.4 used Gson for requests and Jackson 1 for responses.
 */
public class TestRangerReferenceJson {

  // The Gson configuration of RangerRESTClient in Ranger 2.4, used to serialize request bodies.
  private static final Gson RANGER_24_REQUEST_GSON =
      new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").create();

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testUserRequestMatchesRanger24Format() throws Exception {
    VXUser user = VXUser.builder().withName("user1").withDescription("desc").build();

    JsonNode actual = MAPPER.readTree(JsonUtils.objectToJson(user));
    JsonNode expected = MAPPER.readTree(RANGER_24_REQUEST_GSON.toJson(user));

    Assertions.assertEquals(expected, actual);
    Assertions.assertEquals("user1", actual.get("name").asText());
    Assertions.assertEquals("rangerR0cks!", actual.get("password").asText());
    Assertions.assertEquals(RangerCommonEnums.STATUS_ENABLED, actual.get("status").asInt());
    Assertions.assertEquals(RangerCommonEnums.IS_VISIBLE, actual.get("isVisible").asInt());
    Assertions.assertFalse(actual.has("id"));
    Assertions.assertFalse(actual.has("createDate"));
  }

  @Test
  public void testGroupRequestMatchesRanger24Format() throws Exception {
    VXGroup group = VXGroup.builder().withName("group1").withDescription("desc").build();

    JsonNode actual = MAPPER.readTree(JsonUtils.objectToJson(group));
    JsonNode expected = MAPPER.readTree(RANGER_24_REQUEST_GSON.toJson(group));

    Assertions.assertEquals(expected, actual);
    Assertions.assertEquals("group1", actual.get("name").asText());
    Assertions.assertEquals(RangerCommonEnums.GROUP_INTERNAL, actual.get("groupSource").asInt());
    Assertions.assertFalse(actual.has("id"));
  }

  @Test
  public void testDateUsesRangerAdminFormat() throws Exception {
    VXUser user = VXUser.builder().withName("user1").build();
    Date epoch = Date.from(Instant.EPOCH);
    user.createDate = epoch;

    JsonNode actual = MAPPER.readTree(JsonUtils.objectToJson(user));

    // JsonDateSerializer, copied from Ranger Admin, formats in the JVM default time zone, so the
    // expected value does too. Pinning it to UTC would fail in other time zones.
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    Assertions.assertEquals(format.format(epoch), actual.get("createDate").asText());
  }

  @Test
  public void testParseUserResponses() throws Exception {
    String userJson =
        "{\"id\":7,\"createDate\":\"2024-05-01T10:00:00Z\",\"updateDate\":\"2024-05-01T10:00:00Z\","
            + "\"owner\":\"Admin\",\"updatedBy\":\"Admin\",\"name\":\"user1\",\"firstName\":\"user1\","
            + "\"password\":\"*****\",\"description\":\"desc\",\"groupIdList\":[1,2],"
            + "\"groupNameList\":[\"g1\",\"g2\"],\"status\":1,\"isVisible\":1,\"userSource\":1,"
            + "\"userRoleList\":[\"ROLE_USER\"],\"otherAttributes\":\"{}\",\"syncSource\":\"ldap\","
            + "\"fieldAddedByNewerRangerAdmin\":{\"nested\":true}}";
    String userListJson =
        "{\"startIndex\":0,\"pageSize\":200,\"totalCount\":1,\"resultSize\":1,\"sortType\":\"asc\","
            + "\"sortBy\":\"id\",\"queryTimeMS\":1714557600000,\"listSize\":1,\"vXUsers\":["
            + userJson
            + "]}";

    VXUser user = JsonUtilsV2.jsonToObj(userJson, VXUser.class);
    Assertions.assertEquals(7L, user.getId());
    Assertions.assertEquals("user1", user.getName());
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    format.setTimeZone(TimeZone.getTimeZone("UTC"));
    Assertions.assertEquals(format.parse("2024-05-01T10:00:00Z"), user.createDate);

    VXUserList userList = JsonUtilsV2.jsonToObj(userListJson, VXUserList.class);
    Assertions.assertEquals(1, userList.getListSize());
    Assertions.assertEquals(200, userList.getPageSize());
    Assertions.assertEquals(7L, userList.getList().get(0).getId());
    Assertions.assertEquals("user1", userList.getList().get(0).getName());
  }

  @Test
  public void testParseGroupResponses() throws Exception {
    String groupJson =
        "{\"id\":3,\"createDate\":\"2024-05-01T10:00:00Z\",\"name\":\"group1\","
            + "\"description\":\"desc\",\"groupType\":1,\"groupSource\":0,\"isVisible\":1,"
            + "\"fieldAddedByNewerRangerAdmin\":\"x\"}";
    String groupListJson =
        "{\"startIndex\":0,\"pageSize\":200,\"totalCount\":1,\"resultSize\":1,"
            + "\"vXGroups\":["
            + groupJson
            + "],\"fieldAddedByNewerRangerAdmin\":[]}";

    VXGroup group = JsonUtilsV2.jsonToObj(groupJson, VXGroup.class);
    Assertions.assertEquals(3L, group.getId());
    Assertions.assertTrue(group.toString().contains("name={group1}"));

    VXGroupList groupList = JsonUtilsV2.jsonToObj(groupListJson, VXGroupList.class);
    Assertions.assertEquals(1, groupList.getListSize());
    Assertions.assertEquals(3L, groupList.getList().get(0).getId());
  }

  @Test
  public void testParseEmptyListResponse() throws Exception {
    VXUserList userList =
        JsonUtilsV2.jsonToObj("{\"startIndex\":0,\"totalCount\":0}", VXUserList.class);
    Assertions.assertEquals(0, userList.getListSize());
  }
}
