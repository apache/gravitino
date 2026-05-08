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
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.RepresentationDTO;
import org.apache.gravitino.dto.rel.SQLRepresentationDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.ViewChange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestViewUpdateRequest {

  @Test
  public void testRenameViewRequestSerDeAndChange() throws JsonProcessingException {
    ViewUpdateRequest request = new ViewUpdateRequest.RenameViewRequest("v2");

    String json = JsonUtils.objectMapper().writeValueAsString(request);
    ViewUpdateRequest deserialized =
        JsonUtils.objectMapper().readValue(json, ViewUpdateRequest.class);

    Assertions.assertInstanceOf(ViewUpdateRequest.RenameViewRequest.class, deserialized);
    Assertions.assertDoesNotThrow(deserialized::validate);
    ViewChange change = deserialized.viewChange();
    Assertions.assertInstanceOf(ViewChange.RenameView.class, change);
    Assertions.assertEquals("v2", ((ViewChange.RenameView) change).getNewName());
  }

  @Test
  public void testSetAndRemovePropertyRequestValidation() {
    ViewUpdateRequest setProperty = new ViewUpdateRequest.SetViewPropertyRequest("k", "v");
    ViewUpdateRequest removeProperty = new ViewUpdateRequest.RemoveViewPropertyRequest("k");

    Assertions.assertDoesNotThrow(setProperty::validate);
    Assertions.assertDoesNotThrow(removeProperty::validate);

    ViewChange setPropertyChange = setProperty.viewChange();
    Assertions.assertInstanceOf(ViewChange.SetProperty.class, setPropertyChange);
    Assertions.assertEquals("k", ((ViewChange.SetProperty) setPropertyChange).getProperty());
    Assertions.assertEquals("v", ((ViewChange.SetProperty) setPropertyChange).getValue());

    ViewChange removePropertyChange = removeProperty.viewChange();
    Assertions.assertInstanceOf(ViewChange.RemoveProperty.class, removePropertyChange);
    Assertions.assertEquals("k", ((ViewChange.RemoveProperty) removePropertyChange).getProperty());
  }

  @Test
  public void testReplaceViewRequestValidateAndChange() {
    ViewUpdateRequest.ReplaceViewRequest request =
        new ViewUpdateRequest.ReplaceViewRequest(
            new ColumnDTO[0],
            new RepresentationDTO[] {
              SQLRepresentationDTO.builder().withDialect("spark").withSql("SELECT 2").build()
            },
            "cat1",
            "sch1",
            "new comment");

    Assertions.assertDoesNotThrow(request::validate);
    ViewChange change = request.viewChange();
    Assertions.assertInstanceOf(ViewChange.ReplaceView.class, change);
    ViewChange.ReplaceView replaceView = (ViewChange.ReplaceView) change;
    Assertions.assertEquals(1, replaceView.getRepresentations().length);
    Assertions.assertEquals("cat1", replaceView.getDefaultCatalog());
    Assertions.assertEquals("sch1", replaceView.getDefaultSchema());
    Assertions.assertEquals("new comment", replaceView.getComment());
  }

  @Test
  public void testReplaceViewRequestValidateDuplicateDialect() {
    ViewUpdateRequest.ReplaceViewRequest request =
        new ViewUpdateRequest.ReplaceViewRequest(
            new ColumnDTO[0],
            new RepresentationDTO[] {
              SQLRepresentationDTO.builder().withDialect("trino").withSql("SELECT 1").build(),
              SQLRepresentationDTO.builder().withDialect("trino").withSql("SELECT 2").build()
            },
            null,
            null,
            null);

    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, request::validate);
    Assertions.assertEquals("Duplicate SQL representation dialect: trino", exception.getMessage());
  }

  @Test
  public void testReplaceViewRequestValidateNullColumn() {
    ViewUpdateRequest.ReplaceViewRequest request =
        new ViewUpdateRequest.ReplaceViewRequest(
            new ColumnDTO[] {null},
            new RepresentationDTO[] {
              SQLRepresentationDTO.builder().withDialect("spark").withSql("SELECT 2").build()
            },
            null,
            null,
            null);

    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, request::validate);
    Assertions.assertEquals("column must not be null", exception.getMessage());
  }

  @Test
  public void testReplaceViewRequestValidateInvalidColumn() throws JsonProcessingException {
    String rawJson =
        "{"
            + "\"@type\":\"replaceView\","
            + "\"columns\":[{}],"
            + "\"representations\":[{\"type\":\"sql\",\"dialect\":\"spark\",\"sql\":\"SELECT 2\"}]"
            + "}";

    ViewUpdateRequest.ReplaceViewRequest request =
        (ViewUpdateRequest.ReplaceViewRequest)
            JsonUtils.objectMapper().readValue(rawJson, ViewUpdateRequest.class);

    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, request::validate);
    Assertions.assertEquals("Column name cannot be null or empty.", exception.getMessage());
  }

  @Test
  public void testViewUpdatesRequestSerDeAndValidate() throws JsonProcessingException {
    ViewUpdatesRequest request =
        new ViewUpdatesRequest(
            ImmutableList.of(
                new ViewUpdateRequest.RenameViewRequest("v2"),
                new ViewUpdateRequest.SetViewPropertyRequest("k", "v"),
                new ViewUpdateRequest.ReplaceViewRequest(
                    new ColumnDTO[0],
                    new RepresentationDTO[] {
                      SQLRepresentationDTO.builder()
                          .withDialect("spark")
                          .withSql("SELECT 2")
                          .build()
                    },
                    null,
                    null,
                    null)));

    String json = JsonUtils.objectMapper().writeValueAsString(request);
    ViewUpdatesRequest deserialized =
        JsonUtils.objectMapper().readValue(json, ViewUpdatesRequest.class);

    Assertions.assertEquals(3, deserialized.getUpdates().size());
    Assertions.assertDoesNotThrow(deserialized::validate);
    Assertions.assertInstanceOf(
        ViewUpdateRequest.RenameViewRequest.class, deserialized.getUpdates().get(0));
    Assertions.assertInstanceOf(
        ViewUpdateRequest.SetViewPropertyRequest.class, deserialized.getUpdates().get(1));
    Assertions.assertInstanceOf(
        ViewUpdateRequest.ReplaceViewRequest.class, deserialized.getUpdates().get(2));
  }

  @Test
  public void testViewUpdatesRequestValidateEmptyUpdates() {
    ViewUpdatesRequest empty = new ViewUpdatesRequest(ImmutableList.of());
    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, empty::validate);
    Assertions.assertEquals("updates must not be empty", exception.getMessage());
  }

  @Test
  public void testViewUpdatesRequestDeserializeFromRawJson() throws JsonProcessingException {
    String rawJson =
        "{"
            + "\"updates\":["
            + "{\"@type\":\"rename\",\"newName\":\"v2\"},"
            + "{\"@type\":\"setProperty\",\"property\":\"k\",\"value\":\"v\"},"
            + "{\"@type\":\"replaceView\","
            + "\"columns\":[],"
            + "\"representations\":[{\"type\":\"sql\",\"dialect\":\"trino\",\"sql\":\"SELECT 1\"}],"
            + "\"defaultCatalog\":\"cat\","
            + "\"defaultSchema\":\"sch\","
            + "\"comment\":\"new comment\""
            + "}"
            + "]"
            + "}";

    ViewUpdatesRequest request =
        JsonUtils.objectMapper().readValue(rawJson, ViewUpdatesRequest.class);

    Assertions.assertEquals(3, request.getUpdates().size());
    Assertions.assertInstanceOf(
        ViewUpdateRequest.RenameViewRequest.class, request.getUpdates().get(0));
    Assertions.assertInstanceOf(
        ViewUpdateRequest.SetViewPropertyRequest.class, request.getUpdates().get(1));
    Assertions.assertInstanceOf(
        ViewUpdateRequest.ReplaceViewRequest.class, request.getUpdates().get(2));
    Assertions.assertDoesNotThrow(request::validate);
  }
}
