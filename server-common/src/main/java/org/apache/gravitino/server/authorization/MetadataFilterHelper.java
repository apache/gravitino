/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.authorization;

import java.util.Map;
import java.util.function.Function;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionExecutor;

/**
 * MetadataFilterHelper performs permission checks on the list data returned by the REST API based
 * on expressions or metadata types, and calls {@link GravitinoAuthorizer} for authorization,
 * returning only the metadata that the user has permission to access.
 */
public class MetadataFilterHelper {

  private MetadataFilterHelper() {}

  /**
   * Call {@link GravitinoAuthorizer} to filter the metadata list
   *
   * @param metadataType for example, CATALOG, SCHEMA,TABLE, etc.
   * @param privilege for example, CREATE_CATALOG, CREATE_TABLE, etc.
   * @param metadataList metadata list.
   * @param metadataIdExtractor extract the metadata id from list items.
   * @return metadata List that the user has permission to access.
   * @param <E> metadata object.
   */
  public static <E> E[] filter(
      MetadataObject.Type metadataType,
      String privilege,
      E[] metadataList,
      Function<E, Long> metadataIdExtractor) {
    return null;
  }

  /**
   * Call {@link AuthorizationExpressionExecutor} to filter the metadata list
   *
   * @param expression authorization expression
   * @param metadataList metaData list.
   * @param metadataContextExtractor extract the metadata context(key-metadata type, value-metadata
   *     id) from list items.
   * @return metadata List that the user has permission to access.
   * @param <E> metadata object.
   */
  public static <E> E[] filterByExpression(
      String expression,
      E[] metadataList,
      Function<E, Map<MetadataObject.Type, Long>> metadataContextExtractor) {
    return null;
  }
}
