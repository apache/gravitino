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
package org.apache.gravitino.server.authorization.expression;

public class AuthorizationExpressionConstants {
  public static final String LOAD_CATALOG_AUTHORIZATION_EXPRESSION =
      "ANY_USE_CATALOG || ANY(OWNER, METALAKE, CATALOG)";

  public static final String LOAD_SCHEMA_AUTHORIZATION_EXPRESSION =
      """
          ANY(OWNER, METALAKE, CATALOG) ||
          ANY_USE_CATALOG && (SCHEMA::OWNER || ANY_USE_SCHEMA)
           """;

  public static final String LOAD_MODEL_AUTHORIZATION_EXPRESSION =
      """
            ANY(OWNER, METALAKE, CATALOG) ||
             SCHEMA_OWNER_WITH_USE_CATALOG ||
              ANY_USE_CATALOG && ANY_USE_SCHEMA && (MODEL::OWNER || ANY_USE_MODEL)
                  """;

  public static final String LOAD_TABLE_AUTHORIZATION_EXPRESSION =
      """
                  ANY(OWNER, METALAKE, CATALOG) ||
                  SCHEMA_OWNER_WITH_USE_CATALOG ||
                  ANY_USE_CATALOG && ANY_USE_SCHEMA  && (TABLE::OWNER || ANY_SELECT_TABLE || ANY_MODIFY_TABLE)
                  """;

  //  Adding ANY_CREATE_TABLE here as Spark calls tableExists before creating a table.
  public static final String ICEBERG_LOAD_TABLE_AUTHORIZATION_EXPRESSION =
      """
                  ANY(OWNER, METALAKE, CATALOG) ||
                  SCHEMA_OWNER_WITH_USE_CATALOG ||
                  ANY_USE_CATALOG && ANY_USE_SCHEMA  && (TABLE::OWNER || ANY_SELECT_TABLE || ANY_MODIFY_TABLE || ANY_CREATE_TABLE)
                  """;

  public static final String MODIFY_TABLE_AUTHORIZATION_EXPRESSION =
      """
                  ANY(OWNER, METALAKE, CATALOG) ||
                  SCHEMA_OWNER_WITH_USE_CATALOG ||
                  ANY_USE_CATALOG && ANY_USE_SCHEMA && (TABLE::OWNER || ANY_MODIFY_TABLE)
                  """;

  public static final String LOAD_TOPICS_AUTHORIZATION_EXPRESSION =
      """
          ANY(OWNER, METALAKE, CATALOG) ||
          SCHEMA_OWNER_WITH_USE_CATALOG ||
          ANY_USE_CATALOG && ANY_USE_SCHEMA && (TOPIC::OWNER || ANY_CONSUME_TOPIC || ANY_PRODUCE_TOPIC)
          """;

  public static final String LOAD_FILESET_AUTHORIZATION_EXPRESSION =
      """
                 ANY(OWNER, METALAKE, CATALOG) ||
                 SCHEMA_OWNER_WITH_USE_CATALOG ||
                 ANY_USE_CATALOG && ANY_USE_SCHEMA && (FILESET::OWNER || ANY_READ_FILESET || ANY_WRITE_FILESET)
                  """;

  public static final String FILTER_SCHEMA_AUTHORIZATION_EXPRESSION =
      "ANY(OWNER, METALAKE, CATALOG, SCHEMA) || ANY_USE_SCHEMA";

  public static final String FILTER_MODEL_AUTHORIZATION_EXPRESSION =
      "ANY(OWNER, METALAKE, CATALOG, SCHEMA, MODEL) || ANY_USE_MODEL";

  public static final String LOAD_VIEW_AUTHORIZATION_EXPRESSION =
      """
                  ANY(OWNER, METALAKE, CATALOG) ||
                  SCHEMA_OWNER_WITH_USE_CATALOG ||
                  ANY_USE_CATALOG && ANY_USE_SCHEMA && (VIEW::OWNER || ANY_SELECT_VIEW || ANY_CREATE_VIEW)
                  """;

  public static final String FILTER_TABLE_AUTHORIZATION_EXPRESSION =
      """
                  ANY(OWNER, METALAKE, CATALOG, SCHEMA, TABLE) ||
                  ANY_SELECT_TABLE ||
                  ANY_MODIFY_TABLE
                  """;

  public static final String FILTER_VIEW_AUTHORIZATION_EXPRESSION =
      """
                  ANY(OWNER, METALAKE, CATALOG, SCHEMA, VIEW) ||
                  ANY_SELECT_VIEW
                  """;

  public static final String FILTER_MODIFY_TABLE_AUTHORIZATION_EXPRESSION =
      """
                  ANY(OWNER, METALAKE, CATALOG, SCHEMA, TABLE) ||
                  ANY_MODIFY_TABLE
                  """;

  public static final String FILTER_WRITE_FILESET_AUTHORIZATION_EXPRESSION =
      """
                  ANY(OWNER, METALAKE, CATALOG, SCHEMA, FILESET) ||
                  ANY_WRITE_FILESET
                  """;

  public static final String FILTER_TOPICS_AUTHORIZATION_EXPRESSION =
      """
              ANY(OWNER, METALAKE, CATALOG, SCHEMA, TOPIC) ||
              ANY_CONSUME_TOPIC ||
              ANY_PRODUCE_TOPIC
       """;

  public static final String FILTER_FILESET_AUTHORIZATION_EXPRESSION =
      """
              ANY(OWNER, METALAKE, CATALOG, SCHEMA, FILESET) ||
              ANY_READ_FILESET ||
              ANY_WRITE_FILESET
                  """;

  public static final String LOAD_ROLE_AUTHORIZATION_EXPRESSION =
      """
          METALAKE::OWNER || METALAKE::MANAGE_GRANTS
          || ROLE::OWNER || ROLE::SELF
          """;

  public static final String CAN_ACCESS_METADATA = "CAN_ACCESS_METADATA";

  public static final String CAN_ACCESS_METADATA_AND_TAG =
      """
          METALAKE::OWNER ||
          ((CAN_ACCESS_METADATA) && (TAG::OWNER || ANY_APPLY_TAG))
          """;

  public static final String LOAD_TAG_AUTHORIZATION_EXPRESSION =
      "METALAKE::OWNER || TAG::OWNER || ANY_APPLY_TAG";

  public static final String APPLY_TAG_AUTHORIZATION_EXPRESSION =
      "METALAKE::OWNER || TAG::OWNER || ANY_APPLY_TAG";

  public static final String LOAD_POLICY_AUTHORIZATION_EXPRESSION =
      """
          METALAKE::OWNER || POLICY::OWNER || ANY_APPLY_POLICY
          """;

  /**
   * Special case: "METALAKE_USER" is used here as a unique authorization token, not a logical
   * expression like other constants. This is intentional and required for metalake-level user
   * authorization checks {@link
   * org.apache.gravitino.authorization.GravitinoAuthorizer#isMetalakeUser(String)}.
   */
  public static final String LOAD_METALAKE_AUTHORIZATION_EXPRESSION = "METALAKE_USER";

  public static final String LOAD_JOB_AUTHORIZATION_EXPRESSION = "METALAKE::OWNER || JOB::OWNER";

  public static final String LOAD_JOB_TEMPLATE_AUTHORIZATION_EXPRESSION =
      "METALAKE::OWNER || JOB_TEMPLATE::OWNER || ANY_USE_JOB_TEMPLATE";

  public static final String REQUEST_REQUIRED_PRIVILEGES_CONTAINS_MODIFY_TABLE =
      "REQUEST::REQUIRED_PRIVILEGES_CONTAINS_MODIFY_TABLE";
}
