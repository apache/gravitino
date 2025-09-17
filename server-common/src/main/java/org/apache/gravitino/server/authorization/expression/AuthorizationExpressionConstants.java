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
  public static final String loadCatalogAuthorizationExpression =
      "ANY_USE_CATALOG || ANY(OWNER, METALAKE, CATALOG)";

  public static final String loadSchemaAuthorizationExpression =
      " ANY(OWNER, METALAKE, CATALOG) || "
          + "ANY_USE_CATALOG && (SCHEMA::OWNER || ANY_USE_SCHEMA) ";

  public static final String loadModelAuthorizationExpression =
      "ANY(OWNER, METALAKE, CATALOG) ||"
          + " SCHEMA_OWNER_WITH_USE_CATALOG || "
          + " ANY_USE_CATALOG && ANY_USE_SCHEMA && (MODEL::OWNER || ANY_USE_MODEL)";

  public static final String loadTableAuthorizationExpression =
      "ANY(OWNER, METALAKE, CATALOG) ||"
          + "SCHEMA_OWNER_WITH_USE_CATALOG ||"
          + "ANY_USE_CATALOG && ANY_USE_SCHEMA  && (TABLE::OWNER || ANY_SELECT_TABLE || ANY_MODIFY_TABLE)";

  public static final String loadTopicsAuthorizationExpression =
      "ANY(OWNER, METALAKE, CATALOG) || "
          + "SCHEMA_OWNER_WITH_USE_CATALOG || "
          + "ANY_USE_CATALOG && ANY_USE_SCHEMA && (TOPIC::OWNER || ANY_CONSUME_TOPIC || ANY_PRODUCE_TOPIC)";

  public static final String loadFilesetAuthorizationExpression =
      "ANY(OWNER, METALAKE, CATALOG) || "
          + "SCHEMA_OWNER_WITH_USE_CATALOG || "
          + "ANY_USE_CATALOG && ANY_USE_SCHEMA && (FILESET::OWNER || ANY_READ_FILESET || ANY_WRITE_FILESET)";

  public static final String filterSchemaAuthorizationExpression =
      "ANY(OWNER, METALAKE, CATALOG, SCHEMA) || ANY_USE_SCHEMA";

  public static final String filterModelAuthorizationExpression =
      "ANY(OWNER, METALAKE, CATALOG, SCHEMA, MODEL) || ANY_USE_MODEL";

  public static final String filterTableAuthorizationExpression =
      "ANY(OWNER, METALAKE, CATALOG, SCHEMA, TABLE) || "
          + "ANY_SELECT_TABLE || "
          + "ANY_MODIFY_TABLE";

  public static final String filterTopicsAuthorizationExpression =
      "ANY(OWNER, METALAKE, CATALOG, SCHEMA, TOPIC) || "
          + "ANY_CONSUME_TOPIC || "
          + "ANY_PRODUCE_TOPIC";

  public static final String filterFilesetAuthorizationExpression =
      "ANY(OWNER, METALAKE, CATALOG, SCHEMA, FILESET) || "
          + "ANY_READ_FILESET || "
          + "ANY_WRITE_FILESET";
}
