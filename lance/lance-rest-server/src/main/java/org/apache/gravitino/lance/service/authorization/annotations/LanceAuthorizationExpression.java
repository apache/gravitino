/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.lance.service.authorization.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares the authorization expressions of a Lance REST namespace operation.
 *
 * <p>A Lance namespace ID is a single string whose level count decides which Gravitino entity it
 * addresses: zero levels is the root, one level is a catalog and two levels are a schema. The
 * required privileges therefore depend on the request, not only on the method, so an expression is
 * declared per level.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface LanceAuthorizationExpression {

  /**
   * The expression evaluated when the namespace ID addresses a catalog.
   *
   * @return the catalog-level authorization expression.
   */
  String catalogExpression();

  /**
   * The expression evaluated when the namespace ID addresses a schema.
   *
   * @return the schema-level authorization expression.
   */
  String schemaExpression();

  /**
   * Whether the root namespace ID is accepted by this operation.
   *
   * <p>The root itself carries no privileges: the caller only has to be a valid user of the
   * metalake, and the listed catalogs are filtered afterwards.
   *
   * @return {@code true} if a zero-level namespace ID is allowed.
   */
  boolean allowRootNamespace() default false;
}
