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
package org.apache.gravitino.server.authorization.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.gravitino.MetadataObject;

/**
 * This annotation is used to implement unified authentication in AOP. Use Expressions to define the
 * required privileges for an API.
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface AuthorizationExpression {
  /**
   * The expression to evaluate for authorization, which represents multiple privileges.
   *
   * @return the expression to evaluate for authorization.
   */
  String expression() default "";

  /**
   * Used to identify the type of metadata that needs to be accessed
   *
   * @return accessMetadataType
   */
  MetadataObject.Type accessMetadataType() default MetadataObject.Type.METALAKE;

  /**
   * Error message when authorization failed.
   *
   * @return error message
   */
  String errorMessage() default "";
}
