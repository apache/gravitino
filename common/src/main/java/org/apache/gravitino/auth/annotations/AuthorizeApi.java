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
package org.apache.gravitino.auth.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privilege;

/**
 * This annotation is used to implement unified authentication in AOP.
 *
 * @author pancx
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface AuthorizeApi {
  /**
   * The list of privileges required to access the API.
   *
   * @return the list of privileges required to access the API.
   */
  Privilege.Name[] privileges() default {};

  /**
   * The resource type of the API.
   *
   * @return the resource type of the API.
   */
  MetadataObject.Type resourceType() default MetadataObject.Type.UNKNOWN;

  /**
   * The expression to evaluate for authorization.
   *
   * @return the expression to evaluate for authorization.
   */
  String expression() default "";

  /**
   * The rule to use for authorization. ether {@link AuthorizeType#EXPRESSION} or {@link
   * AuthorizeType#RESOURCE_TYPE}.
   *
   * @return the rule to use for authorization.
   */
  AuthorizeType rule() default AuthorizeType.RESOURCE_TYPE;

  /** The type of the authorization rule. */
  enum AuthorizeType {
    /** Use the expression to evaluate for authorization. */
    EXPRESSION,
    /** Use the resource type to evaluate for authorization. */
    RESOURCE_TYPE
  }
}
