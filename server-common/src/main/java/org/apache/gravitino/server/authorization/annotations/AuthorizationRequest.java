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

package org.apache.gravitino.server.authorization.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface AuthorizationRequest {

  RequestType type() default RequestType.COMMON;

  enum RequestType {
    COMMON,
    ASSOCIATE_TAG,
    BIND_TAG_POLICY,
    UNBIND_TAG_POLICY,
    ALTER_POLICY,
    /** Creates a policy with type-specific authorization. */
    CREATE_POLICY,
    RUN_JOB,
    LINEAGE,
    LOAD_TABLE,
    /** Loads a view with authorized existence probing. */
    LOAD_VIEW,
    CREATE_SCHEMA,
    TEST_CATALOG_CONNECTION,
    /** Retrieves credentials with provider-specific authorization. */
    GET_CREDENTIALS
  }
}
