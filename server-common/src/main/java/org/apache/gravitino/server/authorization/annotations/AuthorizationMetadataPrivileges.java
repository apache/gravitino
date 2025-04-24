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
import org.apache.gravitino.authorization.Privilege;

/**
 * Defines the annotation for authorizing access to an API. Use the resourceType and privileges
 * fields to define the required privileges and resource type for the API.
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface AuthorizationMetadataPrivileges {
  /**
   * The list of privileges required to access the API.
   *
   * @return the list of privileges required to access the API.
   */
  Privilege.Name[] privileges();

  /**
   * The resource type of the API.
   *
   * @return the resource type of the API.
   */
  MetadataObject.Type metadataType();
}
