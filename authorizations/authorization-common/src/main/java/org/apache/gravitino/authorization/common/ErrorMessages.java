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

package org.apache.gravitino.authorization.common;

public class ErrorMessages {
  public static final String MISSING_REQUIRED_ARGUMENT = "%s is required";
  public static final String PRIVILEGE_NOT_SUPPORTED =
      "The privilege %s is not supported for the securable object: %s";
  public static final String OWNER_PRIVILEGE_NOT_SUPPORTED =
      "The owner privilege is not supported for the securable " + "object: %s";
  public static final String INVALID_POLICY_NAME = "The policy name (%s) is invalid!";
}
