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
package org.apache.gravitino.storage;

/** Property names for AWS static credentials shared across catalogs. */
public final class AWSProperties {

  /** AWS access key ID. Not hidden. */
  public static final String GRAVITINO_AWS_ACCESS_KEY_ID = "aws-access-key-id";

  /** AWS secret access key. Hidden. */
  public static final String GRAVITINO_AWS_SECRET_ACCESS_KEY = "aws-secret-access-key";

  private AWSProperties() {}
}
