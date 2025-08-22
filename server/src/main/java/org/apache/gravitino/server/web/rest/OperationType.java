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
package org.apache.gravitino.server.web.rest;

public enum OperationType {
  LIST,
  CREATE,
  LOAD,
  ALTER,
  DROP,
  ENABLE,
  DISABLE,
  /** This is a special operation type that is used to get a partition from a table. */
  GET,
  ADD,
  REMOVE,
  DELETE,
  GRANT,
  REVOKE,
  ASSOCIATE,
  SET,
  REGISTER, // An operation to register a model
  LIST_VERSIONS, // An operation to list versions of a model
  LINK, // An operation to link a version to a model
  RUN, // An operation to run a job
  CANCEL, // An operation to cancel a job
  UPDATE
}
