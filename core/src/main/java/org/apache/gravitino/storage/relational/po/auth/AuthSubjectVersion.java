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
package org.apache.gravitino.storage.relational.po.auth;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * One row of the batched user + group version probe. The {@code subjectType} literal column
 * (carrying {@code "USER"} or {@code "GROUP"}) lets the caller split a single UNION result back
 * into the per-subject {@code UserUpdatedAt} / {@code GroupUpdatedAt} snapshots used by the cache
 * layer.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class AuthSubjectVersion {

  /** {@code "USER"} or {@code "GROUP"}. */
  private String subjectType;

  /** {@code user_id} or {@code group_id}. */
  private long id;

  /** {@code user_name} or {@code group_name}; used to match the row back to the requested key. */
  private String name;

  /** {@code updated_at} value used as the cache staleness sentinel. */
  private long updatedAt;
}
