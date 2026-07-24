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
package org.apache.gravitino.server.authorization.jcasbin;

/**
 * The effect of a single privilege rule in the per-role policy index. {@link #DENY} beats {@link
 * #ALLOW} both within a single role and across the roles a user holds, mirroring the jcasbin {@code
 * policy_effect} {@code some(where (p.eft == allow)) && !some(where (p.eft == deny))}.
 */
enum Effect {
  /** The rule grants the privilege. */
  ALLOW,
  /** The rule denies the privilege; wins over any {@link #ALLOW}. */
  DENY
}
