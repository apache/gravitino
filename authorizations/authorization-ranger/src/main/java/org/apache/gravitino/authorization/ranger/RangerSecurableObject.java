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
package org.apache.gravitino.authorization.ranger;

import java.util.List;
import org.apache.gravitino.annotation.Unstable;

/**
 * The Ranger securable object is the entity which access can be granted. Unless allowed by a grant,
 * access is denied. <br>
 * You can use the helper class `RangerSecurableObjects` to create the Ranger securable object which
 * you need. <br>
 * There is a clear difference between Ranger's Securable Object and Gravitino's Securable Object,
 * Ranger's Securable Object does not have the concept of `METALAKE`, so it needs to be defined
 * specifically.
 */
@Unstable
public interface RangerSecurableObject extends RangerMetadataObject {
  /**
   * The privileges of the Ranger securable object.
   *
   * @return The privileges of the securable object.
   */
  List<RangerPrivilege> privileges();
}
