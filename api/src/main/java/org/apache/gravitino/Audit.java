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
package org.apache.gravitino;

import java.time.Instant;
import org.apache.gravitino.annotation.Evolving;

/** Represents the audit information of an entity. */
@Evolving
public interface Audit {

  /**
   * The creator of the entity.
   *
   * @return the creator of the entity.
   */
  String creator();

  /**
   * The creation time of the entity.
   *
   * @return The creation time of the entity.
   */
  Instant createTime();

  /**
   * @return The last modifier of the entity.
   */
  String lastModifier();

  /**
   * @return The last modified time of the entity.
   */
  Instant lastModifiedTime();
}
