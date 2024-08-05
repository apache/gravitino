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
package org.apache.gravitino.authorization;

/**
 * Every metadata object has an owner. The owner can have all their privileges. The owner could be a
 * user or a group. The owner could be transferred to another user or group.
 */
public interface Owner {

  /**
   * The name of the owner.
   *
   * @return The name of the owner.
   */
  String name();

  /**
   * The type of the owner. Only supports user or group.
   *
   * @return The type of the owner.
   */
  Type type();

  /** The type of the owner. */
  enum Type {
    /** The type of the owner is a user. */
    USER,
    /** The type of the owner is a group. */
    GROUP
  }
}
