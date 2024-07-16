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
package org.apache.gravitino.integration.test.container;

public enum PGImageName {
  VERSION_12("postgres:12"),
  VERSION_13("postgres:13"),
  VERSION_14("postgres:14"),
  VERSION_15("postgres:15"),
  VERSION_16("postgres:16");

  private final String imageName;

  PGImageName(String imageName) {
    this.imageName = imageName;
  }

  @Override
  public String toString() {
    return this.imageName;
  }
}
