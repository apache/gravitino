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

/**
 * Doris Docker image versions for multi-version integration testing.
 *
 * <p>{@link #VERSION_1_2} uses the Gravitino CI all-in-one image (FE + BE in a single image) for
 * backward-compatible testing against Doris 1.2.x.
 *
 * <p>{@link #VERSION_3_0} uses the official Apache Doris split images ({@code apache/doris:fe-3.x}
 * and {@code apache/doris:be-3.x}). {@link DorisContainer} detects the {@code :fe-} prefix and
 * automatically derives the corresponding BE image, passing both via compose environment variables.
 *
 * @see DorisContainer
 * @see ContainerSuite#startDorisContainer(DorisImageName)
 */
public enum DorisImageName {
  VERSION_1_2("apache/gravitino-ci:doris-0.1.5"),
  VERSION_3_0("apache/doris:fe-3.0.6.2"),
  VERSION_4_0("apache/doris:fe-4.0.6");

  private final String imageName;

  DorisImageName(String imageName) {
    this.imageName = imageName;
  }

  @Override
  public String toString() {
    return this.imageName;
  }
}
