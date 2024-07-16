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

import java.util.UUID;

/**
 * Random id generator. This is used to generate random ids for entities. Please see {@link
 * org.apache.gravitino.meta.BaseMetalake#ID} for more details.
 */
public class RandomIdGenerator implements IdGenerator {

  public static final RandomIdGenerator INSTANCE = new RandomIdGenerator();

  public static final long MAX_ID = 0x7fffffffffffffffL;

  @Override
  public long nextId() {
    // Make sure this is a positive number.
    return UUID.randomUUID().getLeastSignificantBits() & MAX_ID;
  }
}
