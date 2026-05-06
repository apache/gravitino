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

package org.apache.gravitino.auth.local.password;

import org.bouncycastle.crypto.params.Argon2Parameters;

/** Default parameters for the built-in Argon2id password hasher. */
public final class Argon2idDefaults {

  public static final int DEFAULT_VERSION = Argon2Parameters.ARGON2_VERSION_13;
  public static final int DEFAULT_TYPE = Argon2Parameters.ARGON2_id;
  public static final int DEFAULT_HASH_LENGTH = 32;
  public static final int DEFAULT_MEMORY_KB = 1 << 16;
  public static final int DEFAULT_ITERATIONS = 3;
  public static final int DEFAULT_PARALLELISM = 1;
  public static final int DEFAULT_SALT_LENGTH = 16;

  private Argon2idDefaults() {}
}
