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

package org.apache.gravitino.idp.basic.password;

/** Default parameters for the built-in SHA3-512 password hasher. */
public final class Sha3512Defaults {

  /** JCA {@link java.security.MessageDigest} algorithm name for SHA3-512. */
  public static final String DIGEST_ALGORITHM = "SHA3-512";

  /** Default iteration count for salted SHA3-512 password stretching. */
  public static final int DEFAULT_ITERATIONS = 100_000;

  /** PHC-style prefix for stored password hashes. */
  public static final String PHC_PREFIX = "$sha3-512$";

  /** Default salt length in bytes. */
  public static final int DEFAULT_SALT_LENGTH = 16;

  /** SHA3-512 digest output length in bytes. */
  public static final int DEFAULT_HASH_LENGTH = 64;

  private Sha3512Defaults() {}
}
