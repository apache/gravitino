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

/** Default parameters for the built-in PBKDF2-HMAC-SHA512 password hasher. */
public final class Pbkdf2Sha512Defaults {

  /** JCA algorithm name for PBKDF2 with HMAC-SHA512. */
  public static final String PBKDF2_ALGORITHM = "PBKDF2WithHmacSHA512";

  /** OWASP minimum iteration count for PBKDF2-HMAC-SHA512. */
  public static final int DEFAULT_ITERATIONS = 210_000;

  /** PHC string prefix for stored password hashes. */
  public static final String PHC_PREFIX = "$pbkdf2-sha512$";

  /** Number of bytes in the derived password hash. */
  public static final int DEFAULT_HASH_LENGTH = 32;

  /** Number of bytes in the random salt. */
  public static final int DEFAULT_SALT_LENGTH = 16;

  private Pbkdf2Sha512Defaults() {}
}
