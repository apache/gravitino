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

package org.apache.gravitino.auth.local;

import com.google.common.base.Preconditions;
import de.mkammerer.argon2.Argon2;
import de.mkammerer.argon2.Argon2Factory;
import org.apache.commons.lang3.StringUtils;

/** Argon2id-based password hasher. */
public class Argon2idPasswordHasher implements PasswordHasher {

  private static final int ITERATIONS = 3;
  private static final int MEMORY_KB = 1 << 16;
  private static final int PARALLELISM = 1;

  @Override
  public String hash(String plainPassword) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(plainPassword), "Plain password must not be blank");

    Argon2 argon2 = Argon2Factory.create(Argon2Factory.Argon2Types.ARGON2id);
    char[] passwordChars = plainPassword.toCharArray();
    try {
      return argon2.hash(ITERATIONS, MEMORY_KB, PARALLELISM, passwordChars);
    } finally {
      argon2.wipeArray(passwordChars);
    }
  }

  @Override
  public boolean verify(String plainPassword, String hashedPassword) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(plainPassword), "Plain password must not be blank");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(hashedPassword), "Hashed password must not be blank");

    Argon2 argon2 = Argon2Factory.create(Argon2Factory.Argon2Types.ARGON2id);
    char[] passwordChars = plainPassword.toCharArray();
    try {
      return argon2.verify(hashedPassword, passwordChars);
    } finally {
      argon2.wipeArray(passwordChars);
    }
  }
}
