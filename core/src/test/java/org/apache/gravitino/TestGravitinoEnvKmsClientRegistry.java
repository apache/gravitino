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

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsClientRegistry;
import org.apache.gravitino.encryption.kms.KmsReference;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoEnvKmsClientRegistry {

  @Test
  void testEmptyRegistryIsOptionalAndClosedWithEnvironment() throws IllegalAccessException {
    TestGravitinoEnv env = new TestGravitinoEnv();
    Assertions.assertThrows(IllegalStateException.class, env::kmsClientRegistry);

    KmsClientRegistry registry = new KmsClientRegistry(new Config(false) {});
    FieldUtils.writeField(env, "kmsClientRegistry", registry, true);

    Assertions.assertSame(registry, env.kmsClientRegistry());
    KmsReference reference = new KmsReference(KmsApi.AWS_KMS, "missing", "key");
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> registry.getKeyProperties(reference));

    env.shutdown();

    Assertions.assertThrows(IllegalStateException.class, env::kmsClientRegistry);
    Assertions.assertThrows(
        IllegalStateException.class, () -> registry.getKeyProperties(reference));
  }

  private static final class TestGravitinoEnv extends GravitinoEnv {}
}
