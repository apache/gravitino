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

package org.apache.gravitino.hive.client;

import java.util.Properties;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;

/**
 * Integration test for Kerberos-enabled Hive2 HMS with impersonation <b>disabled</b>.
 *
 * <p>This test covers the fix for the non-impersonation Kerberos path in {@link HiveClientFactory}:
 * the path was missing a {@code realUgi.doAs()} wrapper, causing GSSAPI to fail with "No valid
 * credentials provided" because the JAAS Subject (containing the TGT) was never bound to the
 * current thread.
 *
 * <p>The existing {@link TestHive2HMSWithKerberos} only tests {@code impersonation=true}. This
 * class reuses the same Docker KDC infrastructure but overrides {@link #createHiveProperties()} to
 * set {@code authentication.impersonation-enable=false}, exercising the previously broken code
 * path. {@link TestHive2HMSWithKerberos#startHiveContainer()} calls {@link #createHiveProperties()}
 * via polymorphism, so no {@code startHiveContainer} override is needed here.
 *
 * <p>Requires Docker; run with {@code -PskipDockerTests=false}.
 */
@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestHive2HMSWithKerberosNoImpersonation extends TestHive2HMSWithKerberos {

  /**
   * Override to disable impersonation. All other Kerberos properties (principal, keytab, sasl) are
   * inherited from {@link TestHive2HMSWithKerberos#createHiveProperties()}.
   *
   * <p>Setting {@code authentication.impersonation-enable=false} forces {@link HiveClientFactory}
   * to take the non-impersonation Kerberos branch in {@code createHiveClientInternal()}.
   */
  @Override
  protected Properties createHiveProperties() {
    Properties properties = super.createHiveProperties();
    properties.setProperty("authentication.impersonation-enable", "false");
    return properties;
  }
}
