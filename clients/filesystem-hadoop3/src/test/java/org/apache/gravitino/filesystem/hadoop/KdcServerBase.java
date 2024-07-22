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

package org.apache.gravitino.filesystem.hadoop;

import java.io.File;
import java.util.UUID;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;

public class KdcServerBase extends KerberosSecurityTestcase {
  private static final KerberosSecurityTestcase INSTANCE = new KerberosSecurityTestcase();
  private static final String CLIENT_PRINCIPAL = "client@EXAMPLE.COM";
  private static final String SERVER_PRINCIPAL = "HTTP/localhost@EXAMPLE.COM";
  private static final String KEYTAB_FILE =
      new File(System.getProperty("test.dir", "target"), UUID.randomUUID().toString())
          .getAbsolutePath();

  static {
    try {
      INSTANCE.startMiniKdc();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private KdcServerBase() {}

  public static void stopKdc() {
    INSTANCE.stopMiniKdc();
  }

  public static void initKeyTab() throws Exception {
    File keytabFile = new File(KEYTAB_FILE);
    String clientPrincipal = removeRealm(CLIENT_PRINCIPAL);
    String serverPrincipal = removeRealm(SERVER_PRINCIPAL);
    INSTANCE.getKdc().createPrincipal(keytabFile, clientPrincipal, serverPrincipal);
  }

  private static String removeRealm(String principal) {
    return principal.substring(0, principal.lastIndexOf("@"));
  }

  public static String getServerPrincipal() {
    return SERVER_PRINCIPAL;
  }

  public static String getClientPrincipal() {
    return CLIENT_PRINCIPAL;
  }

  public static String getKeytabFile() {
    return KEYTAB_FILE;
  }
}
