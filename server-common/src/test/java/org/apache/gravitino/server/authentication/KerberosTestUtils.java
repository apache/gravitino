/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

// Referred from Apache Hadoop KerberosTestUtils.java
// Remove part methods
// hadoop-common-project/hadoop-auth/src/test/java/org/apache/hadoop/security/\
// authentication/KerberosTestUtils.java

package org.apache.gravitino.server.authentication;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.Callable;
import javax.security.auth.login.LoginContext;
import org.apache.gravitino.auth.KerberosUtils;

/** Test helper class for Java Kerberos setup. */
public class KerberosTestUtils {
  private static String keytabFile =
      new File(System.getProperty("test.dir", "target"), UUID.randomUUID().toString())
          .getAbsolutePath();

  public static String getRealm() {
    return "EXAMPLE.COM";
  }

  public static String getClientPrincipal() {
    return "client@EXAMPLE.COM";
  }

  public static String getServerPrincipal() {
    return "HTTP/localhost@EXAMPLE.COM";
  }

  public static String getKeytabFile() {
    return keytabFile;
  }

  public static <T> T doAsClient(Callable<T> callable) throws Exception {
    LoginContext context = null;
    try {
      context = KerberosUtils.login(getClientPrincipal(), getKeytabFile());
      return KerberosUtils.doAs(context.getSubject(), callable);
    } finally {
      if (context != null) {
        context.logout();
      }
    }
  }
}
