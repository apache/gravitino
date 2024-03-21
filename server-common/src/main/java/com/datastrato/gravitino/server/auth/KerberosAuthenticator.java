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
package com.datastrato.gravitino.server.auth;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.UserPrincipal;
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.auth.KerberosUtils;
import com.datastrato.gravitino.exceptions.UnauthorizedException;
import com.google.common.base.Splitter;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.List;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KeyTab;
import org.apache.commons.lang3.StringUtils;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides Kerberos authentication mechanism. Referred from Apache Hadoop
 * KerberosAuthenticationHandler.java
 *
 * <p>KerberosAuthenticator doesn't support to use * as principal.
 *
 * <p>hadoop-common-project/hadoop-auth/src/main/java/org/apache/hadoop/\
 * security/authentication/server/KerberosAuthenticationHandler.java
 */
public class KerberosAuthenticator implements Authenticator {

  public static final Logger LOG = LoggerFactory.getLogger(KerberosAuthenticator.class);
  private final Subject serverSubject = new Subject();
  private GSSManager gssManager;

  @Override
  public void initialize(Config config) throws RuntimeException {
    try {
      String principal = config.get(KerberosConfig.PRINCIPAL);
      if (!principal.startsWith("HTTP/")) {
        throw new IllegalArgumentException("Principal must starts with `HTTP/`");
      }

      String keytab = config.get(KerberosConfig.KEYTAB);
      File keytabFile = new File(keytab);
      if (!keytabFile.exists()) {
        throw new IllegalArgumentException(String.format("Keytab %s doesn't exist", keytab));
      }

      if (!keytabFile.canRead()) {
        throw new IllegalArgumentException(String.format("Keytab %s can't be read", keytab));
      }

      Principal krbPrincipal = new KerberosPrincipal(principal);
      LOG.info("Using keytab {}, for principal {}", keytab, krbPrincipal);
      serverSubject.getPrincipals().add(krbPrincipal);
      KeyTab keytabInstance = KeyTab.getInstance(keytabFile);
      serverSubject.getPrivateCredentials().add(keytabInstance);

      gssManager =
          Subject.doAs(
              serverSubject,
              new PrivilegedExceptionAction<GSSManager>() {
                @Override
                public GSSManager run() throws Exception {
                  return GSSManager.getInstance();
                }
              });
    } catch (PrivilegedActionException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public boolean isDataFromToken() {
    return true;
  }

  @Override
  public Principal authenticateToken(byte[] tokenData) {
    if (tokenData == null) {
      throw new UnauthorizedException("Empty token authorization header", AuthConstants.NEGOTIATE);
    }

    String authData = new String(tokenData, StandardCharsets.UTF_8);
    if (StringUtils.isBlank(authData)
        || !authData.startsWith(AuthConstants.AUTHORIZATION_NEGOTIATE_HEADER)) {
      throw new UnauthorizedException(
          "Invalid token authorization header", AuthConstants.NEGOTIATE);
    }

    String token = authData.substring(AuthConstants.AUTHORIZATION_NEGOTIATE_HEADER.length());
    byte[] clientToken = Base64.getDecoder().decode(token);
    if (StringUtils.isBlank(token)) {
      throw new UnauthorizedException("Blank token found", AuthConstants.NEGOTIATE);
    }

    try {
      String serverPrincipal = KerberosServerUtils.getTokenServerName(clientToken);
      if (!serverPrincipal.startsWith("HTTP/")) {
        throw new IllegalArgumentException("Principal must start with `HTTP/`");
      }

      return Subject.doAs(
          serverSubject,
          new PrivilegedExceptionAction<Principal>() {
            @Override
            public Principal run() throws Exception {
              return retrievePrincipalFromToken(serverPrincipal, clientToken);
            }
          });
    } catch (Exception e) {
      LOG.warn("Fail to validate the token, exception: ", e);
      throw new UnauthorizedException("Fail to validate the token", AuthConstants.NEGOTIATE);
    }
  }

  private Principal retrievePrincipalFromToken(String serverPrincipal, byte[] clientToken)
      throws GSSException {
    GSSContext gssContext = null;
    GSSCredential gssCreds = null;
    try {
      if (LOG.isTraceEnabled()) {
        LOG.trace("SPNEGO initiated with server principal [{}]", serverPrincipal);
      }

      gssCreds =
          this.gssManager.createCredential(
              this.gssManager.createName(serverPrincipal, KerberosUtils.NT_GSS_KRB5_PRINCIPAL_OID),
              GSSCredential.INDEFINITE_LIFETIME,
              new Oid[] {KerberosUtils.GSS_SPNEGO_MECH_OID, KerberosUtils.GSS_KRB5_MECH_OID},
              GSSCredential.ACCEPT_ONLY);

      gssContext = this.gssManager.createContext(gssCreds);
      byte[] serverToken = gssContext.acceptSecContext(clientToken, 0, clientToken.length);

      String authenticateToken = null;
      if (serverToken != null && serverToken.length > 0) {
        authenticateToken = Base64.getEncoder().encodeToString(serverToken);
      }

      if (!gssContext.isEstablished()) {
        LOG.trace("SPNEGO in progress");
        String challenge = AuthConstants.AUTHORIZATION_NEGOTIATE_HEADER + authenticateToken;
        throw new UnauthorizedException("GssContext isn't established", challenge);
      }

      // Usually principal names are in the form 'user/instance@REALM' or 'user@REALM'.
      List<String> principalComponents =
          Splitter.on('@').splitToList(gssContext.getSrcName().toString());
      if (principalComponents.size() != 2) {
        throw new UnauthorizedException("Principal has wrong format", AuthConstants.NEGOTIATE);
      }

      String user = principalComponents.get(0);
      // TODO: We will have KerberosUserPrincipal in the future.
      //  We can put more information of Kerberos to the KerberosUserPrincipal
      // For example, we can put the token into the KerberosUserPrincipal,
      // We can return the token to the client in the AuthenticationFilter. It will be convenient
      // for client to establish the security context. Hadoop uses the cookie to store the token.
      // For now, we don't store it in the cookie. I can have a simple implementation. first.
      // It's also not required for the protocol.
      // https://datatracker.ietf.org/doc/html/rfc4559
      return new UserPrincipal(user);
    } finally {
      if (gssContext != null) {
        gssContext.dispose();
      }

      if (gssCreds != null) {
        gssCreds.dispose();
      }
    }
  }
}
