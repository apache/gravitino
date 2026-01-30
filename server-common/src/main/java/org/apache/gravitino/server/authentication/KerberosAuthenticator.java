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
package org.apache.gravitino.server.authentication;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KeyTab;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auth.KerberosUtils;
import org.apache.gravitino.auth.PrincipalMapper;
import org.apache.gravitino.auth.PrincipalMapperFactory;
import org.apache.gravitino.exceptions.UnauthorizedException;
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
  private PrincipalMapper principalMapper;

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

      // Initialize principal mapper. The default regex pattern '([^@]+).*'
      // extracts everything before '@' (e.g., "user" from "user@REALM",
      // "HTTP/host" from "HTTP/host@REALM")
      String mapperType = config.get(KerberosConfig.PRINCIPAL_MAPPER);
      String regexPattern = config.get(KerberosConfig.PRINCIPAL_MAPPER_REGEX_PATTERN);
      this.principalMapper = PrincipalMapperFactory.create(mapperType, regexPattern);

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

  @Override
  public boolean supportsToken(byte[] tokenData) {
    return tokenData != null
        && new String(tokenData, StandardCharsets.UTF_8)
            .startsWith(AuthConstants.AUTHORIZATION_NEGOTIATE_HEADER);
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

      // Extract principal from GSS context and map it using configured mapper
      String principalString = gssContext.getSrcName().toString();

      // Use principal mapper to extract the principal
      // This allows for flexible mapping strategies (regex, kerberos-specific parsing, etc.)
      // The mapper will handle validation and extraction based on its configured type
      return principalMapper.map(principalString);
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
