/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.auth;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.exceptions.UnauthorizedException;
import org.apache.commons.lang3.StringUtils;
import org.ietf.jgss.GSSManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KeyTab;
import java.io.File;
import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;

public class KerberosAuthenticator implements Authenticator {

    public static final Logger LOG = LoggerFactory.getLogger(
            KerberosAuthenticator.class);
    private static final String PRINCIPAL = "kerberos.principal";

    private final Subject serverSubject = new Subject();
    private GSSManager gssManager;

    @Override
    public void initialize(Config config) throws RuntimeException {
        try {
        String principal = config.get(KerberosConfig.PRINCIPAL);
        String keytab = config.get(KerberosConfig.KEYTAB);
        File keytabFile = new File(keytab);
        if (!keytabFile.exists()) {
            throw new IllegalArgumentException("Keytab doesn't exist: " + keytab);
        }

        Principal krbPrincipal = new KerberosPrincipal(principal);
        LOG.info("Using keytab {}, for principal {}",
                keytab, krbPrincipal);
        serverSubject.getPrincipals().add(krbPrincipal);
        KeyTab keytabInstance = KeyTab.getInstance(keytabFile);
        serverSubject.getPrivateCredentials().add(keytabInstance);
        gssManager = Subject.doAs(serverSubject,
                new PrivilegedExceptionAction<GSSManager>() {
                    @Override
                    public GSSManager run() throws Exception {
                        return GSSManager.getInstance();
                    }
                });
        } catch (PrivilegedActionException | IOException ex) {
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
            throw new UnauthorizedException("Empty token authorization header");
        }
        String authData = new String(tokenData);
        if (StringUtils.isBlank(authData)
                || !authData.startsWith(AuthConstants.AUTHORIZATION_NEGOTIATE_HEADER)) {
            throw new UnauthorizedException("Invalid token authorization header");
        }
        String token = authData.substring(AuthConstants.AUTHORIZATION_NEGOTIATE_HEADER.length());
        byte[] clientToken = Base64.getDecoder().decode(token);
        if (StringUtils.isBlank(token)) {
            throw new UnauthorizedException("Blank token found");
        }
        try {
            String serverPrincipal =

        }
    }

}
