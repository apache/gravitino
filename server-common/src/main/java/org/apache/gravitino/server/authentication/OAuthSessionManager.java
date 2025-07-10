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
package org.apache.gravitino.server.authentication;

import com.google.common.base.Strings;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OAuth Session Manager
 *
 * <p>Manages user sessions after successful OAuth authentication. Creates and validates Gravitino
 * session tokens for authenticated users.
 */
public class OAuthSessionManager {

  private static final Logger LOG = LoggerFactory.getLogger(OAuthSessionManager.class);
  private static final String HMAC_SHA256 = "HmacSHA256";
  private static final long DEFAULT_SESSION_DURATION_SECONDS = 3600; // 1 hour

  // In-memory session store (in production, consider using Redis or database)
  private final ConcurrentHashMap<String, SessionInfo> sessions = new ConcurrentHashMap<>();
  private final SecureRandom secureRandom = new SecureRandom();
  private final byte[] sessionSigningKey;

  public OAuthSessionManager() {
    // Generate a random signing key for session tokens
    // In production, this should be configurable and shared across instances
    this.sessionSigningKey = new byte[32];
    secureRandom.nextBytes(sessionSigningKey);
    LOG.info("OAuth Session Manager initialized");
  }

  /**
   * Create a new session for an authenticated user
   *
   * @param userInfo User information from OAuth provider
   * @return Session token that can be used for subsequent requests
   * @throws Exception if session creation fails
   */
  public String createSession(UserInfo userInfo) throws Exception {
    if (userInfo == null || Strings.isNullOrEmpty(userInfo.getEmail())) {
      throw new IllegalArgumentException("User info and email are required");
    }

    LOG.info("Creating session for user: {}", userInfo.getEmail());

    // Generate session ID
    String sessionId = generateSessionId();

    // Create session info
    SessionInfo sessionInfo = new SessionInfo();
    sessionInfo.setSessionId(sessionId);
    sessionInfo.setUserInfo(userInfo);
    sessionInfo.setExpiresAt(Instant.now().plusSeconds(DEFAULT_SESSION_DURATION_SECONDS));

    // Store session
    sessions.put(sessionId, sessionInfo);

    // Generate signed token
    String sessionToken = generateSignedToken(sessionInfo);

    LOG.debug("Session created with ID: {}", sessionId);
    return sessionToken;
  }

  /**
   * Validate and retrieve session information
   *
   * @param sessionToken Session token to validate
   * @return User information if session is valid
   * @throws Exception if session is invalid or expired
   */
  public UserInfo validateSession(String sessionToken) throws Exception {
    if (Strings.isNullOrEmpty(sessionToken)) {
      throw new IllegalArgumentException("Session token is required");
    }

    // Parse and validate token
    SessionInfo sessionInfo = parseSignedToken(sessionToken);

    // Check if session exists
    SessionInfo storedSession = sessions.get(sessionInfo.getSessionId());
    if (storedSession == null) {
      throw new Exception("Session not found");
    }

    // Check if session is expired
    if (Instant.now().isAfter(storedSession.getExpiresAt())) {
      sessions.remove(sessionInfo.getSessionId());
      throw new Exception("Session expired");
    }

    // Validate session info matches
    if (!storedSession.getSessionId().equals(sessionInfo.getSessionId())) {
      throw new Exception("Session validation failed");
    }

    LOG.debug("Session validated for user: {}", storedSession.getUserInfo().getEmail());
    return storedSession.getUserInfo();
  }

  /**
   * Invalidate a session
   *
   * @param sessionToken Session token to invalidate
   */
  public void invalidateSession(String sessionToken) {
    try {
      SessionInfo sessionInfo = parseSignedToken(sessionToken);
      sessions.remove(sessionInfo.getSessionId());
      LOG.info("Session invalidated: {}", sessionInfo.getSessionId());
    } catch (Exception e) {
      LOG.warn("Failed to invalidate session: {}", e.getMessage());
    }
  }

  /** Clean up expired sessions */
  public void cleanupExpiredSessions() {
    Instant now = Instant.now();
    int removedCount = 0;

    for (String sessionId : sessions.keySet()) {
      SessionInfo session = sessions.get(sessionId);
      if (session != null && now.isAfter(session.getExpiresAt())) {
        sessions.remove(sessionId);
        removedCount++;
      }
    }

    if (removedCount > 0) {
      LOG.info("Cleaned up {} expired sessions", removedCount);
    }
  }

  /** Generate a unique session ID */
  private String generateSessionId() {
    byte[] sessionBytes = new byte[16];
    secureRandom.nextBytes(sessionBytes);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(sessionBytes);
  }

  /** Generate a signed token for the session */
  private String generateSignedToken(SessionInfo sessionInfo) throws Exception {
    // Create payload: sessionId:email:expiresAt
    String payload =
        sessionInfo.getSessionId()
            + ":"
            + sessionInfo.getUserInfo().getEmail()
            + ":"
            + sessionInfo.getExpiresAt().getEpochSecond();

    // Encode payload
    String encodedPayload =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(payload.getBytes(StandardCharsets.UTF_8));

    // Generate signature
    Mac mac = Mac.getInstance(HMAC_SHA256);
    SecretKeySpec keySpec = new SecretKeySpec(sessionSigningKey, HMAC_SHA256);
    mac.init(keySpec);
    byte[] signature = mac.doFinal(encodedPayload.getBytes(StandardCharsets.UTF_8));
    String encodedSignature = Base64.getUrlEncoder().withoutPadding().encodeToString(signature);

    // Return token: payload.signature
    return encodedPayload + "." + encodedSignature;
  }

  /** Parse and validate a signed token */
  private SessionInfo parseSignedToken(String token) throws Exception {
    String[] parts = token.split("\\.");
    if (parts.length != 2) {
      throw new Exception("Invalid token format");
    }

    String encodedPayload = parts[0];
    String providedSignature = parts[1];

    // Verify signature
    Mac mac = Mac.getInstance(HMAC_SHA256);
    SecretKeySpec keySpec = new SecretKeySpec(sessionSigningKey, HMAC_SHA256);
    mac.init(keySpec);
    byte[] expectedSignature = mac.doFinal(encodedPayload.getBytes(StandardCharsets.UTF_8));
    String expectedSignatureString =
        Base64.getUrlEncoder().withoutPadding().encodeToString(expectedSignature);

    if (!providedSignature.equals(expectedSignatureString)) {
      throw new Exception("Token signature verification failed");
    }

    // Decode payload
    String payload =
        new String(Base64.getUrlDecoder().decode(encodedPayload), StandardCharsets.UTF_8);
    String[] payloadParts = payload.split(":");
    if (payloadParts.length != 3) {
      throw new Exception("Invalid token payload format");
    }

    // Parse payload parts
    String sessionId = payloadParts[0];
    String email = payloadParts[1];
    long expiresAtSeconds = Long.parseLong(payloadParts[2]);

    // Create session info
    SessionInfo sessionInfo = new SessionInfo();
    sessionInfo.setSessionId(sessionId);
    sessionInfo.setExpiresAt(Instant.ofEpochSecond(expiresAtSeconds));

    UserInfo userInfo = new UserInfo();
    userInfo.setEmail(email);
    sessionInfo.setUserInfo(userInfo);

    return sessionInfo;
  }

  /** Internal session information */
  private static class SessionInfo {
    private String sessionId;
    private UserInfo userInfo;
    private Instant expiresAt;

    public String getSessionId() {
      return sessionId;
    }

    public void setSessionId(String sessionId) {
      this.sessionId = sessionId;
    }

    public UserInfo getUserInfo() {
      return userInfo;
    }

    public void setUserInfo(UserInfo userInfo) {
      this.userInfo = userInfo;
    }

    public Instant getExpiresAt() {
      return expiresAt;
    }

    public void setExpiresAt(Instant expiresAt) {
      this.expiresAt = expiresAt;
    }
  }
}
