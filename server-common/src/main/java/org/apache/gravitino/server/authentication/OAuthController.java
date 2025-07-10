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
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.GravitinoEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OAuth Authorization Code Flow Controller
 *
 * <p>Handles OAuth 2.0 Authorization Code Flow for "Login with Provider" functionality. Supports
 * multiple OAuth providers (Azure AD, Google, GitHub, etc.) through generic configuration.
 */
@Path("/oauth")
public class OAuthController {

  private static final Logger LOG = LoggerFactory.getLogger(OAuthController.class);
  private static final String STATE_SESSION_KEY = "oauth_state";
  private static final String REDIRECT_AFTER_LOGIN_KEY = "redirect_after_login";

  private OAuthAuthorizationCodeConfig oauthConfig;
  private OAuthTokenService tokenService;
  private JwtValidationService jwtValidator;

  public OAuthController() {
    // Initialize services - access configuration from GravitinoEnv
    this.oauthConfig =
        new OAuthAuthorizationCodeConfig(GravitinoEnv.getInstance().config().getAllConfig());
    this.tokenService = new OAuthTokenService(oauthConfig);

    // Initialize JwtValidationService with JWKS URI
    String jwksUri = GravitinoEnv.getInstance().config().get(OAuthAuthorizationCodeConfig.JWKS_URI);
    this.jwtValidator = new JwtValidationService(jwksUri);
  }

  /**
   * Initiate OAuth Authorization Code Flow
   *
   * <p>This endpoint redirects the user to the OAuth provider (e.g., Microsoft Azure AD) to begin
   * the login process. The user will authenticate with the provider and be redirected back to the
   * callback endpoint.
   *
   * @param request HTTP request
   * @param response HTTP response
   * @param redirectUri Optional redirect URI after successful login
   * @return Redirect response to OAuth provider
   */
  @GET
  @Path("/authorize")
  public Response authorize(
      @Context HttpServletRequest request,
      @Context HttpServletResponse response,
      @QueryParam("redirect_uri") String redirectUri) {

    try {
      LOG.info("Starting OAuth authorization flow");

      // Validate OAuth configuration
      if (!oauthConfig.isAuthorizationCodeFlowEnabled()) {
        LOG.error("OAuth Authorization Code Flow is not enabled");
        return Response.status(Response.Status.BAD_REQUEST).build();
      }

      // Generate state parameter for CSRF protection
      String state = UUID.randomUUID().toString();

      // Store state and redirect URI in session
      HttpSession session = request.getSession(true);
      session.setAttribute(STATE_SESSION_KEY, state);
      if (!Strings.isNullOrEmpty(redirectUri)) {
        session.setAttribute(REDIRECT_AFTER_LOGIN_KEY, redirectUri);
      }

      LOG.info("[OAuth /authorize] Session ID: {} | Set state: {}", session.getId(), state);

      // Build authorization URL
      String authorizationUrl = buildAuthorizationUrl(state);

      LOG.info("Redirecting to OAuth provider: {}", oauthConfig.getProviderName());
      LOG.info("Authorization URL: {}", authorizationUrl);

      // Redirect user to OAuth provider
      return Response.status(Response.Status.FOUND).location(URI.create(authorizationUrl)).build();

    } catch (Exception e) {
      LOG.error("Failed to initiate OAuth authorization", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * Handle OAuth callback from provider
   *
   * <p>This endpoint receives the authorization code from the OAuth provider, exchanges it for an
   * access token, validates the token, creates a Gravitino session, and redirects the user back to
   * the frontend.
   *
   * @param request HTTP request
   * @param response HTTP response
   * @param code Authorization code from OAuth provider
   * @param state State parameter for CSRF validation
   * @param error Error parameter if OAuth failed
   * @return Redirect response to frontend with token or error
   */
  @GET
  @Path("/callback")
  public Response callback(
      @Context HttpServletRequest request,
      @Context HttpServletResponse response,
      @QueryParam("code") String code,
      @QueryParam("state") String state,
      @QueryParam("error") String error) {

    try {
      LOG.info("Received OAuth callback");
      // Log session and state info for debugging
      HttpSession session = request.getSession(false);
      String sessionId = (session != null) ? session.getId() : "null";
      String sessionState =
          (session != null) ? (String) session.getAttribute(STATE_SESSION_KEY) : "null";
      LOG.info(
          "[OAuth /callback] Session ID: {} | Session state: {} | Received state: {}",
          sessionId,
          sessionState,
          state);

      // Handle OAuth error responses
      if (!Strings.isNullOrEmpty(error)) {
        LOG.error("OAuth provider returned error: {}", error);
        return redirectToFrontendWithError("oauth_provider_error", error);
      }

      // Validate required parameters
      if (Strings.isNullOrEmpty(code) || Strings.isNullOrEmpty(state)) {
        LOG.error("Missing required OAuth callback parameters");
        return redirectToFrontendWithError("missing_parameters", "Missing code or state parameter");
      }

      // Validate state parameter (CSRF protection)
      if (session == null || !isValidState(session, state)) {
        LOG.error("Invalid or missing state parameter");
        return redirectToFrontendWithError("invalid_state", "Invalid state parameter");
      }

      // Exchange authorization code for access token
      LOG.info("Exchanging authorization code for access token");
      OAuthTokenService.TokenResponse tokenResponse = tokenService.exchangeCodeForToken(code);

      // Validate and decode JWT token using JwtValidationService
      LOG.info("Validating JWT token from OAuth provider using JwtValidationService");
      UserInfo userInfo;
      try {
        userInfo = jwtValidator.validateToken(tokenResponse.getAccessToken());
      } catch (Exception ex) {
        LOG.error("JWT validation failed", ex);
        return redirectToFrontendWithError("jwt_validation_failed", ex.getMessage());
      }
      if (userInfo == null) {
        LOG.warn("JWT validation failed, creating placeholder user info");
        userInfo = new UserInfo();
        userInfo.setId("temp-user-" + System.currentTimeMillis());
        userInfo.setEmail("user@example.com");
      }

      // No longer create Gravitino session, just use the Azure AD JWT token
      LOG.info("Passing Azure AD JWT token to frontend for user: {}", userInfo.getEmail());
      String jwtToken = tokenResponse.getAccessToken();

      // Clean up session
      session.removeAttribute(STATE_SESSION_KEY);
      session.removeAttribute(REDIRECT_AFTER_LOGIN_KEY);

      // Redirect to frontend with JWT token
      String frontendUrl = buildFrontendRedirectUrl(jwtToken);

      LOG.info("OAuth login successful for user: {}", userInfo.getEmail());
      LOG.info("Redirecting to frontend: {}", frontendUrl);

      if (Strings.isNullOrEmpty(frontendUrl)) {
        LOG.error("Frontend URL is not configured or invalid");
        return redirectToFrontendWithError("frontend_url_error", "Frontend URL is not configured");
      }
      return Response.status(Response.Status.FOUND).location(URI.create(frontendUrl)).build();

    } catch (Exception e) {
      LOG.error("OAuth callback processing failed", e);
      return redirectToFrontendWithError("callback_processing_failed", e.getMessage());
    }
  }

  /**
   * Get OAuth configuration for frontend
   *
   * <p>Returns OAuth configuration information that the frontend needs to determine whether to show
   * OAuth login options.
   *
   * @return OAuth configuration response
   */
  @GET
  @Path("/config")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getConfig() {
    try {
      OAuthConfigResponse resp = new OAuthConfigResponse();
      if (oauthConfig != null && oauthConfig.isAuthorizationCodeFlowEnabled()) {
        resp.setProviderName(oauthConfig.getProviderName());
      }
      resp.setAuthorizationCodeFlowEnabled(oauthConfig.isAuthorizationCodeFlowEnabled());
      return Response.ok(resp).build();
    } catch (Exception e) {
      LOG.error("Failed to get OAuth configuration", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /** Build authorization URL for OAuth provider */
  private String buildAuthorizationUrl(String state) throws Exception {
    StringBuilder url = new StringBuilder(oauthConfig.getAuthorizationUrl());

    // Add query parameters
    url.append("?client_id=")
        .append(URLEncoder.encode(oauthConfig.getClientId(), StandardCharsets.UTF_8.name()));
    url.append("&response_type=code");
    url.append("&redirect_uri=")
        .append(URLEncoder.encode(oauthConfig.getRedirectUri(), StandardCharsets.UTF_8.name()));
    url.append("&scope=")
        .append(URLEncoder.encode(oauthConfig.getScope(), StandardCharsets.UTF_8.name()));
    url.append("&state=").append(URLEncoder.encode(state, StandardCharsets.UTF_8.name()));

    return url.toString();
  }

  /** Validate state parameter from session */
  private boolean isValidState(HttpSession session, String receivedState) {
    String sessionState = (String) session.getAttribute(STATE_SESSION_KEY);
    return sessionState != null && sessionState.equals(receivedState);
  }

  /** Build frontend redirect URL with token */
  private String buildFrontendRedirectUrl(String token) throws Exception {
    String baseUrl = oauthConfig.getFrontendBaseUrl();
    LOG.info("Frontend base URL: {}", baseUrl);
    if (!Strings.isNullOrEmpty(baseUrl)) {
      return baseUrl
          + "/ui/oauth/callback?access_token="
          + URLEncoder.encode(token, StandardCharsets.UTF_8.name());
    } else {
      return null;
    }
  }

  /** Redirect to frontend with error information */
  private Response redirectToFrontendWithError(String errorCode, String errorDescription) {
    try {
      String errorUrl =
          oauthConfig.getFrontendBaseUrl()
              + "/oauth/callback?error="
              + URLEncoder.encode(errorCode, StandardCharsets.UTF_8.name());

      if (!Strings.isNullOrEmpty(errorDescription)) {
        errorUrl +=
            "&error_description="
                + URLEncoder.encode(errorDescription, StandardCharsets.UTF_8.name());
      }

      return Response.status(Response.Status.FOUND).location(URI.create(errorUrl)).build();

    } catch (Exception e) {
      LOG.error("Failed to build error redirect URL", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /** OAuth configuration response for frontend */
  public static class OAuthConfigResponse {
    private String providerName;
    private boolean isAuthorizationCodeFlowEnabled;

    public String getProviderName() {
      return providerName;
    }

    public void setProviderName(String providerName) {
      this.providerName = providerName;
    }

    public boolean isAuthorizationCodeFlowEnabled() {
      return isAuthorizationCodeFlowEnabled;
    }

    public void setAuthorizationCodeFlowEnabled(boolean isAuthorizationCodeFlowEnabled) {
      this.isAuthorizationCodeFlowEnabled = isAuthorizationCodeFlowEnabled;
    }
  }
}
