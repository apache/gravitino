/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 */

package org.apache.gravitino.client;

/**
 * Authentication method used by OAuth2 client credentials flow.
 */
public enum OAuth2ClientAuthenticationMethod {

  /**
   * Client credentials are sent in the request body.
   */
  CLIENT_SECRET_POST,

  /**
   * Client credentials are sent using HTTP Basic authentication.
   */
  CLIENT_SECRET_BASIC
}