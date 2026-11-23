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

const SECRETS = ['oauth_client_secret', 'basic_auth_password']

// Optional fields that the gateway takes as null, not as an empty string.
const OPTIONAL_URLS = ['oauth_metadata_url']

/**
 * The fields to send for a server from its form. An empty secret keeps the current
 * one; `remove_oauth_client_secret` removes the client secret, which the gateway
 * does when sent an empty one. An empty optional URL is none.
 */
export const serverFields = ({ remove_oauth_client_secret: removeClientSecret, ...values }) => {
  const fields = Object.fromEntries(
    Object.entries(values)
      .filter(([key, value]) => !SECRETS.includes(key) || value)
      .map(([key, value]) => [key, OPTIONAL_URLS.includes(key) && !value ? null : value])
  )

  return removeClientSecret ? { ...fields, oauth_client_secret: '' } : fields
}

// Whether a server has basic auth credentials already, for people or for service accounts.
export const hasBasicAuth = server => server?.auth_type === 'basic' || server?.service_auth_type === 'basic'
