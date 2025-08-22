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

import { setupServer } from 'msw/node'
import { http, HttpResponse } from 'msw'

const handlers = [
  // Mock auth config endpoint (used by factory tests)
  http.get('/configs', () => {
    return HttpResponse.json({
      'gravitino.authenticator': 'oauth',
      'gravitino.authenticator.oauth.provider': 'oidc',
      'gravitino.authenticator.oauth.authority': 'https://test-oidc.example.com',
      'gravitino.authenticator.oauth.clientId': 'test-client-id',
      'gravitino.authenticator.oauth.scope': 'openid profile email'
    })
  })
]

export const server = setupServer(...handlers)
