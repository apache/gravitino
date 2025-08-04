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

import { defHttp } from '@/lib/utils/axios'
import { ContentTypeEnum } from '@/lib/enums/httpEnum'

export const getAuthConfigsApi = () => {
  return defHttp.get({
    url: '/configs'
  })
}

export const loginApi = (url, params) => {
  return defHttp.post(
    {
      url,
      data: params,
      headers: {
        'Content-Type': ContentTypeEnum.FORM_URLENCODED
      }
    },
    { withToken: false }
  )
}

export const initiateOAuthFlowApi = redirectUri => {
  const params = new URLSearchParams()
  if (redirectUri) {
    params.append('redirect_uri', redirectUri)
  }

  // This will redirect the browser to the OAuth provider
  window.location.href = `/api/oauth/authorize${params.toString() ? '?' + params.toString() : ''}`
}

export const handleOAuthCallbackApi = () => {
  // The callback is handled by the backend endpoint automatically
  // This function is here for completeness but not typically called directly
  return defHttp.get({
    url: '/api/oauth/callback'
  })
}

export const getOAuthConfigsApi = () => {
  return defHttp.get({
    url: '/api/oauth/config',
    headers: { Accept: 'application/json' }
  })
}
