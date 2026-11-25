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

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { getAuthConfigsApi } from '@/lib/api/auth'
import { oauthProviderFactory } from '@/lib/auth/providers/factory'
import { defHttp } from '@/lib/utils/axios'

describe('getAuthConfigsApi', () => {
  let adapter
  let originalAdapter
  let getAccessToken

  beforeEach(() => {
    localStorage.clear()
    sessionStorage.clear()

    const axiosInstance = defHttp.getAxios()
    originalAdapter = axiosInstance.defaults.adapter
    adapter = vi.fn(async config => ({
      data: {},
      status: 200,
      statusText: 'OK',
      headers: {},
      config
    }))
    axiosInstance.defaults.adapter = adapter
    getAccessToken = vi.spyOn(oauthProviderFactory, 'getAccessToken').mockResolvedValue('oauth-token')
  })

  afterEach(() => {
    defHttp.getAxios().defaults.adapter = originalAdapter
    vi.restoreAllMocks()
    localStorage.clear()
    sessionStorage.clear()
  })

  it.each(['oauth', 'basic', 'simple', null])('omits Authorization for auth type %s', async authType => {
    if (authType) {
      localStorage.setItem('authType', authType)
    }

    if (authType === 'basic') {
      sessionStorage.setItem('accessToken', 'basic-token')
    } else if (authType === 'simple') {
      sessionStorage.setItem('simpleAuthUser', JSON.stringify({ name: 'simple-user' }))
    } else if (authType === null) {
      localStorage.setItem('accessToken', 'stale-token')
    }

    await getAuthConfigsApi()

    const requestConfig = adapter.mock.calls[0][0]
    expect(requestConfig.url).toBe('/configs')
    expect(requestConfig.headers.get('Authorization')).toBeUndefined()
    expect(getAccessToken).not.toHaveBeenCalled()
  })
})
