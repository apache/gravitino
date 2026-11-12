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
import { AxiosError } from 'axios'
import toast from 'react-hot-toast'
import { defHttp } from '@/lib/utils/axios'
import { getGitHubApi } from '@/lib/api/github'

vi.mock('react-hot-toast', () => ({ default: { error: vi.fn() } }))
vi.mock('@/lib/provider/session', () => ({ useAuth: vi.fn() }))
vi.mock('@/lib/auth/providers/factory', () => ({ oauthProviderFactory: {} }))

describe('GitHub repository statistics', () => {
  let originalAdapter

  beforeEach(() => {
    originalAdapter = defHttp.getAxios().defaults.adapter
    vi.clearAllMocks()
  })

  afterEach(() => {
    defHttp.getAxios().defaults.adapter = originalAdapter
  })

  it('rejects rate-limited requests without displaying a global error', async () => {
    defHttp.getAxios().defaults.adapter = async config => {
      throw new AxiosError('Request failed with status code 403', 'ERR_BAD_REQUEST', config, null, {
        status: 403,
        data: { message: 'API rate limit exceeded' },
        config
      })
    }

    await expect(getGitHubApi()).rejects.toMatchObject({ response: { status: 403 } })
    expect(toast.error).not.toHaveBeenCalled()
  })

  it('rejects network failures without reporting that Gravitino is unavailable', async () => {
    defHttp.getAxios().defaults.adapter = async config => {
      throw new AxiosError('Network Error', 'ERR_NETWORK', config)
    }

    await expect(getGitHubApi()).rejects.toMatchObject({ code: 'ERR_NETWORK' })
    expect(toast.error).not.toHaveBeenCalled()
  })

  it('returns repository statistics on success', async () => {
    const data = { stargazers_count: 100, forks_count: 20 }
    defHttp.getAxios().defaults.adapter = async config => ({ status: 200, data, config })

    await expect(getGitHubApi()).resolves.toEqual(data)
    expect(toast.error).not.toHaveBeenCalled()
  })

  it('preserves global error messages for Gravitino requests', async () => {
    defHttp.getAxios().defaults.adapter = async config => {
      throw new AxiosError('Request failed with status code 403', 'ERR_BAD_REQUEST', config, null, {
        status: 403,
        data: { message: 'Permission denied' },
        config
      })
    }

    await expect(defHttp.get({ url: '/api/metalakes' })).rejects.toMatchObject({ response: { status: 403 } })
    expect(toast.error).toHaveBeenCalledWith('Permission denied', { id: 'global_error_message_status_403' })
  })
})
