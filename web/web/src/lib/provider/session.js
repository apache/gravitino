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

'use client'

import { createContext, useEffect, useState, useContext } from 'react'

import { useRouter } from 'next/navigation'
import { useSearchParams } from 'next/navigation'

import { useAppDispatch } from '@/lib/hooks/useStore'
import { initialVersion, fetchGitHubInfo } from '@/lib/store/sys'
import { oauthProviderFactory } from '@/lib/auth/providers/factory'

import { to } from '../utils'
import { getAuthConfigs, setAuthToken } from '../store/auth'

import { useIdle } from 'react-use'

const authProvider = {
  version: '',
  loading: true,
  setLoading: () => Boolean
}

const AuthContext = createContext(authProvider)

export const useAuth = () => useContext(AuthContext)

const AuthProvider = ({ children }) => {
  const router = useRouter()
  const searchParams = useSearchParams()
  const dispatch = useAppDispatch()

  const [loading, setLoading] = useState(authProvider.loading)
  const [token, setToken] = useState(null)

  const version = (typeof window !== 'undefined' && localStorage.getItem('version')) || null
  const paramsSize = [...searchParams.keys()].length

  const expiredIn = localStorage.getItem('expiredIn') && JSON.parse(localStorage.getItem('expiredIn')) // seconds
  const idleOn = (expiredIn + 60) * 1000
  const isIdle = useIdle(idleOn)

  useEffect(() => {
    if (isIdle) {
      localStorage.setItem('isIdle', true)
    }
  }, [isIdle])

  const goToMetalakeListPage = () => {
    if (paramsSize) {
      router.refresh()
    } else {
      router.push('/metalakes')
    }
  }

  useEffect(() => {
    const initAuth = async () => {
      const [authConfigsErr, resAuthConfigs] = await to(dispatch(getAuthConfigs()))
      const authType = resAuthConfigs?.payload?.authType

      // Always fetch GitHub info since it's a public API call
      dispatch(fetchGitHubInfo())

      if (authType === 'simple') {
        dispatch(initialVersion())
        goToMetalakeListPage()
      } else if (authType === 'oauth') {
        const tokenToUse = await oauthProviderFactory.getAccessToken()

        // Update local token state
        setToken(tokenToUse)

        if (tokenToUse) {
          dispatch(setAuthToken(tokenToUse))
          dispatch(initialVersion())
          goToMetalakeListPage()
        } else {
          // Don't redirect to login if we're on the OAuth callback page
          // Let the callback page handle the OAuth flow completion
          if (typeof window !== 'undefined' && window.location.pathname.startsWith('/ui/oauth/callback')) {
            return
          }
          router.push('/login')
        }
      }
    }

    initAuth()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const values = {
    version,
    token,
    loading
  }

  return <AuthContext.Provider value={values}>{children}</AuthContext.Provider>
}

export default AuthProvider
