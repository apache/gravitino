/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { createContext, useEffect, useState, useContext } from 'react'

import { useRouter } from 'next/navigation'

import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import { initialVersion, setVersion as setStoreVersion } from '@/lib/store/sys'

import { useLocalStorage } from 'react-use'

import { getVersionApi } from '@/lib/api/version'
import { loginApi } from '@/lib/api/auth'

import { isProdEnv, to, loggerVersion } from '../utils'
import { getAuthConfigs, setAuthParams, setAuthToken, setExpiredIn, refreshToken } from '../store/auth'

const devOauthUrl = process.env.NEXT_PUBLIC_OAUTH_PATH

const authProvider = {
  version: '',
  loading: true,
  setLoading: () => Boolean,
  login: () => Promise.resolve(),
  logout: () => Promise.resolve()
}

const AuthContext = createContext(authProvider)

export const useAuth = () => useContext(AuthContext)

const AuthProvider = ({ children }) => {
  const router = useRouter()
  const [loading, setLoading] = useState(authProvider.loading)
  const [authParams, setLocalAuthParams] = useLocalStorage('authParams', '', { raw: true })
  const [localExpiredIn, setLocalExpiredIn] = useLocalStorage('expiredIn', '', { raw: true })
  const [token, setToken] = useLocalStorage('accessToken', '', { raw: true })
  const [version, setVersion] = useLocalStorage('version', authProvider.version, { raw: false })
  const authStore = useAppSelector(state => state.auth)
  const dispatch = useAppDispatch()

  const handleLogin = async params => {
    let oauthUrl = authStore.oauthUrl

    dispatch(setAuthParams(params))
    setLocalAuthParams(params)

    const getTokenAction = new Promise(async resolve => {
      const url = isProdEnv ? oauthUrl : devOauthUrl
      const [err, res] = await to(loginApi(url, params))

      if (err || !res) {
        throw new Error(err)
      }

      const { access_token, expires_in } = res.data

      setToken(access_token)
      dispatch(setAuthToken(access_token))
      setLocalExpiredIn(expires_in)
      dispatch(setExpiredIn(expires_in))
      resolve(access_token)
    })

    getTokenAction.then(async token => {
      if (!token) {
        throw new Error('Token not found')
      }
      const [verErr, resVer] = await to(getVersionApi())

      const { version } = resVer.data

      loggerVersion(version.version)
      setVersion(version)
      dispatch(setStoreVersion(version.version))
      dispatch(refreshToken())
      router.replace('/')
    })
  }

  const handleLogout = () => {
    setVersion('')
    dispatch(setStoreVersion(''))
    setToken('')
    dispatch(setAuthToken(''))
    router.push('/login')
  }

  useEffect(() => {
    const initAuth = async () => {
      // ** the expired time obtained from the backend is in seconds, default value is 299 seconds
      const expired = (authStore.expiredIn ?? Number(localExpiredIn)) * (2 / 3) * 1000
      const defaultExpired = 299 * (2 / 3) * 1000

      let intervalId = null

      const [authConfigsErr, resAuthConfigs] = await to(dispatch(getAuthConfigs()))
      const { authType, oauthUrl } = resAuthConfigs.payload

      if (authType === 'simple') {
        dispatch(initialVersion())
        router.replace('/')
      } else {
        if (token) {
          intervalId = setInterval(() => {
            dispatch(refreshToken())
          }, expired || defaultExpired)

          dispatch(setAuthToken(token))
          dispatch(initialVersion())
        } else {
          router.replace('/login')
        }
      }

      return () => {
        clearInterval(intervalId)
      }
    }

    initAuth()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const values = {
    version,
    token,
    loading,
    login: handleLogin,
    logout: handleLogout
  }

  return <AuthContext.Provider value={values}>{children}</AuthContext.Provider>
}

export default AuthProvider
