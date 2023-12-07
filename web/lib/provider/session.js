/*
 * Copyright 2023 Datastrato.
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

import { isProdEnv, to } from '../utils'
import { getAuthConfigs } from '../store/auth'

const devOauthUrl = process.env.NEXT_PUBLIC_OAUTH_PATH

const authProvider = {
  version: null,
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
  const [token, setToken] = useLocalStorage('accessToken', null, { raw: true })
  const [version, setVersion] = useLocalStorage('version', authProvider.version, { raw: false })
  const authStore = useAppSelector(state => state.auth)
  const dispatch = useAppDispatch()

  const handleLogin = async params => {
    let oauthUrl = authStore.oauthUrl

    try {
      const response = await loginApi(isProdEnv ? oauthUrl : devOauthUrl, params)

      const { access_token } = response.data

      if (access_token) {
        setToken(access_token)

        getVersionApi()
          .then(async res => {
            const { version } = res.data
            console.log(
              `Gravitino Version: %c${version.version}`,
              `color: white; background-color: #6062E0; padding: 2px; border-radius: 4px;`
            )
            setVersion(version)
            dispatch(setStoreVersion(version.version))
            router.replace('/')
          })
          .catch(error => {
            console.error(error)
          })
      }
    } catch (e) {
      throw new Error(e)
    }
  }

  const handleLogout = () => {
    setVersion(null)
    setToken(null)
    router.push('/login')
  }

  useEffect(() => {
    const initAuth = async () => {
      const [authConfigsErr, resAuthConfigs] = await to(dispatch(getAuthConfigs()))
      const { authType, oauthUrl } = resAuthConfigs.payload

      if (authType !== 'simple') {
        if (token) {
          getVersionApi()
            .then(res => {
              setLoading(false)
              const { version } = res.data
              console.log(
                `Gravitino Version: %c${version.version}`,
                `color: white; background-color: #6062E0; padding: 2px; border-radius: 4px;`
              )
              setVersion(version)
              dispatch(setStoreVersion(version.version))
            })
            .catch(() => {
              localStorage.removeItem('version')
              localStorage.removeItem('accessToken')
              setLoading(false)

              router.replace('/login')
            })
        } else {
          setLoading(false)
          router.replace('/login')
        }
      } else {
        dispatch(initialVersion())
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
