/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { createContext, useEffect, useState, useContext } from 'react'

import { useRouter } from 'next/navigation'

import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import { initialVersion, setVersion as setStoreVersion } from '@/lib/store/sys'

import { to } from '../utils'
import { getAuthConfigs, setAuthToken, setIntervalId, loginAction } from '../store/auth'

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

  const token = (typeof window !== 'undefined' && localStorage.getItem('accessToken')) || null
  const version = (typeof window !== 'undefined' && localStorage.getItem('version')) || null

  const authStore = useAppSelector(state => state.auth)
  const dispatch = useAppDispatch()

  const handleLogin = async params => {
    dispatch(loginAction({ params, router }))
  }

  const handleLogout = () => {
    localStorage.removeItem('version')
    localStorage.removeItem('accessToken')
    localStorage.removeItem('authParams')

    dispatch(setStoreVersion(''))
    dispatch(setAuthToken(''))
    router.push('/ui/login')
  }

  useEffect(() => {
    const initAuth = async () => {
      const [authConfigsErr, resAuthConfigs] = await to(dispatch(getAuthConfigs()))
      const { authType } = resAuthConfigs.payload

      if (authType === 'simple') {
        dispatch(initialVersion())
      } else if (authType === 'oauth') {
        if (token) {
          dispatch(setIntervalId())
          dispatch(setAuthToken(token))
          dispatch(initialVersion())
        } else {
          router.push('/ui/login')
        }
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
