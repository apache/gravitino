/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { createContext, useEffect, useState, useContext } from 'react'

import { useRouter } from 'next/navigation'

import { useAppDispatch } from '@/lib/hooks/useStore'
import { initialVersion } from '@/lib/store/sys'

import { to } from '../utils'
import { getAuthConfigs, setAuthToken } from '../store/auth'

const authProvider = {
  version: '',
  loading: true,
  setLoading: () => Boolean
}

const AuthContext = createContext(authProvider)

export const useAuth = () => useContext(AuthContext)

const AuthProvider = ({ children }) => {
  const router = useRouter()
  const [loading, setLoading] = useState(authProvider.loading)

  const token = (typeof window !== 'undefined' && localStorage.getItem('accessToken')) || null
  const version = (typeof window !== 'undefined' && localStorage.getItem('version')) || null

  const dispatch = useAppDispatch()

  useEffect(() => {
    const initAuth = async () => {
      const [authConfigsErr, resAuthConfigs] = await to(dispatch(getAuthConfigs()))
      const { authType } = resAuthConfigs.payload

      if (authType === 'simple') {
        dispatch(initialVersion())
      } else if (authType === 'oauth') {
        if (token) {
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
    loading
  }

  return <AuthContext.Provider value={values}>{children}</AuthContext.Provider>
}

export default AuthProvider
