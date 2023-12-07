/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import axios from 'axios'

import { useRouter } from 'next/navigation'

const defHttp = axios.create({
  baseURL: '/',
  headers: {
    Accept: 'application/vnd.gravitino.v1+json'
  }
})

defHttp.interceptors.request.use(config => {
  if (config.url.includes('/oauth2/token')) {
    return config
  }
  const accessToken = typeof window !== 'undefined' ? window.localStorage.getItem('accessToken') : null

  if (accessToken) {
    config.headers.Authorization = `Bearer ${accessToken}`
  }

  return config
})

defHttp.interceptors.response.use(
  res => {
    return res
  },
  err => {
    console.error(err)
    if (err.response.status === 401) {
      if (typeof window !== 'undefined') {
        window.localStorage.removeItem('accessToken')
        window.localStorage.removeItem('version')
        const router = useRouter()
        router.replace('/login')
      }
    }
  }
)

export default defHttp
