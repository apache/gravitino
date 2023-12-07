/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import axios from 'axios'

const defHttp = axios.create({
  baseURL: '/',
  headers: {
    Accept: 'application/vnd.gravitino.v1+json'
  }
})

defHttp.interceptors.request.use(config => {
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
  }
)

export default defHttp
