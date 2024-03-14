/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { defHttp } from '@/lib/utils/axios'

export const getAuthConfigsApi = () => {
  return defHttp.get({
    url: '/configs'
  })
}

export const loginApi = (url, params) => {
  return defHttp.post(
    {
      url,
      params
    },
    { withToken: false }
  )
}
