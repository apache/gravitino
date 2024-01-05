/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { defHttp } from '@/lib/utils/axios'
import qs from 'qs'

export const getAuthConfigsApi = () => {
  return defHttp.get({
    url: '/configs'
  })
}

export const loginApi = (url, params) => {
  return defHttp.post({
    url: `${url}?${qs.stringify(params)}`
  })
}
