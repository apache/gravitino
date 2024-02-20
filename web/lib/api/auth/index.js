/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import axios from 'axios'
import { defHttp } from '@/lib/utils/axios'

export const getAuthConfigsApi = () => {
  return defHttp.get({
    url: '/configs'
  })
}

export const loginApi = (url, params) => {
  return axios({
    url,
    method: 'post',
    params
  })
}
