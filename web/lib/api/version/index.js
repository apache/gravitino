/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import defHttp from '@/lib/api'

const Apis = {
  GET: '/api/version'
}

export const getVersionApi = () => {
  return defHttp.request({
    method: 'get',
    url: `${Apis.GET}`
  })
}
