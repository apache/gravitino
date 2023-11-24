/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import axios from 'axios'

import { VersionApiEnum } from 'src/@core/enums/apiEnum'

const ApiEnum = {
  ...VersionApiEnum
}

export const getVersionApi = () => {
  return axios({
    url: `${ApiEnum.GET}`,
    method: 'get',
    headers: {
      Accept: 'application/vnd.gravitino.v1+json'
    }
  })
}
