/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import dayjs from 'dayjs'

const DATE_TIME_FORMAT = 'YYYY-MM-DD HH:mm:ss'

export const formatToDateTime = (date, format = DATE_TIME_FORMAT) => {
  return dayjs(date).format(format)
}

export const isValidDate = value => {
  return dayjs(value).isValid()
}
