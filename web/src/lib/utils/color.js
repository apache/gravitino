/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import chroma from 'chroma-js'

export const lighten = (color, value = null) => {
  return chroma(color).brighten(value)
}

export const darken = (color, value = null) => {
  return chroma(color).darken(value)
}

export const alpha = (color, value) => {
  return chroma(color).alpha(value).css()
}
