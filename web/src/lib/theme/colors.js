/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { lighten, darken } from '../utils/color'
import settings from '../settings'

export const primaryColor = settings.primaryColor || '#6478f7'

const whiteColor = '#fff'
const blackColor = '#000'
const lightColor = '#32465a'
const darkColor = '#dbdbeb'
const lightBgColor = '#f5f5f8'
const darkBgColor = '#232323'

export const mainColor = settings.mode === 'light' ? lightColor : darkColor

const colors = {
  customs: {
    main: mainColor,
    white: whiteColor,
    black: blackColor,
    light: lightColor,
    dark: darkColor,
    lightBg: lightBgColor,
    darkBg: darkBgColor
  },
  primary: {
    main: primaryColor,
    light: lighten(primaryColor),
    dark: darken(primaryColor, 0.3)
  },
  secondary: {
    light: '#97A2B1',
    main: '#8592A3',
    dark: '#798594'
  },
  success: {
    light: '#86E255',
    main: '#71DD37',
    dark: '#67C932'
  },
  error: {
    light: '#FF5B3F',
    main: '#FF3E1D',
    dark: '#E8381A'
  },
  warning: {
    light: '#FFB826',
    main: '#FFAB00',
    dark: '#E89C00'
  },
  info: {
    light: '#29CCEF',
    main: '#03C3EC',
    dark: '#03B1D7'
  },
  grey: {}
}

export default colors
