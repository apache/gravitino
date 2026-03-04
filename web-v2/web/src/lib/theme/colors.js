/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
