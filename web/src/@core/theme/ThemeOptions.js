/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import { deepmerge } from '@mui/utils'

import palette from './palette'
import spacing from './spacing'
import shadows from './shadows'
import overrides from './overrides'
import typography from './typography'
import breakpoints from './breakpoints'

const themeOptions = (settings, overrideMode) => {
  const { mode, direction, themeColor } = settings

  const mergedThemeConfig = deepmerge({
    breakpoints: breakpoints(),
    direction,
    components: overrides(settings),
    palette: palette(mode === 'semi-dark' ? overrideMode : mode, settings),
    ...spacing,
    shape: {
      borderRadius: 6
    },
    mixins: {
      toolbar: {
        minHeight: 64
      }
    },
    shadows: shadows(mode === 'semi-dark' ? overrideMode : mode),
    typography
  })

  return deepmerge(mergedThemeConfig, {
    palette: {
      primary: {
        ...(mergedThemeConfig.palette
          ? mergedThemeConfig.palette[themeColor]
          : palette(mode === 'semi-dark' ? overrideMode : mode, settings).primary)
      }
    }
  })
}

export default themeOptions
