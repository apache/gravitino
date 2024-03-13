/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { useEffect, createContext, useContext } from 'react'

import { StyledEngineProvider, ThemeProvider as MuiThemeProvider, CssBaseline } from '@mui/material'

import { useLocalStorage } from 'react-use'

import { settings as settingsConfig } from '@/src/lib/settings'
import createMuiTheme from '@/src/lib/theme/mui'

const ThemeContext = createContext()

export const useTheme = () => {
  return useContext(ThemeContext)
}

const MuiProvider = props => {
  const { children, mode } = props

  return (
    <StyledEngineProvider injectFirst>
      <MuiThemeProvider theme={createMuiTheme({ mode })}>
        <CssBaseline />
        {children}
      </MuiThemeProvider>
    </StyledEngineProvider>
  )
}

const ThemeProvider = props => {
  const { children } = props

  const [mode, setMode] = useLocalStorage('theme', settingsConfig.mode, { raw: true })

  useEffect(() => {
    if (mode) {
      setMode(mode)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const toggleTheme = () => {
    const themeMode = mode === 'light' ? 'dark' : 'light'
    setMode(themeMode)
  }

  return (
    <MuiProvider mode={mode}>
      <ThemeContext.Provider value={{ mode, toggleTheme }}>{children}</ThemeContext.Provider>
    </MuiProvider>
  )
}

export default ThemeProvider
