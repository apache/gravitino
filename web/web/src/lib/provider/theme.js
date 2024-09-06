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

'use client'

import { useEffect, createContext, useContext } from 'react'

import { StyledEngineProvider, ThemeProvider as MuiThemeProvider, CssBaseline } from '@mui/material'

import { useLocalStorage } from 'react-use'

import { settings as settingsConfig } from '@/lib/settings'
import createMuiTheme from '@/lib/theme/mui'

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
