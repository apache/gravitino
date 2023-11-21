/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import CssBaseline from '@mui/material/CssBaseline'
import GlobalStyles from '@mui/material/GlobalStyles'
import { ThemeProvider, createTheme, responsiveFontSizes } from '@mui/material/styles'

import themeOptions from './ThemeOptions'

import GlobalStyling from './globalStyles'

const ThemeComponent = props => {
  const { settings, children } = props

  let theme = responsiveFontSizes(createTheme(themeOptions(settings, 'light')))

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <GlobalStyles styles={() => GlobalStyling(theme)} />
      {children}
    </ThemeProvider>
  )
}

export default ThemeComponent
