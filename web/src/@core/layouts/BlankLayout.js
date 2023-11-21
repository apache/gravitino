/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import { styled } from '@mui/material/styles'
import Box from '@mui/material/Box'

const BlankLayoutWrapper = styled(Box)(({ theme }) => ({
  height: '100vh',

  '& .content-center': {
    display: 'flex',
    minHeight: '100vh',
    alignItems: 'center',
    justifyContent: 'center',
    padding: theme.spacing(5)
  },

  '& .content-right': {
    display: 'flex',
    overflow: 'hidden',
    minHeight: '100vh',
    position: 'relative'
  }
}))

const BlankLayout = ({ children }) => {
  return (
    <BlankLayoutWrapper className='layout-wrapper'>
      <Box className='app-content' sx={{ overflow: 'hidden', minHeight: '100vh', position: 'relative' }}>
        {children}
      </Box>
    </BlankLayoutWrapper>
  )
}

export default BlankLayout
