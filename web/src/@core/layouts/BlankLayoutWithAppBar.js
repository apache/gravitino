import { styled } from '@mui/material/styles'
import Box from '@mui/material/Box'

import AppBar from 'src/@core/layouts/components/blank-layout-with-appBar'

const BlankLayoutWithAppBarWrapper = styled(Box)(({ theme }) => ({
  height: '100vh',

  '& .content-center': {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    padding: theme.spacing(5),
    minHeight: `calc(100vh - ${theme.spacing(theme.mixins.toolbar.minHeight / 4)})`
  },

  '& .content-right': {
    display: 'flex',
    overflowX: 'hidden',
    position: 'relative',
    minHeight: `calc(100vh - ${theme.spacing(theme.mixins.toolbar.minHeight / 4)})`
  }
}))

const BlankLayoutWithAppBar = props => {
  const { children } = props

  return (
    <BlankLayoutWithAppBarWrapper>
      <AppBar />
      <Box
        className='app-content'
        sx={{
          overflowX: 'hidden',
          position: 'relative',
          minHeight: theme => `calc(100vh - ${theme.spacing(theme.mixins.toolbar.minHeight / 4)})`
        }}
      >
        {children}
      </Box>
    </BlankLayoutWithAppBarWrapper>
  )
}

export default BlankLayoutWithAppBar
