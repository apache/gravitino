import Box from '@mui/material/Box'

import themeConfig from 'src/configs/themeConfig'

import HorizontalNavItems from './HorizontalNavItems'

const Navigation = props => {
  return (
    <Box
      className='menu-content'
      sx={{
        display: 'flex',
        flexWrap: 'wrap',
        alignItems: 'center',
        '& > *': {
          '&:not(:last-child)': { mr: 0.5 },
          maxWidth: 200
        }
      }}
    >
      <HorizontalNavItems {...props} />
    </Box>
  )
}

export default Navigation
