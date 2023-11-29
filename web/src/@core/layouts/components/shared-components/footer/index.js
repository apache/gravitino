/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import Box from '@mui/material/Box'

import FooterContent from './FooterContent'

const Footer = props => {
  const { settings, footerStyles, footerContent: userFooterContent } = props

  const { skin, layout, footer, contentWidth } = settings
  if (footer === 'hidden') {
    return null
  }

  return (
    <Box
      component='footer'
      className='layout-footer'
      sx={{
        zIndex: 10,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        ...(footer === 'fixed' && {
          bottom: 0,
          position: 'sticky',
          ...(layout === 'vertical'
            ? { px: [4, 6] }
            : {
                backgroundColor: theme => theme.palette.background.paper,
                ...(skin === 'bordered'
                  ? { borderTop: theme => `1px solid ${theme.palette.divider}` }
                  : { boxShadow: 6 })
              })
        }),
        ...footerStyles
      }}
    >
      <Box
        className='footer-content-container'
        sx={{
          px: 6,
          width: '100%',
          py: theme => theme.spacing(footer === 'fixed' && skin === 'bordered' ? 2.875 : 3),
          ...(contentWidth === 'boxed' && { '@media (min-width:1440px)': { maxWidth: 1440 } }),
          ...(layout === 'vertical' && {
            borderTopLeftRadius: 8,
            borderTopRightRadius: 8,
            ...(footer === 'fixed' && { backgroundColor: theme => theme.palette.background.paper })
          }),
          ...(footer === 'fixed' && {
            ...(contentWidth === 'boxed' &&
              layout === 'vertical' && {
                '@media (min-width:1440px)': { maxWidth: theme => `calc(1440px - ${theme.spacing(6)} * 2)` }
              }),
            ...(layout === 'vertical' && {
              ...(skin === 'bordered'
                ? { border: theme => `1px solid ${theme.palette.divider}`, borderBottomWidth: 0 }
                : { boxShadow: 6 })
            })
          })
        }}
      >
        {userFooterContent ? userFooterContent(props) : <FooterContent />}
      </Box>
    </Box>
  )
}

export default Footer
