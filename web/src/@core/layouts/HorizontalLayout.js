/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import Fab from '@mui/material/Fab'
import AppBar from '@mui/material/AppBar'
import { styled } from '@mui/material/styles'
import Box from '@mui/material/Box'
import MuiToolbar from '@mui/material/Toolbar'

import Icon from 'src/@core/components/icon'

import Footer from './components/shared-components/footer'
import Navigation from './components/horizontal/navigation'
import ScrollToTop from 'src/@core/components/scroll-to-top'
import AppBarContent from './components/horizontal/app-bar-content'

import { hexToRGBA } from 'src/@core/utils/hex-to-rgba'

const HorizontalLayoutWrapper = styled('div')({
  height: '100%',
  display: 'flex',
  overflow: 'clip'
})

const MainContentWrapper = styled(Box)({
  flexGrow: 1,
  minWidth: 0,
  display: 'flex',
  minHeight: '100vh',
  flexDirection: 'column'
})

const Toolbar = styled(MuiToolbar)(({ theme }) => ({
  width: '100%',
  padding: `${theme.spacing(0, 6)} !important`,
  [theme.breakpoints.down('sm')]: {
    paddingLeft: theme.spacing(2),
    paddingRight: theme.spacing(4)
  },
  [theme.breakpoints.down('xs')]: {
    paddingLeft: theme.spacing(2),
    paddingRight: theme.spacing(2)
  }
}))

const ContentWrapper = styled('main')(({ theme }) => ({
  flexGrow: 1,
  width: '100%',
  padding: theme.spacing(6),
  transition: 'padding .25s ease-in-out',
  [theme.breakpoints.down('sm')]: {
    paddingLeft: theme.spacing(4),
    paddingRight: theme.spacing(4)
  }
}))

const HorizontalLayout = props => {
  const {
    hidden,
    children,
    settings,
    scrollToTop,
    footerProps,
    saveSettings,
    contentHeightFixed,
    horizontalLayoutProps
  } = props

  const appBarProps = horizontalLayoutProps?.appBar?.componentProps
  const userNavMenuContent = horizontalLayoutProps?.navMenu?.content
  const { skin, appBar, navHidden, contentWidth } = settings
  let userAppBarStyle = {}
  if (appBarProps && appBarProps.sx) {
    userAppBarStyle = appBarProps.sx
  }
  const userAppBarProps = Object.assign({}, appBarProps)
  delete userAppBarProps.sx

  return (
    <HorizontalLayoutWrapper className='layout-wrapper'>
      <MainContentWrapper className='layout-content-wrapper' sx={{ ...(contentHeightFixed && { maxHeight: '100vh' }) }}>
        <AppBar
          color='default'
          elevation={skin === 'bordered' ? 0 : 6}
          className='layout-navbar-and-nav-container'
          position={appBar === 'fixed' ? 'sticky' : 'static'}
          sx={{
            alignItems: 'center',
            color: 'text.primary',
            justifyContent: 'center',
            ...(appBar === 'static' && { zIndex: 13 }),
            transition: 'border-bottom 0.2s ease-in-out',
            ...(appBar === 'fixed' && { backdropFilter: 'saturate(200%) blur(6px)' }),
            backgroundColor: theme => hexToRGBA(theme.palette.background.paper, 0.95),
            ...(skin === 'bordered' && { borderBottom: theme => `1px solid ${theme.palette.divider}` }),
            ...userAppBarStyle
          }}
          {...userAppBarProps}
        >
          <Box
            className='layout-navbar'
            sx={{
              width: '100%',
              ...(navHidden ? {} : { borderBottom: theme => `1px solid ${theme.palette.divider}` })
            }}
          >
            <Toolbar
              className='navbar-content-container'
              sx={{
                mx: 'auto',
                ...(contentWidth === 'boxed' && { '@media (min-width:1440px)': { maxWidth: 1440 } }),
                minHeight: theme => `${theme.mixins.toolbar.minHeight - 1}px !important`
              }}
            >
              <AppBarContent
                {...props}
                hidden={hidden}
                settings={settings}
                saveSettings={saveSettings}
                appBarContent={horizontalLayoutProps?.appBar?.content}
                appBarBranding={horizontalLayoutProps?.appBar?.branding}
              />
            </Toolbar>
          </Box>

          {navHidden ? null : (
            <Box className='layout-horizontal-nav' sx={{ width: '100%', ...horizontalLayoutProps?.navMenu?.sx }}>
              <Toolbar
                className='horizontal-nav-content-container'
                sx={{
                  mx: 'auto',
                  ...(contentWidth === 'boxed' && { '@media (min-width:1440px)': { maxWidth: 1440 } }),
                  minHeight: theme =>
                    `${theme.mixins.toolbar.minHeight - 4 - (skin === 'bordered' ? 1 : 0)}px !important`
                }}
              >
                {(userNavMenuContent && userNavMenuContent(props)) || (
                  <Navigation {...props} horizontalNavItems={horizontalLayoutProps.navMenu?.navItems} />
                )}
              </Toolbar>
            </Box>
          )}
        </AppBar>

        <ContentWrapper
          className='layout-page-content'
          sx={{
            ...(contentHeightFixed && { display: 'flex', overflow: 'hidden' }),
            ...(contentWidth === 'boxed' && {
              mx: 'auto',
              '@media (min-width:1440px)': { maxWidth: 1440 },
              '@media (min-width:1200px)': { maxWidth: '100%' }
            })
          }}
        >
          {children}
        </ContentWrapper>

        <Footer {...props} footerStyles={footerProps?.sx} footerContent={footerProps?.content} />

        {scrollToTop ? (
          scrollToTop(props)
        ) : (
          <ScrollToTop className='mui-fixed'>
            <Fab color='primary' size='small' aria-label='scroll back to top'>
              <Icon icon='bx:up-arrow-alt' />
            </Fab>
          </ScrollToTop>
        )}
      </MainContentWrapper>
    </HorizontalLayoutWrapper>
  )
}

export default HorizontalLayout
