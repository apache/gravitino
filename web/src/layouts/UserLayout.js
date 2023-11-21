/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import useMediaQuery from '@mui/material/useMediaQuery'

import Layout from 'src/@core/layouts/Layout'

import HorizontalNavItems from 'src/navigation/horizontal'
import HorizontalAppBarContent from './components/horizontal/AppBarContent'

import { useSettings } from 'src/@core/hooks/useSettings'

const UserLayout = ({ children, contentHeightFixed }) => {
  const { settings, saveSettings } = useSettings()

  const hidden = useMediaQuery(theme => theme.breakpoints.down('lg'))
  if (hidden && settings.layout === 'horizontal') {
    settings.layout = 'vertical'
  }

  return (
    <Layout
      hidden={hidden}
      settings={settings}
      saveSettings={saveSettings}
      contentHeightFixed={contentHeightFixed}
      horizontalLayoutProps={{
        navMenu: {
          navItems: HorizontalNavItems()
        },
        appBar: {
          content: () => <HorizontalAppBarContent settings={settings} saveSettings={saveSettings} />
        }
      }}
    >
      {children}
    </Layout>
  )
}

export default UserLayout
