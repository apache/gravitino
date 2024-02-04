/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { useState } from 'react'

import Box from '@mui/material/Box'
import Tab from '@mui/material/Tab'
import TabContext from '@mui/lab/TabContext'
import TabList from '@mui/lab/TabList'
import TabPanel from '@mui/lab/TabPanel'
import Typography from '@mui/material/Typography'

import Icon from '@/components/Icon'

import TableView from './TableView'
import DetailsView from './DetailsView'

const CustomTab = props => {
  const { icon, label, value, ...others } = props

  return (
    <Tab
      label={
        <Box sx={{ display: 'flex', alignItems: 'center' }}>
          <Icon icon={icon} />
          <Typography className={`twc-font-bold twc-ml-1 twc-normal-case`} color={'inherit'}>
            {label}
          </Typography>
        </Box>
      }
      value={value}
      {...others}
    />
  )
}

const CustomTabPanel = props => {
  const { value, children, ...others } = props

  return (
    <TabPanel value={value} sx={{ boxShadow: 'none', p: 0, height: 'calc(100% - 4rem)' }} {...others}>
      {children}
    </TabPanel>
  )
}

const TabsContent = props => {
  const { tableTitle, store, page, routeParams } = props
  const [tab, setTab] = useState('table')

  const handleChangeTab = (event, newValue) => {
    setTab(newValue)
  }

  return (
    <TabContext value={tab}>
      <Box sx={{ px: 6, py: 2, borderBottom: 1, borderColor: 'divider' }}>
        <TabList onChange={handleChangeTab} aria-label='tabs'>
          <CustomTab icon='mdi:list-box-outline' label={tableTitle} value='table' />
          <CustomTab icon='mdi:clipboard-text-outline' label='Details' value='details' />
        </TabList>
      </Box>
      <CustomTabPanel value='table'>
        <TableView page={page} routeParams={routeParams} />
      </CustomTabPanel>
      <CustomTabPanel value='details'>
        <DetailsView store={store} page={page} />
      </CustomTabPanel>
    </TabContext>
  )
}

export default TabsContent
