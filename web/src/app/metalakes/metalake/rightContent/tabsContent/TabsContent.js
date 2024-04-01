/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { useState, useEffect } from 'react'

import Box from '@mui/material/Box'
import Tab from '@mui/material/Tab'
import TabContext from '@mui/lab/TabContext'
import TabList from '@mui/lab/TabList'
import TabPanel from '@mui/lab/TabPanel'
import Typography from '@mui/material/Typography'
import { useSearchParams } from 'next/navigation'
import TableView from './tableView/TableView'
import DetailsView from './detailsView/DetailsView'

import Icon from '@/components/Icon'

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

const TabsContent = () => {
  let tableTitle = ''
  const searchParams = useSearchParams()
  const paramsSize = [...searchParams.keys()].length
  const type = searchParams.get('type')
  const [tab, setTab] = useState('table')
  const isNotNeedTableTab = type && type === 'fileset' && paramsSize === 5

  const handleChangeTab = (event, newValue) => {
    setTab(newValue)
  }

  switch (paramsSize) {
    case 1:
      tableTitle = 'Catalogs'
      break
    case 3:
      tableTitle = 'Schemas'
      break
    case 4:
      tableTitle = type === 'fileset' ? 'Filesets' : 'Tables'
      break
    case 5:
      tableTitle = 'Columns'
      break
    default:
      break
  }

  useEffect(() => {
    if (isNotNeedTableTab) {
      setTab('details')
    } else {
      setTab('table')
    }

    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams])

  return (
    <TabContext value={tab}>
      <Box sx={{ px: 6, py: 2, borderBottom: 1, borderColor: 'divider' }}>
        <TabList onChange={handleChangeTab} aria-label='tabs'>
          {!isNotNeedTableTab ? (
            <CustomTab icon='mdi:list-box-outline' label={tableTitle} value='table' data-refer='tab-table' />
          ) : null}
          <CustomTab icon='mdi:clipboard-text-outline' label='Details' value='details' data-refer='tab-details' />
        </TabList>
      </Box>
      {!isNotNeedTableTab ? (
        <CustomTabPanel value='table' data-refer='tab-table-panel'>
          <TableView />
        </CustomTabPanel>
      ) : null}

      <CustomTabPanel value='details' data-refer='tab-details-panel'>
        <DetailsView />
      </CustomTabPanel>
    </TabContext>
  )
}

export default TabsContent
