/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { Inconsolata } from 'next/font/google'

import { useState, useEffect } from 'react'

import { styled, Box, Divider, List, ListItem, ListItemText, Stack, Tab, Typography } from '@mui/material'
import Tooltip, { tooltipClasses } from '@mui/material/Tooltip'
import { TabContext, TabList, TabPanel } from '@mui/lab'

import { useAppSelector } from '@/lib/hooks/useStore'

import { useSearchParams } from 'next/navigation'
import TableView from './tableView/TableView'
import DetailsView from './detailsView/DetailsView'

import Icon from '@/components/Icon'

const fonts = Inconsolata({ subsets: ['latin'] })

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

const CustomTooltip = styled(({ className, ...props }) => <Tooltip {...props} classes={{ popper: className }} />)(
  ({ theme }) => ({
    [`& .${tooltipClasses.tooltip}`]: {
      backgroundColor: '#23282a',
      padding: 0,
      border: '1px solid #dadde9'
    }
  })
)

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
  const store = useAppSelector(state => state.metalakes)
  const searchParams = useSearchParams()
  const paramsSize = [...searchParams.keys()].length
  const type = searchParams.get('type')
  const [tab, setTab] = useState('table')
  const isNotNeedTableTab = type && ['fileset', 'messaging'].includes(type) && paramsSize === 5
  const isShowTableProps = paramsSize === 5 && !['fileset', 'messaging'].includes(type)

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
      switch (type) {
        case 'fileset':
          tableTitle = 'Filesets'
          break
        case 'messaging':
          tableTitle = 'Topics'
          break
        default:
          tableTitle = 'Tables'
      }
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
      <Box
        sx={{
          px: 6,
          py: 2,
          borderBottom: 1,
          borderColor: 'divider',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center'
        }}
      >
        <TabList onChange={handleChangeTab} aria-label='tabs' variant='scrollable' scrollButtons='auto'>
          {!isNotNeedTableTab ? (
            <CustomTab icon='mdi:list-box-outline' label={tableTitle} value='table' data-refer='tab-table' />
          ) : null}
          <CustomTab icon='mdi:clipboard-text-outline' label='Details' value='details' data-refer='tab-details' />
        </TabList>
        {isShowTableProps && (
          <Box>
            <List dense sx={{ p: 0 }}>
              <Stack spacing={0} direction={'row'} divider={<Divider orientation='vertical' flexItem />}>
                {store.tableProps
                  .filter(i => i.items.length !== 0)
                  .map((item, index) => {
                    return (
                      <CustomTooltip
                        key={item.type}
                        title={
                          <>
                            <Box
                              sx={{
                                backgroundColor: '#525c61',
                                p: 1.5,
                                px: 4,
                                borderTopLeftRadius: 4,
                                borderTopRightRadius: 4
                              }}
                            >
                              <Typography
                                color='white'
                                fontWeight={700}
                                fontSize={14}
                                sx={{ textTransform: 'capitalize' }}
                              >
                                {item.type}:{' '}
                              </Typography>
                            </Box>

                            <Box sx={{ p: 1.5, px: 4 }}>
                              {item.items.map(i => {
                                return (
                                  <Typography key={i} variant='caption' color='white' className={fonts.className}>
                                    {item.type === 'sortOrders' ? i.text : i.fields}
                                  </Typography>
                                )
                              })}
                            </Box>
                          </>
                        }
                      >
                        <ListItem sx={{ maxWidth: 140, py: 0 }}>
                          <ListItemText
                            sx={{ m: 0 }}
                            primary={
                              <Box
                                sx={{
                                  display: 'flex',
                                  alignItems: 'center',
                                  color: theme => theme.palette.text.primary
                                }}
                              >
                                <Typography
                                  component={'span'}
                                  textTransform={'capitalize'}
                                  fontWeight={700}
                                  fontSize={14}
                                  sx={{ display: 'inline-block', pr: 2 }}
                                >
                                  {item.type}
                                </Typography>{' '}
                                <Box sx={{ display: 'flex', alignItems: 'center' }}>
                                  <Icon icon={item.icon} />
                                </Box>
                              </Box>
                            }
                            secondary={
                              <Box
                                component={'span'}
                                sx={{
                                  display: 'inline-block',
                                  width: '100%',
                                  overflow: 'hidden',
                                  whiteSpace: 'nowrap',
                                  textOverflow: 'ellipsis'
                                }}
                              >
                                <Typography variant='caption' className={fonts.className}>
                                  {item.type === 'sortOrders'
                                    ? item.items.map(i => i.text)
                                    : item.items.map(i => i.fields)}
                                </Typography>
                              </Box>
                            }
                          />
                        </ListItem>
                      </CustomTooltip>
                    )
                  })}
              </Stack>
            </List>
          </Box>
        )}
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
