/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import Link from 'next/link'

import { Box, Button, IconButton } from '@mui/material'

import Icon from '@/components/Icon'

import { useAppSelector } from '@/lib/hooks/useStore'

import TabsContent from './TabsContent'
import MetalakePath from './MetalakePath'

const RightContent = props => {
  const { tableTitle, routeParams, page } = props

  const store = useAppSelector(state => state.metalakes)

  return (
    <Box className={`twc-w-0 twc-grow twc-h-full twc-bg-customs-white twc-overflow-hidden`}>
      <Box
        className={`twc-py-3 twc-px-5 twc-flex twc-items-center twc-justify-between`}
        sx={{
          borderBottom: theme => `1px solid ${theme.palette.divider}`
        }}
      >
        <Box className={`twc-flex twc-items-center`}>
          <Box className={`twc-flex twc-items-center twc-justify-between`}>
            <Box className={`twc-flex twc-items-center`}>
              <IconButton color='primary' component={Link} href='/' sx={{ mr: 2 }}>
                <Icon icon='mdi:arrow-left' />
              </IconButton>
              <MetalakePath routeParams={routeParams} />
            </Box>
          </Box>
        </Box>

        {page === 'metalake' && (
          <Box className={`twc-flex twc-items-center`}>
            <Button variant='contained' startIcon={<Icon icon='mdi:plus-box' />}>
              Create Catalog
            </Button>
          </Box>
        )}
      </Box>

      <Box sx={{ height: 'calc(100% - 4rem)' }}>
        <TabsContent tableTitle={tableTitle} store={store} />
      </Box>
    </Box>
  )
}

export default RightContent
