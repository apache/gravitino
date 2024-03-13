/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { useState } from 'react'

import Link from 'next/link'

import { Box, Button, IconButton } from '@mui/material'

import Icon from '@/components/Icon'

import { useAppSelector } from '@/lib/hooks/useStore'

import TabsContent from './TabsContent'
import MetalakePath from './MetalakePath'
import CreateCatalogDialog from './CreateCatalogDialog'

const RightContent = props => {
  const { tableTitle, routeParams, page } = props

  const store = useAppSelector(state => state.metalakes)

  const [open, setOpen] = useState(false)

  const handleCreateCatalog = () => {
    setOpen(true)
  }

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
              <IconButton color='primary' component={Link} href='/' sx={{ mr: 2 }} data-refer='back-home-btn'>
                <Icon icon='mdi:arrow-left' />
              </IconButton>
              <MetalakePath routeParams={routeParams} />
            </Box>
          </Box>
        </Box>

        {page === 'metalakes' && (
          <Box className={`twc-flex twc-items-center`}>
            <Button
              variant='contained'
              startIcon={<Icon icon='mdi:plus-box' />}
              onClick={handleCreateCatalog}
              data-refer='create-catalog-btn'
            >
              Create Catalog
            </Button>
            <CreateCatalogDialog open={open} setOpen={setOpen} routeParams={routeParams} />
          </Box>
        )}
      </Box>

      <Box sx={{ height: 'calc(100% - 4.1rem)' }}>
        <TabsContent tableTitle={tableTitle} store={store} page={page} routeParams={routeParams} />
      </Box>
    </Box>
  )
}

export default RightContent
