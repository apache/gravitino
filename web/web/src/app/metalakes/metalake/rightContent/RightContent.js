/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import { Box, Button, IconButton } from '@mui/material'
import Icon from '@/components/Icon'
import MetalakePath from './MetalakePath'
import CreateCatalogDialog from './CreateCatalogDialog'
import TabsContent from './tabsContent/TabsContent'
import { useSearchParams } from 'next/navigation'

const RightContent = () => {
  const [open, setOpen] = useState(false)
  const searchParams = useSearchParams()
  const [isShowBtn, setBtnVisiable] = useState(true)

  const handleCreateCatalog = () => {
    setOpen(true)
  }

  useEffect(() => {
    const paramsSize = [...searchParams.keys()].length
    const isMetalakePage = paramsSize == 1 && searchParams.get('metalake')
    setBtnVisiable(isMetalakePage)
  }, [searchParams])

  return (
    <Box className={`twc-w-0 twc-grow twc-h-full twc-bg-customs-white twc-overflow-hidden`}>
      <Box
        className={`twc-py-3 twc-px-5 twc-flex twc-items-center twc-justify-between`}
        sx={{
          borderBottom: theme => `1px solid ${theme.palette.divider}`
        }}
      >
        <Box className={`twc-flex twc-items-center twc-flex-1 twc-overflow-hidden twc-mr-2`}>
          <Box className={`twc-flex twc-items-center twc-justify-between twc-w-full`}>
            <Box className={`twc-flex twc-items-center twc-w-full`}>
              <IconButton color='primary' component={Link} href='/metalakes' sx={{ mr: 2 }} data-refer='back-home-btn'>
                <Icon icon='mdi:arrow-left' />
              </IconButton>
              <MetalakePath />
            </Box>
          </Box>
        </Box>

        {isShowBtn && (
          <Box className={`twc-flex twc-items-center`}>
            <Button
              variant='contained'
              startIcon={<Icon icon='mdi:plus-box' />}
              onClick={handleCreateCatalog}
              sx={{ width: 200 }}
              data-refer='create-catalog-btn'
            >
              Create Catalog
            </Button>
            <CreateCatalogDialog open={open} setOpen={setOpen} />
          </Box>
        )}
      </Box>

      <Box sx={{ height: 'calc(100% - 4.1rem)' }}>
        <TabsContent />
      </Box>
    </Box>
  )
}

export default RightContent
