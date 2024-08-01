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

import Link from 'next/link'
import Image from 'next/image'
import { Roboto } from 'next/font/google'

import { useState, useEffect } from 'react'

import {
  Box,
  AppBar as MuiAppBar,
  Toolbar,
  Stack,
  Typography,
  FormControl,
  InputLabel,
  Select,
  MenuItem
} from '@mui/material'

import clsx from 'clsx'

import VersionView from './VersionView'
import LogoutButton from './Logout'
import { useSearchParams } from 'next/navigation'
import { useRouter } from 'next/navigation'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { fetchMetalakes } from '@/lib/store/metalakes'

const fonts = Roboto({ subsets: ['latin'], weight: ['400'], display: 'swap' })

const AppBar = () => {
  const searchParams = useSearchParams()
  const metalake = searchParams.get('metalake')
  const store = useAppSelector(state => state.metalakes)
  const dispatch = useAppDispatch()
  const [metalakes, setMetalakes] = useState([])
  const router = useRouter()
  const logoSrc = (process.env.NEXT_PUBLIC_BASE_PATH ?? '') + '/icons/gravitino.svg'

  useEffect(() => {
    if (!store.metalakes.length && metalake) {
      dispatch(fetchMetalakes())
    }
    const metalakeItems = store.metalakes.map(i => i.name)
    setMetalakes(metalakeItems)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [store.metalakes, metalake, dispatch])

  return (
    <MuiAppBar
      elevation={6}
      position={'sticky'}
      className={
        'layout-navbar-container twc-items-center twc-justify-center twc-transition-[border-bottom] twc-ease-in-out twc-duration-200 twc-bg-customs-white'
      }
    >
      <Box className={'layout-navbar twc-w-full'}>
        <Toolbar
          className={'navbar-content-container twc-p-0 twc-mx-auto [@media(min-width:1440px)]:twc-max-w-[1440px]'}
        >
          <Box
            className={
              'app-bar-content twc-w-full twc-px-[1.5rem] sm:twc-px-6 twc-flex twc-items-center twc-justify-between'
            }
          >
            <Link href='/metalakes' className={'twc-flex twc-items-center twc-no-underline twc-mr-8'}>
              <Image src={logoSrc} overrideSrc={logoSrc} width={32} height={32} alt='logo' />
              <Typography
                variant='h5'
                className={clsx(
                  'twc-text-[black] twc-ml-2 twc-leading-none twc-tracking-[-0.45px] twc-normal-case twc-text-[1.75rem]',
                  fonts.className
                )}
              >
                Gravitino
              </Typography>
            </Link>
            <Box className={'app-bar-content-right twc-flex twc-items-center'}>
              <Stack direction='row' spacing={2} alignItems='center'>
                {metalake ? (
                  <FormControl sx={{ maxWidth: 240 }} size='small'>
                    <InputLabel id='select-metalake'>Metalake</InputLabel>
                    <Select
                      labelId='select-metalake'
                      id='select-metalake'
                      data-refer='select-metalake'
                      value={metalake}
                      label='Metalake'
                      disabled={store.metalakes.length === 1}
                      sx={{ width: '100%' }}
                    >
                      {metalakes.map(item => {
                        return (
                          <MenuItem
                            title={item}
                            value={item}
                            key={item}
                            data-refer={'select-option-' + item}
                            sx={{
                              maxWidth: 240
                            }}
                            onClick={() => router.push('/metalakes?metalake=' + item)}
                          >
                            <Box
                              sx={{
                                overflow: 'hidden',
                                textOverflow: 'ellipsis'
                              }}
                            >
                              {item}
                            </Box>
                          </MenuItem>
                        )
                      })}
                    </Select>
                  </FormControl>
                ) : null}
                <VersionView />
                <LogoutButton />
              </Stack>
            </Box>
          </Box>
        </Toolbar>
      </Box>
    </MuiAppBar>
  )
}

export default AppBar
