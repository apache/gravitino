/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import Link from 'next/link'
import Image from 'next/image'

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

import VersionView from './VersionView'
import LogoutButton from './Logout'
import { useSearchParams } from 'next/navigation'
import { useRouter } from 'next/navigation'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { fetchMetalakes } from '@/lib/store/metalakes'

import { useState, useEffect } from 'react'

const AppBar = () => {
  const searchParams = useSearchParams()
  const metalake = searchParams.get('metalake')
  const store = useAppSelector(state => state.metalakes)
  const dispatch = useAppDispatch()
  const [metalakes, setMetalakes] = useState([])
  const router = useRouter()

  useEffect(() => {
    if (!store.metalakes.length) {
      dispatch(fetchMetalakes())
    }
    const metalakeItems = store.metalakes.map(i => i.name)
    setMetalakes(metalakeItems)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [store.metalakes])

  return (
    <MuiAppBar
      elevation={6}
      position={'sticky'}
      className={
        'layout-navbar-container twc-px-6 twc-items-center twc-justify-center twc-transition-[border-bottom] twc-ease-in-out twc-duration-200 twc-bg-customs-white'
      }
    >
      <Box className={'layout-navbar twc-w-full'}>
        <Toolbar className={'navbar-content-container twc-mx-auto [@media(min-width:1440px)]:twc-max-w-[1440px]'}>
          <Box className={'app-bar-content twc-w-full twc-flex twc-items-center twc-justify-between'}>
            <Link href='/metalakes' className={'twc-flex twc-items-center twc-no-underline twc-mr-8'}>
              <Image
                src={process.env.NEXT_PUBLIC_BASE_PATH + '/icons/gravitino.svg'}
                width={32}
                height={32}
                alt='logo'
              />
              <Typography
                variant='h5'
                className={
                  'logoText twc-ml-2 twc-leading-none twc-font-bold twc-tracking-[-0.45px] twc-normal-case twc-text-[1.75rem]'
                }
              >
                Gravitino
              </Typography>
            </Link>
            <Box className={'app-bar-content-right twc-flex twc-items-center'}>
              <Stack direction='row' spacing={2} alignItems='center'>
                {metalake ? (
                  <FormControl fullWidth size='small'>
                    <InputLabel id='select-metalake'>Metalake</InputLabel>
                    <Select
                      labelId='select-metalake'
                      id='select-metalake'
                      data-refer='select-metalake'
                      value={metalake}
                      label='Metalake'
                    >
                      {metalakes.map(item => {
                        return (
                          <MenuItem
                            value={item}
                            key={item}
                            data-refer={'select-option-' + item}
                            onClick={() => router.push('/metalakes?metalake=' + item)}
                          >
                            {item}
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
