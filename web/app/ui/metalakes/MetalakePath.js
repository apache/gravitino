/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import Link from 'next/link'
import { useRouter, useSearchParams } from 'next/navigation'

import { Link as MUILink, Breadcrumbs, Typography, Tooltip, styled } from '@mui/material'

import Icon from '@/components/Icon'

const Text = styled(Typography)(({ theme }) => ({
  maxWidth: '120px',
  overflow: 'hidden',
  textOverflow: 'ellipsis'
}))

const MetalakePath = props => {
  const { routeParams } = props

  const { metalake, catalog, schema, table } = routeParams

  const router = useRouter()
  const searchParams = useSearchParams()

  const metalakeUrl = `?metalake=${metalake}`
  const catalogUrl = `?metalake=${metalake}&catalog=${catalog}`
  const schemaUrl = `?metalake=${metalake}&catalog=${catalog}&schema=${schema}`
  const tableUrl = `?metalake=${metalake}&catalog=${catalog}&schema=${schema}&table=${table}`

  const handleClick = (event, path) => {
    path === `?${searchParams.toString()}` && event.preventDefault()
  }

  return (
    <Breadcrumbs
      sx={{
        mt: 0,
        '& a': { display: 'flex', alignItems: 'center' },
        '& ol > li:last-of-type': {
          color: theme => `${theme.palette.text.primary} !important`
        }
      }}
    >
      {metalake && (
        <Tooltip title={metalake} placement='top'>
          <MUILink
            component={Link}
            href={metalakeUrl}
            onClick={event => handleClick(event, metalakeUrl)}
            underline='hover'
            data-refer='metalake-name-link'
          >
            <Text>{metalake}</Text>
          </MUILink>
        </Tooltip>
      )}
      {catalog && (
        <Tooltip title={catalog} placement='top'>
          <MUILink
            component={Link}
            href={catalogUrl}
            onClick={event => handleClick(event, catalogUrl)}
            underline='hover'
          >
            <Icon icon='bx:book' fontSize={20} />
            <Text>{catalog}</Text>
          </MUILink>
        </Tooltip>
      )}
      {schema && (
        <Tooltip title={schema} placement='top'>
          <MUILink component={Link} href={schemaUrl} onClick={event => handleClick(event, schemaUrl)} underline='hover'>
            <Icon icon='bx:coin-stack' fontSize={20} />
            <Text>{schema}</Text>
          </MUILink>
        </Tooltip>
      )}
      {table && (
        <Tooltip title={table} placement='top'>
          <MUILink component={Link} href={tableUrl} onClick={event => handleClick(event, tableUrl)} underline='hover'>
            <Icon icon='bx:table' fontSize={20} />
            <Text>{table}</Text>
          </MUILink>
        </Tooltip>
      )}
    </Breadcrumbs>
  )
}

export default MetalakePath
