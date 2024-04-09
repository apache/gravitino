/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import Link from 'next/link'
import { useSearchParams } from 'next/navigation'

import { Link as MUILink, Breadcrumbs, Typography, Tooltip, styled } from '@mui/material'

import Icon from '@/components/Icon'

const Text = styled(Typography)(({ theme }) => ({
  maxWidth: '120px',
  overflow: 'hidden',
  textOverflow: 'ellipsis'
}))

const MetalakePath = props => {
  const searchParams = useSearchParams()

  const routeParams = {
    metalake: searchParams.get('metalake'),
    catalog: searchParams.get('catalog'),
    type: searchParams.get('type'),
    schema: searchParams.get('schema'),
    table: searchParams.get('table'),
    fileset: searchParams.get('fileset')
  }

  const { metalake, catalog, type, schema, table, fileset } = routeParams

  const metalakeUrl = `?metalake=${metalake}`
  const catalogUrl = `?metalake=${metalake}&catalog=${catalog}&type=${type}`
  const schemaUrl = `?metalake=${metalake}&catalog=${catalog}&type=${type}&schema=${schema}`
  const tableUrl = `?metalake=${metalake}&catalog=${catalog}&type=${type}&schema=${schema}&table=${table}`
  const filesetUrl = `?metalake=${metalake}&catalog=${catalog}&type=${type}&schema=${schema}&fileset=${fileset}`

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
            <Text data-refer={`nav-to-catalog-${catalog}`}>{catalog}</Text>
          </MUILink>
        </Tooltip>
      )}
      {schema && (
        <Tooltip title={schema} placement='top'>
          <MUILink component={Link} href={schemaUrl} onClick={event => handleClick(event, schemaUrl)} underline='hover'>
            <Icon icon='bx:coin-stack' fontSize={20} />
            <Text data-refer={`nav-to-schema-${schema}`}>{schema}</Text>
          </MUILink>
        </Tooltip>
      )}
      {table && (
        <Tooltip title={table} placement='top'>
          <MUILink component={Link} href={tableUrl} onClick={event => handleClick(event, tableUrl)} underline='hover'>
            <Icon icon='bx:table' fontSize={20} />
            <Text data-refer={`nav-to-table-${table}`}>{table}</Text>
          </MUILink>
        </Tooltip>
      )}
      {fileset && (
        <Tooltip title={fileset} placement='top'>
          <MUILink
            component={Link}
            href={filesetUrl}
            onClick={event => handleClick(event, filesetUrl)}
            underline='hover'
          >
            <Icon icon='bx:file' fontSize={20} />
            <Text>{fileset}</Text>
          </MUILink>
        </Tooltip>
      )}
    </Breadcrumbs>
  )
}

export default MetalakePath
