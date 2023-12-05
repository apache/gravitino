/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import Link from 'next/link'
import { useRouter } from 'next/navigation'

import MUILink from '@mui/material/Link'
import Breadcrumbs from '@mui/material/Breadcrumbs'

import Icon from '@/components/Icon'

const MetalakePath = props => {
  const { routeParams } = props

  const { metalake, catalog, schema, table } = routeParams

  const router = useRouter()

  const metalakeUrl = `?metalake=${metalake}`
  const catalogUrl = `?metalake=${metalake}&catalog=${catalog}`
  const schemaUrl = `?metalake=${metalake}&catalog=${catalog}&schema=${schema}`
  const tableUrl = `?metalake=${metalake}&catalog=${catalog}&schema=${schema}&table=${table}`

  const handleClick = (event, path) => {
    router.asPath === path && event.preventDefault()
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
        <MUILink
          component={Link}
          href={metalakeUrl}
          onClick={event => handleClick(event, metalakeUrl)}
          underline='hover'
        >
          {metalake}
        </MUILink>
      )}
      {catalog && (
        <MUILink component={Link} href={catalogUrl} onClick={event => handleClick(event, catalogUrl)} underline='hover'>
          <Icon icon='bx:book' fontSize={20} />
          {catalog}
        </MUILink>
      )}
      {schema && (
        <MUILink component={Link} href={schemaUrl} onClick={event => handleClick(event, schemaUrl)} underline='hover'>
          <Icon icon='bx:coin-stack' fontSize={20} />
          {schema}
        </MUILink>
      )}
      {table && (
        <MUILink component={Link} href={tableUrl} onClick={event => handleClick(event, tableUrl)} underline='hover'>
          <Icon icon='bx:table' fontSize={20} />
          {table}
        </MUILink>
      )}
    </Breadcrumbs>
  )
}

export default MetalakePath
