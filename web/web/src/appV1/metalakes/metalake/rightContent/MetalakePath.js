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
import { useSearchParams } from 'next/navigation'

import { Link as MUILink, Breadcrumbs, Typography, Tooltip, styled, Box, IconButton } from '@mui/material'

import Icon from '@/components/Icon'

const TextWrapper = styled(Typography)(({ theme }) => ({
  maxWidth: '120px',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap'
}))

const Text = props => {
  return <TextWrapper component='span' {...props} />
}

const MetalakePath = props => {
  const searchParams = useSearchParams()

  const routeParams = {
    metalake: searchParams.get('metalake'),
    catalog: searchParams.get('catalog'),
    type: searchParams.get('type'),
    schema: searchParams.get('schema'),
    table: searchParams.get('table'),
    fileset: searchParams.get('fileset'),
    topic: searchParams.get('topic'),
    model: searchParams.get('model'),
    version: searchParams.get('version')
  }

  const { metalake, catalog, type, schema, table, fileset, topic, model, version } = routeParams

  const metalakeUrl = `?metalake=${metalake}`
  const catalogUrl = `?metalake=${metalake}&catalog=${catalog}&type=${type}`
  const schemaUrl = `?metalake=${metalake}&catalog=${catalog}&type=${type}&schema=${schema}`
  const tableUrl = `?metalake=${metalake}&catalog=${catalog}&type=${type}&schema=${schema}&table=${table}`
  const filesetUrl = `?metalake=${metalake}&catalog=${catalog}&type=${type}&schema=${schema}&fileset=${fileset}`
  const topicUrl = `?metalake=${metalake}&catalog=${catalog}&type=${type}&schema=${schema}&topic=${topic}`
  const modelUrl = `?metalake=${metalake}&catalog=${catalog}&type=${type}&schema=${schema}&model=${model}`
  const versionUrl = `?metalake=${metalake}&catalog=${catalog}&type=${type}&schema=${schema}&model=${model}&version=${version}`

  const handleClick = (event, path) => {
    path === `?${searchParams.toString()}` && event.preventDefault()
  }

  const identity = [metalake, catalog, schema, table ?? fileset ?? topic ?? model].filter(Boolean).join('.')

  const handleCopy = async () => {
    if (identity) {
      if (navigator.clipboard && navigator.clipboard.writeText) await navigator.clipboard.writeText(identity)
      else console.warn('Clipboard API not available')
    }
  }

  return (
    <Box width='calc(100% - 48px)' display='flex' alignItems='center' gap={1}>
      <Breadcrumbs
        sx={{
          overflow: 'hidden',
          mt: 0,
          '& a': { display: 'flex', alignItems: 'center' },
          '& ol': {
            flexWrap: 'nowrap'
          },
          '& ol > li.MuiBreadcrumbs-li': {
            overflow: 'hidden',
            display: 'inline-flex',
            '& > a': {
              width: '100%',
              '& > svg': {
                minWidth: 20
              }
            }
          },
          '& ol > li:last-of-type': {
            color: theme => `${theme.palette.text.primary} !important`,
            overflow: 'hidden'
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
            <MUILink
              component={Link}
              href={schemaUrl}
              onClick={event => handleClick(event, schemaUrl)}
              underline='hover'
            >
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
        {topic && (
          <Tooltip title={topic} placement='top'>
            <MUILink component={Link} href={topicUrl} onClick={event => handleClick(event, topicUrl)} underline='hover'>
              <Icon icon='bx:file' fontSize={20} />
              <Text>{topic}</Text>
            </MUILink>
          </Tooltip>
        )}
        {model && (
          <Tooltip title={model} placement='top'>
            <MUILink component={Link} href={modelUrl} onClick={event => handleClick(event, modelUrl)} underline='hover'>
              <Icon icon='bx:file' fontSize={20} />
              <Text>{model}</Text>
            </MUILink>
          </Tooltip>
        )}
        {version && (
          <Tooltip title={version} placement='top'>
            <MUILink
              component={Link}
              href={versionUrl}
              onClick={event => handleClick(event, versionUrl)}
              underline='hover'
            >
              <Icon icon='bx:file' fontSize={20} />
              <Text>{version}</Text>
            </MUILink>
          </Tooltip>
        )}
      </Breadcrumbs>
      <Tooltip title='Copy identity string'>
        <IconButton size='small' onClick={handleCopy}>
          <Icon icon='bx:copy' fontSize={20} />
        </IconButton>
      </Tooltip>
    </Box>
  )
}

export default MetalakePath
