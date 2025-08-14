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

import { useState, useEffect } from 'react'
import { useSearchParams } from 'next/navigation'
import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import { getFilesetFiles } from '@/lib/store/metalakes'

import {
  Box,
  Typography,
  Breadcrumbs,
  Link,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  IconButton,
  Tooltip,
  Chip
} from '@mui/material'
import Icon from '@/components/Icon'

const FilesetView = () => {
  const dispatch = useAppDispatch()
  const searchParams = useSearchParams()
  const store = useAppSelector(state => state.metalakes)

  const metalake = searchParams.get('metalake')
  const catalog = searchParams.get('catalog')
  const schema = searchParams.get('schema')
  const fileset = searchParams.get('fileset')

  const [currentPath, setCurrentPath] = useState('/')
  const [pathHistory, setPathHistory] = useState(['/'])

  useEffect(() => {
    if (metalake && catalog && schema && fileset) {
      dispatch(
        getFilesetFiles({
          metalake,
          catalog,
          schema,
          fileset,
          subPath: currentPath
        })
      )
    }
  }, [dispatch, metalake, catalog, schema, fileset, currentPath])

  const handlePathClick = path => {
    setCurrentPath(path)
    if (!pathHistory.includes(path)) {
      setPathHistory([...pathHistory, path])
    }
  }

  const handleFolderClick = file => {
    if (file.isDir) {
      const newPath = currentPath === '/' ? `/${file.name}` : `${currentPath}/${file.name}`
      handlePathClick(newPath)
    }
  }

  const handleBackClick = () => {
    if (pathHistory.length > 1) {
      const newHistory = pathHistory.slice(0, -1)
      const newPath = newHistory[newHistory.length - 1]
      setPathHistory(newHistory)
      setCurrentPath(newPath)
    }
  }

  const formatFileSize = size => {
    if (size === 0) return '0 B'
    const k = 1024
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
    const i = Math.floor(Math.log(size) / Math.log(k))

    return parseFloat((size / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
  }

  const formatDate = timestamp => {
    return new Date(timestamp).toLocaleString()
  }

  const renderBreadcrumbs = () => {
    const pathParts = currentPath.split('/').filter(part => part !== '')
    const breadcrumbItems = ['/']

    pathParts.forEach((part, index) => {
      const path = '/' + pathParts.slice(0, index + 1).join('/')
      breadcrumbItems.push(path)
    })

    return (
      <Breadcrumbs aria-label='file path'>
        {breadcrumbItems.map((path, index) => (
          <Link
            key={path}
            color='inherit'
            href='#'
            onClick={e => {
              e.preventDefault()
              handlePathClick(path)
            }}
            sx={{ cursor: 'pointer' }}
          >
            {index === 0 ? 'Root' : path.split('/').pop()}
          </Link>
        ))}
      </Breadcrumbs>
    )
  }

  return (
    <Box sx={{ p: 3, height: '100%', overflow: 'auto' }}>
      <Box sx={{ mb: 2, display: 'flex', gap: 1, alignItems: 'center' }}>
        <Tooltip title='Go Back'>
          <IconButton onClick={handleBackClick} disabled={pathHistory.length <= 1}>
            <Icon icon='mdi:arrow-left' />
          </IconButton>
        </Tooltip>
        {renderBreadcrumbs()}
      </Box>

      <TableContainer component={Paper}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell>Type</TableCell>
              <TableCell>Size</TableCell>
              <TableCell>Last Modified</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {store.filesetFiles && store.filesetFiles.length > 0 ? (
              store.filesetFiles.map((file, index) => (
                <TableRow
                  key={index}
                  hover
                  sx={{ cursor: file.isDir ? 'pointer' : 'default' }}
                  onClick={() => handleFolderClick(file)}
                >
                  <TableCell>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <Icon icon={file.isDir ? 'mdi:folder' : 'mdi:file'} color='#666' />
                      <Typography>{file.name}</Typography>
                    </Box>
                  </TableCell>
                  <TableCell>
                    <Chip
                      label={file.isDir ? 'Directory' : 'File'}
                      size='small'
                      color={file.isDir ? 'primary' : 'default'}
                    />
                  </TableCell>
                  <TableCell>{file.isDir ? '-' : formatFileSize(file.size || 0)}</TableCell>
                  <TableCell>{file.lastModified ? formatDate(file.lastModified) : '-'}</TableCell>
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell colSpan={4} align='center'>
                  <Typography variant='body2' color='text.secondary'>
                    No files found in this directory
                  </Typography>
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  )
}

export default FilesetView
