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

import { useEffect, useState, useMemo } from 'react'

import { ArrowLeftOutlined, CopyOutlined, FileOutlined, FolderOutlined } from '@ant-design/icons'
import { App, Button, Select, Space, Spin, Table, Typography } from 'antd'

// import { useListFilesByFileset } from '@/hooks'
import { copyToClipboard, formatFileSize, formatLastModified } from '@/lib/utils'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { useAntdColumnResize } from 'react-antd-column-resize'
import { getFilesetFiles } from '@/lib/store/metalakes'

const { Option } = Select
const { Text, Link } = Typography

const ListFiles = ({ metalake, catalog, schema, fileset, storageLocations, defaultLocationName }) => {
  const [currentLocation, setCurrentLocation] = useState(undefined)
  const [sub_path, setSubPath] = useState('')
  const [pathSegments, setPathSegments] = useState([])
  const { message } = App.useApp()
  const store = useAppSelector(state => state.metalakes)
  const dispatch = useAppDispatch()

  useEffect(() => {
    if (defaultLocationName) {
      setCurrentLocation(defaultLocationName)
    }
  }, [defaultLocationName])

  useEffect(() => {
    if (sub_path) {
      setPathSegments(sub_path.split('/').filter(p => p))
    } else {
      setPathSegments([])
    }
  }, [sub_path])

  useEffect(() => {
    console.log('sub_path changed:', sub_path)
    if (metalake && catalog && schema && fileset) {
      dispatch(
        getFilesetFiles({
          metalake,
          catalog,
          schema,
          fileset,
          subPath: sub_path,
          locationName: currentLocation
        })
      )
    }
  }, [dispatch, metalake, catalog, schema, fileset, sub_path, currentLocation])

  const handleLocationChange = value => {
    setCurrentLocation(value)
    setSubPath('')
  }

  const handlePathSegmentClick = index => {
    const newSubPath = pathSegments.slice(0, index + 1).join('/')
    setSubPath(newSubPath)
  }

  const handleDirectoryClick = dirName => {
    const newSubPath = sub_path ? `${sub_path}/${dirName}` : dirName
    setSubPath(newSubPath)
  }

  const handleBackClick = () => {
    if (pathSegments.length > 0) {
      const newPathSegments = pathSegments.slice(0, -1)
      setSubPath(newPathSegments.join('/'))
    }
  }

  const handleCopyPath = () => {
    if (currentLocation && storageLocations && storageLocations[currentLocation]) {
      const basePath = storageLocations[currentLocation].replace(/\/$/, '')
      const fullPath = sub_path ? `${basePath}/${sub_path}` : basePath
      copyToClipboard(fullPath)
        .then(() => {
          message.success('Path copied!')
        })
        .catch(err => {
          console.error('Failed to copy path: ', err)
          message.error('Failed to copy path')
        })
    }
  }

  const columns = useMemo(
    () => [
      {
        title: 'File Name',
        dataIndex: 'name',
        key: 'name',
        width: 250,
        render: (name, record) => (
          <Space>
            {record.isDir ? <FolderOutlined /> : <FileOutlined />}
            {record.isDir ? <Link onClick={() => handleDirectoryClick(name)}>{name}</Link> : <span>{name}</span>}
          </Space>
        )
      },
      {
        title: 'File Type',
        dataIndex: 'type',
        key: 'type',
        width: 150,
        render: (type, record) => (record.isDir ? 'Directory' : 'File')
      },
      {
        title: 'File Size',
        dataIndex: 'size',
        key: 'size',
        width: 200,
        render: size => formatFileSize(size)
      },
      {
        title: 'File Modified Time',
        dataIndex: 'lastModified',
        key: 'lastModified',
        render: lastModified => formatLastModified(lastModified)
      }
    ],
    [sub_path]
  )

  const { resizableColumns, components, tableWidth } = useAntdColumnResize(() => {
    return { columns, minWidth: 100 }
  }, [columns])

  if (!storageLocations || Object.keys(storageLocations).length === 0) {
    return <p>No storage locations configured</p>
  }

  if (!currentLocation && defaultLocationName) {
    setCurrentLocation(defaultLocationName)

    return <Spin />
  }

  if (!currentLocation && Object.keys(storageLocations).length > 0) {
    const firstLocation = Object.keys(storageLocations)[0]
    setCurrentLocation(firstLocation)

    return <Spin />
  }

  if (!currentLocation) {
    return <p>Please select a storage location</p>
  }

  const displayedFiles = [...store.tableData] || []

  const currentFullPath =
    currentLocation && storageLocations && storageLocations[currentLocation]
      ? `${storageLocations[currentLocation].replace(/\/$/, '')}${sub_path ? `/${sub_path}` : ''}`
      : ''

  return (
    <Spin spinning={store.tableLoading}>
      <Space direction='vertical' style={{ width: '100%' }}>
        <Space wrap style={{ justifyContent: 'space-between', width: '100%' }}>
          <Text type='secondary' style={{ display: 'flex', alignItems: 'center' }}>
            {sub_path && (
              <Button icon={<ArrowLeftOutlined />} onClick={handleBackClick} type='text' style={{ marginRight: 8 }} />
            )}
            {currentLocation && storageLocations && storageLocations[currentLocation] && (
              <Link onClick={() => setSubPath('')}>{storageLocations[currentLocation].replace(/\/$/, '')}</Link>
            )}
            {pathSegments.map((segment, index) => (
              <span key={index}>
                {'/'}
                <Link onClick={() => handlePathSegmentClick(index)}>{segment}</Link>
              </span>
            ))}
            {currentFullPath && <CopyOutlined onClick={handleCopyPath} style={{ marginLeft: 8, cursor: 'pointer' }} />}
          </Text>
          {Object.keys(storageLocations).length > 0 && (
            <Select
              value={currentLocation}
              style={{ minWidth: 200 }}
              onChange={handleLocationChange}
              placeholder='Select Location'
            >
              {Object.entries(storageLocations).map(
                (
                  [name, _path] // Changed path to _path to avoid conflict
                ) => (
                  <Option key={name} value={name}>
                    {name}
                  </Option>
                )
              )}
            </Select>
          )}
        </Space>

        <Table
          style={{ maxHeight: 'calc(100vh - 30rem)' }}
          scroll={{ y: 'calc(100vh - 37rem)' }}
          dataSource={displayedFiles}
          columns={resizableColumns}
          components={components}
          rowKey='name'
          size='small'
          pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
        />
      </Space>
    </Spin>
  )
}

export default ListFiles
