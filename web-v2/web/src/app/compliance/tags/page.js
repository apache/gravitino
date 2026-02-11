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

import { createContext, useMemo, useState, useEffect } from 'react'
import { useRouter, useSearchParams } from 'next/navigation'
import { ExclamationCircleFilled, PlusOutlined } from '@ant-design/icons'
import { Button, Flex, Input, Modal, Spin, Table, Tag, Tooltip, Typography } from 'antd'
import { useAntdColumnResize } from 'react-antd-column-resize'
import ConfirmInput from '@/components/ConfirmInput'
import Icons from '@/components/Icons'
import SectionContainer from '@/components/SectionContainer'
import CreateTagDialog from './CreateTagDialog'
import { formatToDateTime } from '@/lib/utils'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { use } from 'react'
import { fetchTags, deleteTag } from '@/lib/store/tags'

const { Title, Paragraph } = Typography
const { Search } = Input

export default function TagsPage() {
  const [open, setOpen] = useState(false)
  const [editTag, setEditTag] = useState('')
  const [search, setSearch] = useState('')
  const [modal, contextHolder] = Modal.useModal()
  const router = useRouter()
  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.tags)
  const searchParams = useSearchParams()
  const currentMetalake = searchParams.get('metalake')

  useEffect(() => {
    currentMetalake && dispatch(fetchTags({ metalake: currentMetalake, details: true }))
  }, [dispatch, currentMetalake])

  const tableData = store.tagsData
    ?.filter(t => {
      if (search === '') return true

      return t?.name.includes(search)
    })
    .map(tag => {
      return {
        ...tag,
        createTime: tag.audit.createTime,
        key: tag.name
      }
    })

  const onSearchTable = e => {
    const { value } = e.target
    setSearch(value)
  }

  const handleCreateTag = () => {
    setEditTag('')
    setOpen(true)
  }

  const handleEditTag = tag => {
    setEditTag(tag)
    setOpen(true)
  }

  const showDeleteConfirm = (NameContext, tag) => {
    let confirmInput = ''
    let validateFn = null

    const setConfirmInput = value => {
      confirmInput = value
    }

    const registerValidate = fn => {
      validateFn = fn
    }
    modal.confirm({
      title: `Are you sure to delete the Tag ${tag}?`,
      icon: <ExclamationCircleFilled />,
      content: (
        <NameContext.Consumer>
          {name => <ConfirmInput name={name} setConfirmInput={setConfirmInput} registerValidate={registerValidate} />}
        </NameContext.Consumer>
      ),
      okText: 'Delete',
      okType: 'danger',
      cancelText: 'Cancel',
      onOk(close) {
        if (validateFn && !validateFn()) return

        const confirmFn = async () => {
          await dispatch(deleteTag({ metalake: currentMetalake, tag }))
          close()
        }
        confirmFn()
      }
    })
  }

  const handleClick = name => () => {
    router.push(`/metadataObjectsForTag?tag=${name}&metalake=${currentMetalake}`)
  }

  const columns = useMemo(
    () => [
      {
        title: 'Tag Name',
        dataIndex: 'name',
        key: 'name',
        ellipsis: true,
        width: 200,
        sorter: (a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase()),
        render: (_, record) => (
          <Tag
            key={record.name}
            color={record.properties?.color}
            closable={false}
            onClick={handleClick(record.name)}
            className='cursor-pointer'
            style={{ userSelect: 'none' }}
          >
            <span>{record.name.length > 20 ? `${record.name.slice(0, 20)}...` : record.name}</span>
          </Tag>
        )
      },
      {
        title: 'Created At',
        dataIndex: ['audit', 'createTime'],
        sorter: (a, b) => new Date(a.createTime).getTime() - new Date(b.createTime).getTime(),
        key: 'createTime',
        ellipsis: true,
        width: 200,
        render: (_, record) => <>{formatToDateTime(record.createTime)}</>
      },
      {
        title: 'Comment',
        dataIndex: 'comment',
        key: 'comment',
        ellipsis: true,
        render: comment => <span>{comment || '-'}</span>
      },
      {
        title: 'Actions',
        key: 'action',
        width: 100,
        render: (_, record) => {
          const NameContext = createContext(record.name)

          return (
            <div className='flex gap-2'>
              <NameContext.Provider value={record.name}>{contextHolder}</NameContext.Provider>
              <a>
                <Tooltip title='Edit'>
                  <Icons.Pencil className='size-4' onClick={() => handleEditTag(record.name)} />
                </Tooltip>
              </a>
              <a>
                <Tooltip title='Delete'>
                  <Icons.Trash2Icon className='size-4' onClick={() => showDeleteConfirm(NameContext, record.name)} />
                </Tooltip>
              </a>
            </div>
          )
        }
      }
    ],
    [currentMetalake, store.tagsData]
  )

  const { resizableColumns, components, tableWidth } = useAntdColumnResize(() => {
    return { columns, minWidth: 100 }
  }, [columns])

  return (
    <SectionContainer classProps='block'>
      <Title level={2}>Tags</Title>
      <Paragraph type='secondary'>This table lists the tags you have access to.</Paragraph>
      <Flex justify='flex-end' className='mb-4'>
        <div className='flex w-1/3 gap-4'>
          <Search name='searchCatalogInput' placeholder='Search...' onChange={onSearchTable} />
          <Button type='primary' icon={<PlusOutlined />} onClick={handleCreateTag}>
            Create Tag
          </Button>
        </div>
      </Flex>
      <Spin spinning={store.tagsLoading}>
        <Table
          style={{ maxHeight: 'calc(100vh - 25rem)' }}
          scroll={{ x: tableWidth, y: 'calc(100vh - 30rem)' }}
          dataSource={tableData}
          columns={resizableColumns}
          components={components}
          pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
        />
      </Spin>
      <CreateTagDialog open={open} setOpen={setOpen} metalake={currentMetalake} editTag={editTag} />
    </SectionContainer>
  )
}
