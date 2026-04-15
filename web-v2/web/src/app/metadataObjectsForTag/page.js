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

import { useMemo, useState, useEffect } from 'react'
import { useRouter } from 'next/navigation'
import { Flex, Input, Spin, Table, Tag, Tooltip, Typography } from 'antd'
import { useAntdColumnResize } from 'react-antd-column-resize'
import Icons from '@/components/Icons'
import SectionContainer from '@/components/SectionContainer'
import { useSearchParams } from 'next/navigation'
import { getTagDetails, associateTag } from '@/lib/store/tags'
import { getMetadataObjectsForTag } from '@/lib/store/metalakes'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'

const { Title, Paragraph } = Typography
const { Search } = Input

export default function MetadataObjectsForTagPage() {
  const [search, setSearch] = useState('')
  const router = useRouter()
  const searchParams = useSearchParams()
  const currentMetalake = searchParams.get('metalake')
  const tag = searchParams.get('tag')
  const dispatch = useAppDispatch()
  const [tagData, setTagData] = useState(null)
  const [metaDatas, setMetaDatas] = useState([])
  const [isLoading, setIsLoading] = useState(false)

  useEffect(() => {
    const initLoad = async () => {
      setIsLoading(true)
      const { payload: tagData } = await dispatch(getTagDetails({ metalake: currentMetalake, tag }))
      const { payload: metaDatas } = await dispatch(getMetadataObjectsForTag({ metalake: currentMetalake, tag }))
      setTagData(tagData)
      setMetaDatas(metaDatas)
      setIsLoading(false)
    }
    if (tag && currentMetalake) {
      initLoad()
    }
  }, [tag, dispatch, currentMetalake])

  const tableData = metaDatas
    ?.filter(d => {
      if (search === '') return true

      return d.fullName.includes(search)
    })
    .map(metaData => {
      return {
        ...metaData,
        key: metaData.fullName,
        type: metaData.type
      }
    })

  const onSearchTable = e => {
    const { value } = e.target
    setSearch(value)
  }

  const showDeleteConfirm = async object => {
    await dispatch(
      associateTag({
        metalake: currentMetalake,
        metadataObjectType: object.type,
        metadataObjectFullName: object.fullName,
        data: { tagsToRemove: [tag] }
      })
    )
    const { payload: metaDatas } = await dispatch(getMetadataObjectsForTag({ metalake: currentMetalake, tag }))
    setMetaDatas(metaDatas)
  }

  const columns = useMemo(
    () => [
      {
        title: 'Metadata Object Name',
        dataIndex: 'fullName',
        key: 'fullName',
        ellipsis: true,
        sorter: (a, b) => a.fullName.toLowerCase().localeCompare(b.fullName.toLowerCase()),
        width: 300,
        render: (_, record) => <span>{record.fullName}</span>
      },
      {
        title: 'Type',
        dataIndex: 'type',
        key: 'type'
      },
      {
        title: 'Actions',
        key: 'action',
        width: 100,
        render: (_, record) => {
          return (
            <div className='flex gap-2'>
              <a>
                <Tooltip title='Remove Associate'>
                  <Icons.Delete className='size-4' onClick={() => showDeleteConfirm(record)} />
                </Tooltip>
              </a>
            </div>
          )
        }
      }
    ],
    [currentMetalake, metaDatas]
  )

  const { resizableColumns, components, tableWidth } = useAntdColumnResize(() => {
    return { columns, minWidth: 100 }
  }, [columns])

  return (
    <SectionContainer classProps='block'>
      <div className='h-full bg-white p-6'>
        <Title level={2} className='flex items-center gap-1'>
          <Icons.Undo2 className='size-6 cursor-pointer' onClick={() => router.back()} />
          Metadata Objects
          <Tag color={tagData?.properties?.['color']}>{tag}</Tag>
        </Title>
        <Paragraph type='secondary'>This table lists the metadata objects associated with tag {tag}.</Paragraph>
        <Flex justify='flex-end' className='mb-4'>
          <div className='flex w-1/4 gap-4'>
            <Search name='searchCatalogInput' placeholder='Search...' onChange={onSearchTable} />
          </div>
        </Flex>
        <Spin spinning={isLoading}>
          <Table
            style={{ maxHeight: 'calc(100vh - 25rem)' }}
            scroll={{ x: tableWidth, y: 'calc(100vh - 30rem)' }}
            dataSource={tableData}
            columns={resizableColumns}
            components={components}
            pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
          />
        </Spin>
      </div>
    </SectionContainer>
  )
}
