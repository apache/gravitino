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
import { StopOutlined } from '@ant-design/icons'
import { Alert, Button, Flex, Input, Spin, Table, Tag, Typography, theme as antdTheme } from 'antd'
import { useAntdColumnResize } from 'react-antd-column-resize'
import Icons from '@/components/Icons'
import SectionContainer from '@/components/SectionContainer'
import { useSearchParams } from 'next/navigation'
import { useAppDispatch } from '@/lib/hooks/useStore'
import { getPolicyDetails } from '@/lib/store/policies'
import { getTagsForPolicy } from '@/lib/store/metalakes'

const { Title, Paragraph } = Typography
const { Search } = Input

export default function MetadataObjectsForPolicyPage() {
  const [search, setSearch] = useState('')
  const searchParams = useSearchParams()
  const currentMetalake = searchParams.get('metalake')
  const policy = searchParams.get('policy') || ''
  const router = useRouter()
  const { token } = antdTheme.useToken()
  const dispatch = useAppDispatch()
  const [policyData, setPolicyData] = useState(null)
  const [associations, setAssociations] = useState([])
  const [loadError, setLoadError] = useState(false)
  const [isLoading, setIsLoading] = useState(false)

  useEffect(() => {
    const initLoad = async () => {
      setIsLoading(true)
      setLoadError(false)

      const { payload: policyData, error: policyError } = await dispatch(
        getPolicyDetails({ metalake: currentMetalake, policy })
      )

      const { payload: tags, error: tagsError } = await dispatch(
        getTagsForPolicy({ metalake: currentMetalake, policy })
      )
      setPolicyData(policyData)
      setAssociations(tags || [])
      setLoadError(Boolean(policyError || tagsError))
      setIsLoading(false)
    }
    if (policy && currentMetalake) {
      initLoad()
    }
  }, [policy, dispatch, currentMetalake])

  const tableData = associations
    .map(({ tag, selector }) => ({ ...tag, selector, key: tag.name }))
    .filter(tag => tag.name.includes(search))

  const onSearchTable = e => {
    const { value } = e.target
    setSearch(value)
  }

  const columns = useMemo(
    () => [
      {
        title: 'Tag Name',
        dataIndex: 'name',
        key: 'name',
        ellipsis: true,
        sorter: (a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase()),
        width: 300,
        render: name => (
          <Button
            type='link'
            className='p-0'
            onClick={() =>
              router.push(
                `/metadataObjectsForTag?tag=${encodeURIComponent(name)}&metalake=${encodeURIComponent(currentMetalake)}`
              )
            }
          >
            {name}
          </Button>
        )
      },
      {
        title: 'Selector',
        dataIndex: 'selector',
        key: 'selector',
        render: selector => (selector.type === 'ALL_VALUES' ? 'All values' : selector.value)
      }
    ],
    [currentMetalake, router]
  )

  const { resizableColumns, components, tableWidth } = useAntdColumnResize(() => {
    return { columns, minWidth: 100 }
  }, [columns])

  return (
    <SectionContainer classProps='block'>
      <div className='h-full bg-white p-6'>
        <Title level={2} className='flex items-center gap-1'>
          <Icons.Undo2 className='size-6 cursor-pointer' onClick={() => router.back()} />
          Associated Tags
          <Tag
            icon={!policyData?.enabled ? <StopOutlined style={{ color: 'white' }} /> : null}
            color={policyData?.enabled ? token.colorPrimary : token.colorTextDisabled}
          >
            {policy}
          </Tag>
        </Title>
        <Paragraph type='secondary'>
          Policies apply to metadata objects through tags. This table lists the tags associated with policy {policy}.
        </Paragraph>
        <Flex justify='flex-end' className='mb-4'>
          <div className='flex w-1/4 gap-4'>
            <Search name='searchCatalogInput' placeholder='Search...' onChange={onSearchTable} />
          </div>
        </Flex>
        <Spin spinning={isLoading}>
          {loadError ? (
            <Alert type='error' showIcon message='Unable to load policy tags.' />
          ) : (
            <Table
              style={{ maxHeight: 'calc(100vh - 25rem)' }}
              scroll={{ x: tableWidth, y: 'calc(100vh - 30rem)' }}
              dataSource={tableData}
              columns={resizableColumns}
              components={components}
              pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
            />
          )}
        </Spin>
      </div>
    </SectionContainer>
  )
}
