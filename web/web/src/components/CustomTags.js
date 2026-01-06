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

import React, { useEffect, useRef, useState } from 'react'

import { useRouter } from 'next/navigation'

import { LockOutlined, PlusOutlined } from '@ant-design/icons'
import { Flex, Select, Space, Tag, Tooltip, theme } from 'antd'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { associateTag } from '@/lib/store/tags'
import { getCurrentEntityTags } from '@/lib/store/metalakes'
import { useSearchParams } from 'next/navigation'

const tagInputStyle = {
  width: 100,
  height: 22,
  marginInlineEnd: 8,
  verticalAlign: 'top'
}

export default function Tags({ ...props }) {
  const { readOnly, metadataObjectType, metadataObjectFullName } = props
  const { token } = theme.useToken()
  const [inputVisible, setInputVisible] = useState(false)
  const [optionList, setOptionList] = useState([])
  const inputRef = useRef(null)
  const router = useRouter()
  const store = useAppSelector(state => state.tags)
  const dispatch = useAppDispatch()
  const [tagsForEntity, setTagsForEntity] = useState([])
  const searchParams = useSearchParams()
  const currentMetalake = searchParams.get('metalake')

  const getTagsForEntity = async () => {
    const { payload } = await dispatch(
      getCurrentEntityTags({ metalake: currentMetalake, metadataObjectType, metadataObjectFullName, details: true })
    )

    return payload?.tags || []
  }

  useEffect(() => {
    const initLoad = async () => {
      const tags = await getTagsForEntity()
      setTagsForEntity(tags)
    }
    if (currentMetalake && metadataObjectType && metadataObjectFullName) {
      initLoad()
    }
  }, [metadataObjectType, metadataObjectFullName])

  useEffect(() => {
    if (inputVisible) {
      const options =
        store.tagsData?.filter(tagOption => !tagsForEntity?.map(tag => tag.name)?.includes(tagOption.name)) || []
      setOptionList(options)
      inputRef.current?.focus()
    }
  }, [inputVisible, tagsForEntity])

  const showInput = () => {
    setInputVisible(true)
  }

  const tagRender = props => {
    const { value, closable, onClose } = props

    const onPreventMouseDown = event => {
      event.preventDefault()
      event.stopPropagation()
    }
    const tagColor = store.tagsData?.find(tag => tag.name === value)?.properties?.color

    return (
      <Tag
        color={tagColor}
        onMouseDown={onPreventMouseDown}
        closable={closable}
        onClose={onClose}
        style={{ marginInlineEnd: 4 }}
      >
        {value}
      </Tag>
    )
  }

  const handleChange = async value => {
    if (value && !tagsForEntity?.map(tag => tag.name)?.includes(value)) {
      await dispatch(
        associateTag({
          metalake: currentMetalake,
          metadataObjectType,
          metadataObjectFullName,
          data: { tagsToAdd: [value] }
        })
      )
    }
    const tags = await getTagsForEntity()
    setTagsForEntity(tags)
    setInputVisible(false)
  }

  const handleClose = async removedTag => {
    await dispatch(
      associateTag({
        metalake: currentMetalake,
        metadataObjectType,
        metadataObjectFullName,
        data: { tagsToRemove: [removedTag] }
      })
    )
    const tags = await getTagsForEntity()
    setTagsForEntity(tags)
  }

  const handleBlur = () => {
    setInputVisible(false)
  }

  const handleClick = name => () => {
    router.push(`/metadataObjectsForTag?tag=${name}&metalake=${currentMetalake}`)
  }

  const tagPlusStyle = {
    height: 22,
    background: token.colorBgContainer,
    borderStyle: 'dashed'
  }

  return (
    <Flex gap='4px 0' wrap>
      {tagsForEntity?.map((tag, index) => {
        const isLongTag = tag.name.length > 20
        let color = tag.properties['color'] || 'grey'

        const tagElem = (
          <Tag
            key={tag.name + index}
            color={color}
            closable={!readOnly && !tag.inherited}
            icon={tag.inherited ? <LockOutlined /> : null}
            onClick={handleClick(tag.name)}
            className='cursor-pointer'
            style={{ userSelect: 'none' }}
            onClose={() => handleClose(tag.name)}
          >
            <span>{isLongTag ? `${tag.name.slice(0, 20)}...` : tag.name}</span>
          </Tag>
        )

        return isLongTag ? (
          <Tooltip title={tag.name} key={tag.name}>
            {tagElem}
          </Tooltip>
        ) : (
          tagElem
        )
      })}
      {tagsForEntity?.length === 0 && <span>No Tags </span>}
      {inputVisible && !readOnly && (
        <Select
          size='small'
          tagRender={tagRender}
          ref={inputRef}
          style={tagInputStyle}
          onChange={handleChange}
          onBlur={handleBlur}
        >
          {optionList.map(tag => (
            <Select.Option value={tag.name} key={tag.name}>
              <Space>
                <span className='tag-swatch' style={{ backgroundColor: tag.properties?.color }}></span>
                <span>{tag.name}</span>
              </Space>
            </Select.Option>
          ))}
        </Select>
      )}
      {!inputVisible && !readOnly && (
        <Tag style={tagPlusStyle} icon={<PlusOutlined />} onClick={showInput}>
          Associate Tag
        </Tag>
      )}
    </Flex>
  )
}
