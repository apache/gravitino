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
import { Cascader, Flex, Input, Space, Tag, Tooltip, theme } from 'antd'
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
  const [selectedTag, setSelectedTag] = useState(null)
  const [selectedValue, setSelectedValue] = useState(undefined)
  const associatingRef = useRef(false)
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
        store.tagsData?.filter(tagOption => {
          const associatedTag = tagsForEntity.find(tag => tag.name === tagOption.name)

          return (
            !associatedTag ||
            tagOption.allowedValues == null ||
            (tagOption.allowedValues?.length &&
              tagOption.allowedValues.some(value => !associatedTag.assignmentValues?.includes(value)))
          )
        }) || []
      setOptionList(options)
      inputRef.current?.focus()
    }
  }, [inputVisible, tagsForEntity])

  const showInput = () => {
    setInputVisible(true)
  }

  const handleChange = path => {
    const tag = store.tagsData?.find(item => item.name === path[0])

    if (tag?.allowedValues == null && path[1] === '__no_value__') {
      associateSelectedTag(tag, undefined)

      return
    }

    setSelectedTag(tag)
    setSelectedValue(
      tag?.allowedValues == null && path[1] === '__custom_value__'
        ? ''
        : (path[1] ?? (tag?.allowedValues == null ? '' : undefined))
    )
  }

  const associateSelectedTag = async (tag, value) => {
    if (!tag || associatingRef.current) return
    if (tagsForEntity.some(item => item.name === tag.name && (!value || item.assignmentValues?.includes(value)))) {
      setSelectedTag(null)
      setSelectedValue(undefined)
      setInputVisible(false)

      return
    }
    associatingRef.current = true
    await dispatch(
      associateTag({
        metalake: currentMetalake,
        metadataObjectType,
        metadataObjectFullName,
        data: { tagsToAdd: [{ name: tag.name, ...(value !== undefined ? { value } : {}) }] }
      })
    )
    setSelectedTag(null)
    setSelectedValue(undefined)
    const tags = await getTagsForEntity()
    setTagsForEntity(tags)
    setInputVisible(false)
    associatingRef.current = false
  }

  const handleClose = async (tag, value) => {
    await dispatch(
      associateTag({
        metalake: currentMetalake,
        metadataObjectType,
        metadataObjectFullName,
        data: {
          tagsToRemove: [{ name: tag.name, ...(value !== undefined ? { value } : {}) }]
        }
      })
    )
    const tags = await getTagsForEntity()
    setTagsForEntity(tags)
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
      {tagsForEntity?.flatMap(tag =>
        (tag.assignmentValues?.length ? tag.assignmentValues : [undefined]).map((value, index) => {
          const label = value === undefined ? tag.name : `${tag.name}=${value}`
          const isLongTag = label.length > 20
          const color = tag.properties['color'] || 'grey'

          const tagElem = (
            <Tag
              key={`${tag.name}:${value ?? index}`}
              color={color}
              closable={!readOnly && !tag.inherited}
              icon={tag.inherited ? <LockOutlined /> : null}
              onClick={handleClick(tag.name)}
              className='cursor-pointer'
              style={{ userSelect: 'none' }}
              onClose={() => handleClose(tag, value)}
            >
              <span>{isLongTag ? `${label.slice(0, 20)}...` : label}</span>
            </Tag>
          )

          return isLongTag ? (
            <Tooltip title={label} key={`${tag.name}:${value}`}>
              {tagElem}
            </Tooltip>
          ) : (
            tagElem
          )
        })
      )}
      {tagsForEntity?.length === 0 && <span>No Tags </span>}
      {inputVisible && !readOnly && (
        <Cascader
          size='small'
          ref={inputRef}
          open={inputVisible}
          style={tagInputStyle}
          options={optionList.map(tag => ({
            value: tag.name,
            label: (
              <Space>
                <span className='tag-swatch' style={{ backgroundColor: tag.properties?.color }}></span>
                <span>{tag.name}</span>
              </Space>
            ),
            ...(tag.allowedValues?.length
              ? { children: tag.allowedValues.map(value => ({ value, label: value })) }
              : tag.allowedValues == null
                ? {
                    children: [
                      { value: '__no_value__', label: 'Assign without a value' },
                      {
                        value: '__custom_value__',
                        label: (
                          <Input
                            size='small'
                            placeholder='Enter value'
                            maxLength={256}
                            value={selectedTag?.name === tag.name ? selectedValue || '' : ''}
                            onFocus={() => setSelectedTag(tag)}
                            onMouseDown={event => {
                              event.stopPropagation()
                              setSelectedTag(tag)
                            }}
                            onChange={event => {
                              setSelectedTag(tag)
                              setSelectedValue(event.target.value)
                            }}
                            onBlur={() => {
                              if (selectedValue?.trim()) {
                                associateSelectedTag(tag, selectedValue.trim())
                              } else {
                                setSelectedTag(null)
                                setInputVisible(false)
                              }
                            }}
                            onKeyDown={event => event.stopPropagation()}
                          />
                        )
                      }
                    ]
                  }
                : {})
          }))}
          onChange={handleChange}
          onBlur={() => {
            if (selectedTag && selectedTag.allowedValues == null) return
            if (selectedTag && selectedValue !== undefined && selectedValue !== '') {
              associateSelectedTag(selectedTag, selectedValue || undefined)
            } else if (selectedTag?.allowedValues?.length === 0) {
              associateSelectedTag(selectedTag, undefined)
            } else if (!selectedTag) {
              setInputVisible(false)
            }
          }}
          placeholder='Associate Tag'
        />
      )}
      {!inputVisible && !readOnly && (
        <Tag style={tagPlusStyle} icon={<PlusOutlined />} onClick={showInput}>
          Associate Tag
        </Tag>
      )}
    </Flex>
  )
}
