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

import React, { forwardRef, useEffect, useImperativeHandle, useMemo, useRef, useState } from 'react'

import { usePathname, useRouter, useSearchParams } from 'next/navigation'

import { Empty, Input, Spin, Tabs, Tooltip, Tree } from 'antd'
import { useTheme } from 'next-themes'

import Icons from '@/components/Icons'
import { checkCatalogIcon } from '@/config/catalog'
import { cn } from '@/lib/utils/tailwind'
import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import {
  setExpanded,
  setExpandedNodes,
  setIntoTreeNodeWithFetch,
  removeExpandedNode,
  setSelectedNodes,
  setLoadedNodes,
  getTableDetails,
  getFilesetDetails,
  getTopicDetails,
  getModelDetails,
  fetchModelVersions,
  getVersionDetails
} from '@/lib/store/metalakes'

import { extractPlaceholder } from '@/lib/utils'

const { Search } = Input

export const TreeComponent = forwardRef(function TreeComponent(props, ref) {
  const [treeData, setTreeData] = useState([])
  const [searchValue, setSearchValue] = useState('')
  const [expandedKeys, setExpandedKeys] = useState([])
  const [loadedKeys, setLoadedKeys] = useState([])
  const [autoExpandParent, setAutoExpandParent] = useState(false)
  const [selectedKeys, setSelectedKeys] = useState([])
  const router = useRouter()
  const pathname = usePathname()
  const searchParams = useSearchParams()
  const currentMetalake = searchParams.get('metalake')
  const [currentCatalogType, setCatalogType] = useState(searchParams.get('catalogType') || 'relational')
  const [, locale, , catalog, schema, entity] = pathname.split('/')
  const treeRef = useRef(null)
  const { theme } = useTheme()
  const [isHover, setIsHover] = useState('')
  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.metalakes)

  const catalogTypeOptions = [
    { label: 'Relational', key: 'relational' },
    { label: 'Messaging', key: 'messaging' },
    { label: 'Fileset', key: 'fileset' },
    { label: 'Model', key: 'model' }
  ]

  const updateTreeData = (list, key, children) =>
    list.map(node => {
      if (node.key === key) {
        return {
          ...node,
          isLeaf: children?.length === 0,
          children
        }
      }
      if (node.children) {
        return {
          ...node,
          isLeaf: node.children.length === 0,
          children: updateTreeData(node.children, key, children)
        }
      }

      return node
    })

  const handleDoubleClick = event => {
    event.preventDefault()
    event.stopPropagation()
    console.log('Double click is disabled')
  }

  const renderCatalogIcon = catalog => {
    const { key, catalogType, provider, inUse } = catalog
    if (inUse === 'false') {
      return (
        <Tooltip title='Not in use'>
          <span role='img' className='anticon'>
            <Icons.BanIcon className='size-3 text-gray-400' />
          </span>
        </Tooltip>
      )
    }
    const calalogIcon = checkCatalogIcon({ type: catalogType, provider })
    if (calalogIcon.startsWith('custom-icons-')) {
      switch (calalogIcon) {
        case 'custom-icons-hive':
          return (
            <span
              role='img'
              className='anticon'
              onMouseEnter={e => onMouseEnter(e, catalog)}
              onMouseLeave={e => onMouseLeave(e, catalog)}
              onClick={e => handleClickIcon(e, catalog)}
            >
              {isHover !== key ? (
                <Icons.hive className='h-4 w-3'></Icons.hive>
              ) : (
                <Icons.RotateCw className='h-4 w-3'></Icons.RotateCw>
              )}
            </span>
          )
        case 'custom-icons-doris':
          return (
            <span
              role='img'
              className='anticon'
              onMouseEnter={e => onMouseEnter(e, catalog)}
              onMouseLeave={e => onMouseLeave(e, catalog)}
              onClick={e => handleClickIcon(e, catalog)}
            >
              {isHover !== key ? (
                <Icons.doris className='h-4 w-3'></Icons.doris>
              ) : (
                <Icons.RotateCw className='h-4 w-3'></Icons.RotateCw>
              )}
            </span>
          )
        case 'custom-icons-paimon':
          return (
            <span
              role='img'
              className='anticon'
              onMouseEnter={e => onMouseEnter(e, catalog)}
              onMouseLeave={e => onMouseLeave(e, catalog)}
              onClick={e => handleClickIcon(e, catalog)}
            >
              {isHover !== key ? (
                <Icons.paimon className='size-3'></Icons.paimon>
              ) : (
                <Icons.RotateCw className='h-4 w-3'></Icons.RotateCw>
              )}
            </span>
          )
        case 'custom-icons-hudi':
          return (
            <span
              role='img'
              className='anticon'
              onMouseEnter={e => onMouseEnter(e, catalog)}
              onMouseLeave={e => onMouseLeave(e, catalog)}
              onClick={e => handleClickIcon(e, catalog)}
            >
              {isHover !== key ? (
                <Icons.hudi className='size-4'></Icons.hudi>
              ) : (
                <Icons.RotateCw className='h-4 w-3'></Icons.RotateCw>
              )}
            </span>
          )
        case 'custom-icons-oceanbase':
          return (
            <span
              role='img'
              className='anticon'
              onMouseEnter={e => onMouseEnter(e, catalog)}
              onMouseLeave={e => onMouseLeave(e, catalog)}
              onClick={e => handleClickIcon(e, catalog)}
            >
              {isHover !== key ? (
                <Icons.oceanbase className='size-4'></Icons.oceanbase>
              ) : (
                <Icons.RotateCw className='h-4 w-3'></Icons.RotateCw>
              )}
            </span>
          )
        case 'custom-icons-starrocks':
          return (
            <span
              role='img'
              className='anticon'
              onMouseEnter={e => onMouseEnter(e, catalog)}
              onMouseLeave={e => onMouseLeave(e, catalog)}
              onClick={e => handleClickIcon(e, catalog)}
            >
              {isHover !== key ? (
                <Icons.starrocks className='size-4'></Icons.starrocks>
              ) : (
                <Icons.RotateCw className='h-4 w-3'></Icons.RotateCw>
              )}
            </span>
          )
        case 'custom-icons-lakehouse':
          return (
            <span
              role='img'
              className='anticon'
              onMouseEnter={e => onMouseEnter(e, catalog)}
              onMouseLeave={e => onMouseLeave(e, catalog)}
              onClick={e => handleClickIcon(e, catalog)}
            >
              {isHover !== key ? (
                <Icons.lakehouse className='size-4'></Icons.lakehouse>
              ) : (
                <Icons.RotateCw className='h-4 w-3'></Icons.RotateCw>
              )}
            </span>
          )
      }
    } else {
      return (
        <span
          role='img'
          className='anticon'
          onMouseEnter={e => onMouseEnter(e, catalog)}
          onMouseLeave={e => onMouseLeave(e, catalog)}
          onClick={e => handleClickIcon(e, catalog)}
        >
          {isHover !== key ? (
            <Icons.iconify icon={calalogIcon} className='my-icon-small' />
          ) : (
            <Icons.RotateCw className='h-4 w-3'></Icons.RotateCw>
          )}
        </span>
      )
    }
  }

  const renderIcon = nodeProps => {
    switch (nodeProps.data.node) {
      case 'catalog':
        return renderCatalogIcon(nodeProps.data)
      case 'schema':
        return (
          <span
            role='img'
            aria-label='schema'
            className='anticon anticon-frown'
            onMouseEnter={e => onMouseEnter(e, nodeProps.data)}
            onMouseLeave={e => onMouseLeave(e, nodeProps.data)}
            onClick={e => handleClickIcon(e, nodeProps.data)}
          >
            {isHover !== nodeProps.data.key ? (
              <Icons.iconify icon='bx:coin-stack' className='my-icon-small' />
            ) : (
              <Icons.RotateCw className='h-4 w-3'></Icons.RotateCw>
            )}
          </span>
        )
      case 'table':
        return (
          <span role='img' aria-label='table' className='anticon anticon-frown'>
            <Icons.iconify icon='bx:table' className='my-icon-small' />
          </span>
        )
      case 'fileset':
        return (
          <span role='img' aria-label='fileset' className='anticon anticon-frown'>
            <Icons.iconify icon='bx:file' className='my-icon-small' />
          </span>
        )
      case 'topic':
        return (
          <span role='img' aria-label='topic' className='anticon anticon-frown'>
            <Icons.iconify icon='material-symbols:topic-outline' className='my-icon-small' />
          </span>
        )
      case 'model':
        return (
          <span role='img' aria-label='model' className='anticon anticon-frown'>
            <Icons.iconify icon='mdi:globe-model' className='my-icon-small' />
          </span>
        )
      case 'function':
        return (
          <span role='img' aria-label='function' className='anticon anticon-frown'>
            <Icons.iconify icon='material-symbols:function' className='my-icon-small' />
          </span>
        )
      default:
        return <></>
    }
  }

  useEffect(() => {
    if (searchParams.get('catalogType')) {
      setCatalogType(searchParams.get('catalogType'))
    } else {
      setExpandedKeys([])
      setCatalogType('relational')
    }
  }, [searchParams.get('catalogType')])

  useEffect(() => {
    setTimeout(() => {
      if (entity) {
        setExpandedKeys(
          Array.from(new Set([...expandedKeys, catalog, `${catalog}/${schema}`, `${catalog}/${schema}/${entity}`]))
        )
        setSelectedKeys([`${catalog}/${schema}/${entity}`])
      } else if (schema && !entity) {
        setExpandedKeys(Array.from(new Set([...expandedKeys, catalog, `${catalog}/${schema}`])))
        setSelectedKeys([`${catalog}/${schema}`])
      } else if (catalog && !schema && !entity) {
        setExpandedKeys(Array.from(new Set([...expandedKeys, catalog])))
        setSelectedKeys([catalog])
      } else if (!catalog && !schema && !entity) {
        setExpandedKeys(Array.from(new Set([...expandedKeys])))
      }
    }, 1000)
  }, [pathname])

  const renderTitle = title => {
    return (
      <div title={title} className='absolute bottom-[-7px] inline-block w-full truncate'>
        {title}
      </div>
    )
  }

  useEffect(() => {
    if (store.metalakeTree) {
      const catalogs = [...store.metalakeTree]

      const treeData = catalogs
        .filter(catalog => catalog.type === currentCatalogType)
        .sort((a, b) => new Date(b.audit.lastModifiedTime).getTime() - new Date(a.audit.lastModifiedTime).getTime())
        .map(catalog => ({
          ...catalog,
          title: renderTitle(catalog.name),

          // Set isLeaf to true when not in-use to hide expand icon
          isLeaf: catalog.inUse === 'false' ? true : catalog.isLeaf
        }))
      setTreeData(treeData)
    }
  }, [store.metalakeTree, currentCatalogType])

  const handleClickIcon = (e, nodeProps) => {
    e.stopPropagation()
    if (nodeProps.inUse === 'false') return

    switch (nodeProps.node) {
      case 'table': {
        if (store.selectedNodes.includes(nodeProps.key)) {
          const pathArr = extractPlaceholder(nodeProps.key)
          const [metalake, catalog, type, schema, table] = pathArr
          dispatch(getTableDetails({ init: true, metalake, catalog, schema, table }))
        }
        break
      }
      case 'fileset': {
        if (store.selectedNodes.includes(nodeProps.key)) {
          const pathArr = extractPlaceholder(nodeProps.key)
          const [metalake, catalog, type, schema, fileset] = pathArr
          dispatch(getFilesetDetails({ init: true, metalake, catalog, schema, fileset }))
        }
        break
      }
      case 'topic': {
        if (store.selectedNodes.includes(nodeProps.key)) {
          const pathArr = extractPlaceholder(nodeProps.key)
          const [metalake, catalog, type, schema, topic] = pathArr
          dispatch(getTopicDetails({ init: true, metalake, catalog, schema, topic }))
        }
        break
      }
      case 'model': {
        if (store.selectedNodes.includes(nodeProps.key)) {
          const pathArr = extractPlaceholder(nodeProps.key)
          const [metalake, catalog, type, schema, model] = pathArr
          dispatch(fetchModelVersions({ init: true, metalake, catalog, schema, model }))
          dispatch(getModelDetails({ init: true, metalake, catalog, schema, model }))
        }
        break
      }
      case 'version': {
        if (store.selectedNodes.includes(nodeProps.key)) {
          const pathArr = extractPlaceholder(nodeProps.key)
          const [metalake, catalog, type, schema, model, version] = pathArr
          dispatch(getVersionDetails({ init: true, metalake, catalog, schema, model, version }))
        }
        break
      }
      default:
        dispatch(setIntoTreeNodeWithFetch({ key: nodeProps.key, reload: true }))
    }
  }

  const onMouseEnter = (e, nodeProps) => {
    if (nodeProps.inUse === 'false') return
    if (nodeProps.node === 'table') {
      if (store.selectedNodes.includes(nodeProps.key)) {
        setIsHover(nodeProps.key)
      }
    } else {
      setIsHover(nodeProps.key)
    }
  }

  const onMouseLeave = (e, nodeProps) => {
    if (nodeProps.inUse === 'false') return
    setIsHover(null)
  }

  const onLoadData = node => {
    if (node.inUse === 'false') return new Promise(resolve => resolve())
    const { key, children } = node

    dispatch(setLoadedNodes([...store.loadedNodes, key]))

    return new Promise(resolve => {
      if (children && children.length !== 0) {
        resolve()

        return
      }

      dispatch(setIntoTreeNodeWithFetch({ key }))

      resolve()
    })
  }

  const onExpand = (keys, { expanded, node }) => {
    if (node.inUse === 'false') return
    dispatch(setExpanded(keys))
    setAutoExpandParent(false)
  }

  const onSelect = (keys, { selected, node }) => {
    if (node.inUse === 'false') return
    if (!selected) {
      dispatch(setSelectedNodes([node.key]))

      return
    }

    dispatch(setSelectedNodes(keys))
    router.push(node.path)
  }

  const setCatalogInUse = (name, inUse) => {
    const inUseStr = inUse.toString()
    let tree = [...treeData]
    for (let i = 0; i < tree.length; i++) {
      if (tree[i].name === name) {
        if (inUseStr === 'false') {
          // Not in-use: set isLeaf to true to hide expand icon, clear children
          tree[i] = { ...tree[i], inUse: inUseStr, isLeaf: true, children: [] }
        } else {
          // In-use: set isLeaf to false to show expand icon, allow reload
          tree[i] = { ...tree[i], inUse: inUseStr, isLeaf: false, children: undefined }
        }
        break
      }
    }
    if (inUseStr === 'false') {
      // Remove from expanded and loaded nodes
      dispatch(setExpandedNodes([...store.expandedNodes.filter(key => !key.includes(name))]))
      dispatch(setLoadedNodes([...store.loadedNodes.filter(key => !key.includes(name))]))
    } else {
      // Remove from loaded nodes to allow re-fetching children when expanded
      dispatch(setLoadedNodes([...store.loadedNodes.filter(key => key !== name)]))
    }
    setTreeData(tree)
  }

  useImperativeHandle(ref, () => ({
    onLoadData,
    setCatalogInUse
  }))

  const getParentKey = (key, tree) => {
    let parentKey
    for (let i = 0; i < tree.length; i++) {
      const node = tree[i]
      if (node.children?.length) {
        if (node.children.some(item => item.key === key)) {
          parentKey = node.key
        } else if (getParentKey(key, node.children)) {
          parentKey = getParentKey(key, node.children)
        }
      }
    }

    return parentKey
  }

  const dataList = useMemo(() => {
    const list = []

    const generateList = data => {
      for (let i = 0; i < data.length; i++) {
        const node = data[i]
        const { key, name } = node
        list.push({ key, name })
        if (node.children?.length) {
          generateList(node.children)
        }
      }
    }
    generateList(treeData)

    return list
  }, [treeData])

  const onSearchTree = e => {
    const { value } = e.target

    const newExpandedKeys = dataList
      .map(item => {
        if (item.name.indexOf(value) > -1) {
          return getParentKey(item.key, treeData)
        }

        return null
      })
      .filter((item, i, self) => !!(item && self.indexOf(item) === i))

    // Use setExpanded to replace (not merge) expanded nodes for search
    dispatch(setExpanded(newExpandedKeys))
    setSearchValue(value)
    setAutoExpandParent(newExpandedKeys.length > 0)
  }

  const searchTreeData = useMemo(() => {
    const loop = data =>
      data.map(item => {
        const strTitle = item.name
        const index = strTitle.indexOf(searchValue)
        const beforeStr = strTitle.substring(0, index)
        const afterStr = strTitle.slice(index + searchValue.length)

        const title =
          index > -1 ? (
            <div title={item.name} className='absolute bottom-[-7px] inline-block w-full truncate'>
              {beforeStr}
              <span className='text-defaultPrimary'>{searchValue}</span>
              {afterStr}
            </div>
          ) : (
            <div title={item.name} className='absolute bottom-[-7px] inline-block w-full truncate'>
              {strTitle}
            </div>
          )
        if (item.children?.length) {
          return {
            ...item,
            title,
            name: item.name,
            key: item.key,
            node: item.node,
            icon: item.icon,

            // Keep isLeaf true for not in-use catalogs
            isLeaf: item.inUse === 'false' ? true : item.children.length === 0,
            children: loop(item.children)
          }
        }

        return {
          ...item,
          title,
          name: item.name,
          key: item.key,
          node: item.node,
          icon: item.icon,

          // Keep isLeaf true for not in-use catalogs
          isLeaf: item.inUse === 'false' ? true : item.isLeaf
        }
      })

    return loop(treeData)
  }, [searchValue, treeData])

  const onChangeType = key => {
    setCatalogType(key)
    setExpandedKeys([])
    setLoadedKeys([])
    setSelectedKeys([])
    setTreeData([])
    router.push(`/catalogs?metalake=${currentMetalake}&catalogType=${key}`)
  }

  return (
    <div data-refer='tree-view'>
      <Tabs onChange={onChangeType} activeKey={currentCatalogType} items={catalogTypeOptions} />
      <Search name='searchTreeInput' className='mb-2' placeholder='Search...' onChange={onSearchTree} />
      <Spin spinning={false}>
        {treeData && treeData.length > 0 ? (
          <Tree
            ref={treeRef}
            treeData={searchTreeData}
            loadData={onLoadData}
            onSelect={onSelect}
            onExpand={onExpand}
            autoExpandParent={autoExpandParent}
            loadedKeys={store.loadedNodes}
            selectedKeys={store.selectedNodes}
            expandedKeys={store.expandedNodes}
            onDoubleClick={handleDoubleClick}
            blockNode
            showIcon
            onMouseLeave={e => onMouseLeave(e, {})}
            icon={nodeProps => renderIcon(nodeProps)}
            className={cn([
              '[&_.ant-tree-title]:relative',
              '[&_.ant-tree-title]:inline-flex',
              '[&_.ant-tree-title]:w-[calc(100%-24px)]'
            ])}
            style={{ maxHeight: 'calc(100vh - 22rem)', overflowY: 'auto', overflowAnchor: 'none' }}
          />
        ) : (
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} />
        )}
      </Spin>
    </div>
  )
})
