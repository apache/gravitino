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

import { useEffect, useRef, useState } from 'react'

import { useRouter } from 'next/navigation'

import { IconButton, Typography, Box } from '@mui/material'
import { Tree } from 'antd'

import Icon from '@/components/Icon'
import clsx from 'clsx'

import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import {
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

const MetalakeTree = props => {
  const { height: offsetHeight } = props

  const router = useRouter()
  const treeRef = useRef()
  const [height, setHeight] = useState(0)
  const [isHover, setIsHover] = useState(null)

  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.metalakes)

  const checkCatalogIcon = ({ type, provider }) => {
    switch (type) {
      case 'relational':
        switch (provider) {
          case 'hive':
            return 'custom-icons-hive'
          case 'lakehouse-iceberg':
            return 'openmoji:iceberg'
          case 'jdbc-mysql':
            return 'devicon:mysql-wordmark'
          case 'jdbc-postgresql':
            return 'devicon:postgresql-wordmark'
          case 'jdbc-doris':
            return 'custom-icons-doris'
          case 'jdbc-starrocks':
            return 'custom-icons-starrocks'
          case 'lakehouse-paimon':
            return 'custom-icons-paimon'
          case 'lakehouse-hudi':
            return 'custom-icons-hudi'
          case 'jdbc-oceanbase':
            return 'custom-icons-oceanbase'
          default:
            return 'bx:book'
        }
      case 'messaging':
        return 'skill-icons:kafka'
      case 'fileset':
        return 'twemoji:file-folder'
      case 'model':
        return 'carbon:machine-learning-model'
      default:
        return 'bx:book'
    }
  }

  const handleClickIcon = (e, nodeProps) => {
    e.stopPropagation()
    if (nodeProps.data.inUse === 'false') return

    switch (nodeProps.data.node) {
      case 'table': {
        if (store.selectedNodes.includes(nodeProps.data.key)) {
          const pathArr = extractPlaceholder(nodeProps.data.key)
          const [metalake, catalog, type, schema, table] = pathArr
          dispatch(getTableDetails({ init: true, metalake, catalog, schema, table }))
        }
        break
      }
      case 'fileset': {
        if (store.selectedNodes.includes(nodeProps.data.key)) {
          const pathArr = extractPlaceholder(nodeProps.data.key)
          const [metalake, catalog, type, schema, fileset] = pathArr
          dispatch(getFilesetDetails({ init: true, metalake, catalog, schema, fileset }))
        }
        break
      }
      case 'topic': {
        if (store.selectedNodes.includes(nodeProps.data.key)) {
          const pathArr = extractPlaceholder(nodeProps.data.key)
          const [metalake, catalog, type, schema, topic] = pathArr
          dispatch(getTopicDetails({ init: true, metalake, catalog, schema, topic }))
        }
        break
      }
      case 'model': {
        if (store.selectedNodes.includes(nodeProps.data.key)) {
          const pathArr = extractPlaceholder(nodeProps.data.key)
          const [metalake, catalog, type, schema, model] = pathArr
          dispatch(fetchModelVersions({ init: true, metalake, catalog, schema, model }))
          dispatch(getModelDetails({ init: true, metalake, catalog, schema, model }))
        }
        break
      }
      case 'version': {
        if (store.selectedNodes.includes(nodeProps.data.key)) {
          const pathArr = extractPlaceholder(nodeProps.data.key)
          const [metalake, catalog, type, schema, model, version] = pathArr
          dispatch(getVersionDetails({ init: true, metalake, catalog, schema, model, version }))
        }
        break
      }
      default:
        dispatch(setIntoTreeNodeWithFetch({ key: nodeProps.data.key, reload: true }))
    }
  }

  const onMouseEnter = (e, nodeProps) => {
    if (nodeProps.data.inUse === 'false') return
    if (nodeProps.data.node === 'table') {
      if (store.selectedNodes.includes(nodeProps.data.key)) {
        setIsHover(nodeProps.data.key)
      }
    } else {
      setIsHover(nodeProps.data.key)
    }
  }

  const onMouseLeave = (e, nodeProps) => {
    if (nodeProps.data.inUse === 'false') return
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
    if (expanded) {
      dispatch(setExpandedNodes(keys))
    } else {
      dispatch(removeExpandedNode(node.key))
    }
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

  const renderIcon = nodeProps => {
    switch (nodeProps.data.node) {
      case 'catalog':
        return (
          <IconButton
            size='small'
            sx={{ color: '#666' }}
            onClick={e => handleClickIcon(e, nodeProps)}
            onMouseEnter={e => onMouseEnter(e, nodeProps)}
            onMouseLeave={e => onMouseLeave(e, nodeProps)}
            data-refer={`tree-node-refresh-${nodeProps.data.key}`}
          >
            <Icon
              icon={
                isHover !== nodeProps.data.key
                  ? checkCatalogIcon({ type: nodeProps.data.type, provider: nodeProps.data.provider })
                  : 'mdi:reload'
              }
              fontSize='inherit'
            />
          </IconButton>
        )

      case 'schema':
        return (
          <IconButton
            size='small'
            sx={{ color: '#666' }}
            onClick={e => handleClickIcon(e, nodeProps)}
            onMouseEnter={e => onMouseEnter(e, nodeProps)}
            onMouseLeave={e => onMouseLeave(e, nodeProps)}
            data-refer={`tree-node-refresh-${nodeProps.data.key}`}
          >
            <Icon icon={isHover !== nodeProps.data.key ? 'bx:coin-stack' : 'mdi:reload'} fontSize='inherit' />
          </IconButton>
        )
      case 'table':
        return (
          <IconButton
            disableRipple={!store.selectedNodes.includes(nodeProps.data.key)}
            size='small'
            sx={{ color: '#666' }}
            onClick={e => handleClickIcon(e, nodeProps)}
            onMouseEnter={e => onMouseEnter(e, nodeProps)}
            onMouseLeave={e => onMouseLeave(e, nodeProps)}
            data-refer={`tree-node-refresh-${nodeProps.data.key}`}
          >
            <Icon icon={isHover !== nodeProps.data.key ? 'bx:table' : 'mdi:reload'} fontSize='inherit' />
          </IconButton>
        )
      case 'fileset':
        return (
          <IconButton
            disableRipple={!store.selectedNodes.includes(nodeProps.data.key)}
            size='small'
            sx={{ color: '#666' }}
            onClick={e => handleClickIcon(e, nodeProps)}
            onMouseEnter={e => onMouseEnter(e, nodeProps)}
            onMouseLeave={e => onMouseLeave(e, nodeProps)}
            data-refer={`tree-node-refresh-${nodeProps.data.key}`}
          >
            <Icon icon={isHover !== nodeProps.data.key ? 'bx:file' : 'mdi:reload'} fontSize='inherit' />
          </IconButton>
        )
      case 'topic':
        return (
          <IconButton
            disableRipple={!store.selectedNodes.includes(nodeProps.data.key)}
            size='small'
            sx={{ color: '#666' }}
            onClick={e => handleClickIcon(e, nodeProps)}
            onMouseEnter={e => onMouseEnter(e, nodeProps)}
            onMouseLeave={e => onMouseLeave(e, nodeProps)}
          >
            <Icon
              icon={isHover !== nodeProps.data.key ? 'material-symbols:topic-outline' : 'mdi:reload'}
              fontSize='inherit'
            />
          </IconButton>
        )

      case 'model':
        return (
          <IconButton
            disableRipple={!store.selectedNodes.includes(nodeProps.data.key)}
            size='small'
            sx={{ color: '#666' }}
            onClick={e => handleClickIcon(e, nodeProps)}
            onMouseEnter={e => onMouseEnter(e, nodeProps)}
            onMouseLeave={e => onMouseLeave(e, nodeProps)}
          >
            <Icon icon={isHover !== nodeProps.data.key ? 'mdi:globe-model' : 'mdi:reload'} fontSize='inherit' />
          </IconButton>
        )

      default:
        return <></>
    }
  }

  const renderNode = nodeData => {
    const len = extractPlaceholder(nodeData.key).length
    const maxWidth = 260 - (26 * 2 - 26 * (5 - len))
    if (nodeData.path) {
      return (
        <Typography
          sx={{
            color: theme => theme.palette.text.secondary,
            whiteSpace: 'nowrap',
            maxWidth,
            overflow: 'hidden',
            textOverflow: 'ellipsis'
          }}
          data-refer='tree-node'
          data-refer-node={nodeData.key}
        >
          {nodeData.title}
        </Typography>
      )
    }

    return nodeData.title
  }

  useEffect(() => {
    if (offsetHeight) {
      setHeight(offsetHeight)
    }
  }, [offsetHeight])

  useEffect(() => {
    if (store.selectedNodes.length !== 0) {
      treeRef.current && treeRef.current.scrollTo({ key: store.selectedNodes[0] })
    }
  }, [store.selectedNodes, treeRef])

  useEffect(() => {
    dispatch(setExpandedNodes(store.expandedNodes))
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [store.metalakeTree, dispatch])

  return (
    <>
      {store.metalakeTree.length ? (
        <Tree
          ref={treeRef}
          rootStyle={{
            '& .antTreeTitle': {
              width: '100%'
            }
          }}
          treeData={store.metalakeTree}
          loadData={onLoadData}
          loadedKeys={store.loadedNodes}
          selectedKeys={store.selectedNodes}
          expandedKeys={store.expandedNodes}
          onExpand={onExpand}
          onSelect={onSelect}
          height={height}
          defaultExpandAll
          blockNode
          showIcon
          className={clsx([
            '[&_.ant-tree-switcher]:twc-inline-flex',
            '[&_.ant-tree-switcher]:twc-justify-center',
            '[&_.ant-tree-switcher]:twc-items-center',

            '[&_.ant-tree-iconEle]:twc-w-[unset]',
            '[&_.ant-tree-iconEle]:twc-inline-flex',
            '[&_.ant-tree-iconEle]:twc-items-center',

            '[&_.ant-tree-title]:twc-inline-flex',
            '[&_.ant-tree-title]:twc-w-[calc(100%-24px)]',
            '[&_.ant-tree-title]:twc-text-lg',

            '[&_.ant-tree-node-content-wrapper]:twc-inline-flex',
            '[&_.ant-tree-node-content-wrapper]:twc-items-center',
            '[&_.ant-tree-node-content-wrapper]:twc-leading-[28px]'
          ])}
          data-refer='tree-view'
          icon={nodeProps => renderIcon(nodeProps)}
          titleRender={nodeData => renderNode(nodeData)}
        />
      ) : (
        <Box className={`twc-h-full twc-grow twc-flex twc-items-center twc-flex-col twc-justify-center`}>
          <Typography sx={{ color: theme => theme.palette.text.primary }} data-refer='no-data'>
            No data
          </Typography>
        </Box>
      )}
    </>
  )
}

export default MetalakeTree
