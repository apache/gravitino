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

import { Inconsolata } from 'next/font/google'

import { useState, useEffect, Fragment } from 'react'

import { styled, Box, Divider, List, ListItem, ListItemText, Stack, Tab, Typography } from '@mui/material'
import Tooltip, { tooltipClasses } from '@mui/material/Tooltip'
import { TabContext, TabList, TabPanel } from '@mui/lab'

import clsx from 'clsx'
import { useAppSelector } from '@/lib/hooks/useStore'
import { useSearchParams } from 'next/navigation'
import TableView from './tableView/TableView'
import DetailsView from './detailsView/DetailsView'
import Icon from '@/components/Icon'
import dynamic from 'next/dynamic'
import Loading from '@/app/rootLayout/Loading'

const DynamicIframe = dynamic(() => import('./dynamicIframe/DynamicIframe'), {
  loading: () => <Loading />,
  ssr: false,
});

const fonts = Inconsolata({ subsets: ['latin'] })

const CustomTab = props => {
  const { icon, label, value, ...others } = props

  return (
    <Tab
      label={
        <Box sx={{ display: 'flex', alignItems: 'center' }}>
          <Icon icon={icon} />
          <Typography className={`twc-font-bold twc-ml-1 twc-normal-case`} color={'inherit'}>
            {label}
          </Typography>
        </Box>
      }
      value={value}
      {...others}
    />
  )
}

const CustomTooltip = styled(({ className, ...props }) => <Tooltip {...props} classes={{ popper: className }} />)(
  ({ theme }) => ({
    [`& .${tooltipClasses.tooltip}`]: {
      backgroundColor: '#23282a',
      padding: 0,
      border: '1px solid #dadde9'
    }
  })
)

const CustomTabPanel = props => {
  const { value, children, ...others } = props

  return (
    <TabPanel value={value} sx={{ boxShadow: 'none', p: 0, height: 'calc(100% - 4rem)' }} {...others}>
      {children}
    </TabPanel>
  )
}

const TabsContent = () => {
  let tableTitle = ''
  const store = useAppSelector(state => state.metalakes)
  const searchParams = useSearchParams()
  const paramsSize = [...searchParams.keys()].length
  const type = searchParams.get('type')
  const [tab, setTab] = useState('table')
  const isNotNeedTableTab = type && ['fileset', 'messaging'].includes(type) && paramsSize === 5
  const isFilesetPage = type && type === 'fileset' && paramsSize === 5
  const isShowTableProps = paramsSize === 5 && !['fileset', 'messaging'].includes(type)

  const handleChangeTab = (event, newValue) => {
    setTab(newValue)
  }

  switch (paramsSize) {
    case 1:
      tableTitle = 'Catalogs'
      break
    case 3:
      tableTitle = 'Schemas'
      break
    case 4:
      switch (type) {
        case 'fileset':
          tableTitle = 'Filesets'
          break
        case 'messaging':
          tableTitle = 'Topics'
          break
        default:
          tableTitle = 'Tables'
      }
      break
    case 5:
      tableTitle = 'Columns'
      break
    default:
      break
  }

  useEffect(() => {
    if (isFilesetPage) {
      setTab('datasetcard')
    } else if (isNotNeedTableTab) {
      setTab('details')
    } else {
      setTab('table')
    }

    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams])

  return (
    <TabContext value={tab}>
      <Box
        sx={{
          px: 6,
          py: 2,
          borderBottom: 1,
          borderColor: 'divider',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center'
        }}
      >
        <TabList onChange={handleChangeTab} aria-label='tabs' variant='scrollable' scrollButtons='auto'>
          {!isNotNeedTableTab ? (
            <CustomTab icon='mdi:list-box-outline' label={tableTitle} value='table' data-refer='tab-table' />
          ) : null}
          {isFilesetPage && (
            <CustomTab icon='mdi:list-box-outline' label='Dataset card' value='datasetcard' data-refer='tab-datasetcard' />
          )}
          {isFilesetPage && (
            <CustomTab icon='mdi:list-box-outline' label='Files and versions' value='filesversions' data-refer='tab-filesversions' />
          )}
          <CustomTab icon='mdi:clipboard-text-outline' label='Details' value='details' data-refer='tab-details' />
        </TabList>
        {isShowTableProps && (
          <Box>
            <List dense sx={{ p: 0 }}>
              <Stack spacing={0} direction={'row'} divider={<Divider orientation='vertical' flexItem />}>
                {store.tableProps
                  .filter(i => i.items.length !== 0)
                  .map((item, index) => {
                    return (
                      <CustomTooltip
                        key={item.type}
                        title={
                          <>
                            <Box
                              sx={{
                                backgroundColor: '#525c61',
                                p: 1.5,
                                px: 4,
                                borderTopLeftRadius: 4,
                                borderTopRightRadius: 4
                              }}
                            >
                              <Typography
                                color='white'
                                fontWeight={700}
                                fontSize={14}
                                sx={{ textTransform: 'capitalize' }}
                              >
                                {item.type}:{' '}
                              </Typography>
                            </Box>

                            <Box sx={{ p: 1.5, px: 4 }}>
                              {item.items.map((it, idx) => {
                                return (
                                  <Fragment key={idx}>
                                    <Typography
                                      variant='caption'
                                      color='white'
                                      className={fonts.className}
                                      sx={{ display: 'flex', flexDirection: 'column' }}
                                      data-refer={`overview-tip-${item.type}-items`}
                                    >
                                      {item.type === 'sortOrders'
                                        ? it.text
                                        : it.fields.map((v, vi) => {
                                            return (
                                              <Fragment key={vi}>
                                                <Box component={'span'} sx={{}}>
                                                  {Array.isArray(v) ? v.join('.') : v}
                                                </Box>
                                                {vi < it.fields.length - 1 && (
                                                  <Box
                                                    component={'span'}
                                                    sx={{
                                                      display: 'block',
                                                      my: 1,
                                                      borderTop: theme => `1px solid ${theme.palette.grey[800]}`
                                                    }}
                                                  ></Box>
                                                )}
                                              </Fragment>
                                            )
                                          })}
                                    </Typography>
                                    {idx < item.items.length - 1 && (
                                      <Box
                                        component={'span'}
                                        sx={{
                                          display: 'block',
                                          my: 1,
                                          borderTop: theme => `1px solid ${theme.palette.grey[800]}`
                                        }}
                                      ></Box>
                                    )}
                                  </Fragment>
                                )
                              })}
                            </Box>
                          </>
                        }
                      >
                        <ListItem sx={{ maxWidth: 140, py: 0 }} data-refer={`overview-tip-${item.type}`}>
                          <ListItemText
                            sx={{ m: 0 }}
                            primary={
                              <Box
                                sx={{
                                  display: 'flex',
                                  alignItems: 'center',
                                  color: theme => theme.palette.text.primary
                                }}
                              >
                                <Typography
                                  component={'span'}
                                  textTransform={'capitalize'}
                                  fontWeight={700}
                                  fontSize={14}
                                  sx={{ display: 'inline-block', pr: 2 }}
                                >
                                  {item.type}
                                </Typography>{' '}
                                <Box sx={{ display: 'flex', alignItems: 'center' }}>
                                  <Icon icon={item.icon} />
                                </Box>
                              </Box>
                            }
                            secondary={
                              <Box
                                component={'span'}
                                sx={{
                                  display: 'inline-block',
                                  width: '100%',
                                  overflow: 'hidden',
                                  whiteSpace: 'nowrap',
                                  textOverflow: 'ellipsis'
                                }}
                                data-refer={`overview-${item.type}-items`}
                              >
                                {item.items.map((it, idx) => {
                                  return (
                                    <Fragment key={idx}>
                                      <Typography variant='caption' className={fonts.className}>
                                        {it.fields.map(v => (Array.isArray(v) ? v.join('.') : v)).join(',')}
                                      </Typography>
                                      {idx < item.items.length - 1 && (
                                        <span className={clsx(fonts.className, 'twc-text-[12px]')}>,</span>
                                      )}
                                    </Fragment>
                                  )
                                })}
                              </Box>
                            }
                          />
                        </ListItem>
                      </CustomTooltip>
                    )
                  })}
              </Stack>
            </List>
          </Box>
        )}
      </Box>
      {!isNotNeedTableTab ? (
        <CustomTabPanel value='table' data-refer='tab-table-panel'>
          <TableView />
        </CustomTabPanel>
      ) : null}

      {isFilesetPage && (
        <CustomTabPanel value='datasetcard' data-refer='tab-datasetcard-panel'>
          <div className='twc-h-full twc-px-6 twc-pb-6 twc-overflow-hidden twc-mt-[24px]'>
            <DynamicIframe />
          </div>
        </CustomTabPanel>
      )}
      {isFilesetPage && (
        <CustomTabPanel value='filesversions' data-refer='tab-filesversions-panel'>
          <div className='twc-h-full twc-p-6'>
            <div className="twc-container twc-relative twc-flex twc-flex-col md:twc-grid md:twc-space-y-0 md:twc-grid-cols-12 twc-space-y-4 md:twc-gap-6 twc-mb-16">
              <section className="twc-border-gray-100 twc-col-span-full">
                <header className="twc-flex twc-flex-wrap twc-items-center twc-justify-start twc-pb-2 md:twc-justify-end lg:twc-flex-nowrap">
                  <div className="twc-relative twc-mr-4 twc-flex twc-min-w-0 twc-basis-auto twc-flex-wrap twc-items-center md:twc-flex-grow md:twc-basis-full lg:twc-basis-auto lg:twc-flex-nowrap">
                    <div className="SVELTE_HYDRATER contents" data-target="BranchSelector" data-props="{&quot;repoName&quot;:&quot;neuralwork/arxiver&quot;,&quot;repoType&quot;:&quot;dataset&quot;,&quot;rev&quot;:&quot;main&quot;,&quot;refs&quot;:{&quot;branches&quot;:[{&quot;name&quot;:&quot;main&quot;,&quot;ref&quot;:&quot;refs/heads/main&quot;,&quot;targetCommit&quot;:&quot;dd554f988a7b339e3405fed290e5907f31cc9adf&quot;}],&quot;tags&quot;:[],&quot;converts&quot;:[{&quot;name&quot;:&quot;duckdb&quot;,&quot;ref&quot;:&quot;refs/convert/duckdb&quot;,&quot;targetCommit&quot;:&quot;8ac0bd0fa3699b54fcca01bd17f078a21d8983f0&quot;},{&quot;name&quot;:&quot;parquet&quot;,&quot;ref&quot;:&quot;refs/convert/parquet&quot;,&quot;targetCommit&quot;:&quot;1b94f24a6a665ebc47655e198c9e513c99489812&quot;}]},&quot;view&quot;:&quot;tree&quot;}">
                      <div className="twc-relative twc-mr-4 twc-mb-2">
                        <button className="twc-text-[12px] md:twc-text-base filesversions-btn twc-w-full twc-cursor-pointer twc-text-[12px]" type="button">
                          <svg className="twc-mr-1.5 twc-text-gray-700 dark:twc-text-[rgba(156,163,175,1)]" aria-hidden="true" focusable="false" role="img" width="1em" height="1em" preserveAspectRatio="xMidYMid meet" viewBox="0 0 24 24" style={{transform: 'rotate(360deg)'}}>
                            <path d="M13 14c-3.36 0-4.46 1.35-4.82 2.24C9.25 16.7 10 17.76 10 19a3 3 0 0 1-3 3a3 3 0 0 1-3-3c0-1.31.83-2.42 2-2.83V7.83A2.99 2.99 0 0 1 4 5a3 3 0 0 1 3-3a3 3 0 0 1 3 3c0 1.31-.83 2.42-2 2.83v5.29c.88-.65 2.16-1.12 4-1.12c2.67 0 3.56-1.34 3.85-2.23A3.006 3.006 0 0 1 14 7a3 3 0 0 1 3-3a3 3 0 0 1 3 3c0 1.34-.88 2.5-2.09 2.86C17.65 11.29 16.68 14 13 14m-6 4a1 1 0 0 0-1 1a1 1 0 0 0 1 1a1 1 0 0 0 1-1a1 1 0 0 0-1-1M7 4a1 1 0 0 0-1 1a1 1 0 0 0 1 1a1 1 0 0 0 1-1a1 1 0 0 0-1-1m10 2a1 1 0 0 0-1 1a1 1 0 0 0 1 1a1 1 0 0 0 1-1a1 1 0 0 0-1-1z" fill="currentColor"></path>
                          </svg> main <svg className="twc-[-mr-1] twc-text-[rgb(107,114,128,1)]" aria-hidden="true" role="img" width="1em" height="1em" preserveAspectRatio="xMidYMid meet" viewBox="0 0 24 24"><path d="M16.293 9.293L12 13.586L7.707 9.293l-1.414 1.414L12 16.414l5.707-5.707z" fill="currentColor"></path></svg>
                        </button>
                      </div>
                    </div>
		                <div className="twc-relative twc-mb-2 twc-flex twc-flex-wrap twc-items-center">
                      <span className="twc-truncate ttwc-ext-gray-800 hover:twc-underline" href="/datasets/neuralwork/arxiver/tree/main">arxiver</span>
			              </div>
                  </div>
                  <div className="twc-mb-2 twc-flex twc-w-full twc-items-center md:twc-w-auto">
                    <span className="twc-mr-2 twc-flex-grow twc-overflow-hidden md:twc-mr-6" href="/datasets/neuralwork/arxiver/commits/main">
                      <ul className="twc-flex twc-items-center twc-overflow-hidden twc-justify-start md:twc-justify-end twc-flex-row-reverse twc-text-[12px]">
                        <li className="twc-mr-[-0.5rem] twc-h-4 twc-w-4 md:twc-h-5 md:twc-w-5 twc-block twc-flex-none twc-rounded-full twc-border-2 twc-border-white twc-bg-gradient-to-br twc-from-gray-300 twc-to-gray-100 dark:twc-border-gray-900 dark:twc-from-gray-600 dark:twc-to-gray-800" title="adirik" style={{'content-visibility': 'auto'}}>
                          <img className="twc-overflow-hidden twc-rounded-full twc-max-w-full" alt="" src="https://cdn-avatars.huggingface.co/v1/production/uploads/1678118185856-629dffc1efe7b818408189b0.jpeg" />
			                  </li>
                        <li className="twc-mr-[-0.5rem] twc-h-4 twc-w-4 md:twc-h-5 md:twc-w-5 twc-block twc-flex-none twc-rounded-full twc-border-2 twc-border-white twc-bg-gradient-to-br twc-from-gray-300 twc-to-gray-100 dark:twc-border-gray-900 dark:twc-from-gray-600 dark:twc-to-gray-800" title="alikanakar" style={{'content-visibility': 'auto'}}>
                          <img className="twc-overflow-hidden twc-rounded-full twc-max-w-full" alt="" src="https://cdn-avatars.huggingface.co/v1/production/uploads/6329b0cabdb6242b42b8cd63/-4MfhpDNSGw15DTPTAiWi.jpeg" />
			                  </li>
		                    <li className="twc-text-gray-600 hover:twc-text-gray-700 twc-order-first twc-ml-3">
                          <span>2 contributors</span>
                        </li>
                      </ul>
                    </span>
			              <span className="filesversions-btn twc-group twc-mr-0 twc-flex-grow-0 twc-cursor-pointer twc-rounded-full twc-text-[12px] md:twc-px-4 md:twc-text-base" href="/datasets/neuralwork/arxiver/commits/main">
                      <svg className="twc-mr-1" aria-hidden="true" focusable="false" role="img" width="1em" height="1em" preserveAspectRatio="xMidYMid meet" viewBox="0 0 32 32" style={{transform: 'rotate(360deg)'}}>
                        <path d="M16 4C9.383 4 4 9.383 4 16s5.383 12 12 12s12-5.383 12-12S22.617 4 16 4zm0 2c5.535 0 10 4.465 10 10s-4.465 10-10 10S6 21.535 6 16S10.465 6 16 6zm-1 2v9h7v-2h-5V8z" fill="currentColor"></path>
                      </svg>
				              <span className="twc-mr-1 twc-text-gray-600">History:</span>
					            <span className="group-hover:twc-underline">9 commits</span>
                    </span>
                  </div>
	              </header>
				        <div className="SVELTE_HYDRATER contents" data-target="UnsafeBanner" data-props="{&quot;classNames&quot;:&quot;mb-3&quot;,&quot;repoId&quot;:&quot;neuralwork/arxiver&quot;,&quot;repoType&quot;:&quot;dataset&quot;,&quot;revision&quot;:&quot;main&quot;,&quot;securityRepoStatus&quot;:{&quot;scansDone&quot;:false,&quot;filesWithIssues&quot;:[]}}"></div>
				        <div className="SVELTE_HYDRATER contents" data-target="LastCommit" data-props="{&quot;commitLast&quot;:{&quot;date&quot;:&quot;2024-10-23T22:17:12.000Z&quot;,&quot;verified&quot;:&quot;verified&quot;,&quot;subject&quot;:&quot;Update readme&quot;,&quot;authors&quot;:[{&quot;_id&quot;:&quot;629dffc1efe7b818408189b0&quot;,&quot;avatar&quot;:&quot;https://cdn-avatars.huggingface.co/v1/production/uploads/1678118185856-629dffc1efe7b818408189b0.jpeg&quot;,&quot;isHf&quot;:false,&quot;user&quot;:&quot;adirik&quot;}],&quot;commit&quot;:{&quot;id&quot;:&quot;dd554f988a7b339e3405fed290e5907f31cc9adf&quot;,&quot;parentIds&quot;:[&quot;f45458526413c03c63fd9f8ef5114a2b8f3e5536&quot;]},&quot;title&quot;:&quot;Update readme&quot;},&quot;repo&quot;:{&quot;name&quot;:&quot;neuralwork/arxiver&quot;,&quot;type&quot;:&quot;dataset&quot;}}">
                  <div className="from-gray-100-to-white twc-flex twc-items-baseline twc-rounded-t-lg twc-border twc-border-solid twc-border-[#e5e7eb] twc-border-b-0 bg-gradient-to-t twc-px-3 twc-py-2 dark:twc-border-gray-800">
                    <img className="twc-mr-2.5 twc-mt-0.5 twc-h-4 twc-w-4 twc-self-center twc-rounded-full" alt="adirik's picture" src="https://cdn-avatars.huggingface.co/v1/production/uploads/1678118185856-629dffc1efe7b818408189b0.jpeg" />
                    <div className="twc-mr-4 twc-flex twc-flex-none twc-items-center twc-truncate">
                      <span className="hover:twc-underline" href="/adirik">adirik</span>
                    </div>
                    <div className="twc-mr-4 twc-truncate twc-font-mono twc-text-[14px] twc-text-[rgb(107,114,128,1)] hover:twc-prose-a:underline">
                      <span href="/datasets/neuralwork/arxiver/commit/dd554f988a7b339e3405fed290e5907f31cc9adf">Update readme</span>
                    </div>
                    <span className="twc-rounded twc-border twc-bg-gray-50 twc-px-1.5 twc-text-[12px] hover:twc-underline dark:twc-border-gray-800 dark:twc-bg-gray-900" href="/datasets/neuralwork/arxiver/commit/dd554f988a7b339e3405fed290e5907f31cc9adf">dd554f9</span>
                    <span className="twc-mx-2 twc-text-[rgb(16,185,129)] dark:twc-text-green-600 twc-px-1.5 twc-border-solid twc-border-[rgb(209,250,229)] dark:twc-border-green-800 twc-rounded-full twc-border twc-text-[12px] twc-uppercase" title="This commit is signed and the signature is verified">verified</span>
                    <time className="twc-ml-auto twc-hidden twc-flex-none twc-truncate twc-pl-2 twc-text-[rgb(107,114,128,1)] dark:twc-text-[rgba(156,163,175,1)] lg:twc-block" datetime="2024-10-23T22:17:12" title="Wed, 23 Oct 2024 22:17:12 GMT">6 days ago</time>
                  </div>
                </div>
				        <div className="SVELTE_HYDRATER contents" data-target="ViewerIndexTreeList"> 
                  <ul className="twc-mb-8 twc-mt-0 twc-p-0 twc-rounded-b-lg twc-border twc-border-t-0 twc-border-solid twc-border-[#e5e7eb] dark:twc-border-gray-800 dark:twc-bg-gray-900">
                    <li className="twc-grid twc-h-10 twc-grid-cols-12 twc-place-content-center twc-gap-x-3 twc-border-solid twc-border-0 twc-border-t twc-border-[#e5e7eb] twc-px-3 dark:twc-border-gray-800">
                      <span className="twc-col-span-6 twc-flex twc-items-center hover:twc-underline md:twc-col-span-5 lg:twc-col-span-4" href="/datasets/neuralwork/arxiver/tree/main/data">
                        <svg className="twc-flex-none twc-mr-2 twc-text-[rgb(96,165,250,1)] twc-fill-current" aria-hidden="true" focusable="false" role="img" width="1em" height="1em" preserveAspectRatio="xMidYMid meet" viewBox="0 0 24 24" style={{transform: 'rotate(360deg)'}}>
                          <path d="M10 4H4c-1.11 0-2 .89-2 2v12a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2h-8l-2-2z" fill="currentColor"></path>
                        </svg>
                        <span className="twc-truncate">data</span>
                      </span>
                      <div className="twc-col-span-1 md:twc-col-span-2"></div>
                      <span className="twc-col-span-3 twc-text-[14px] twc-text-[rgba(156,163,175,1)] hover:twc-underline md:twc-col-span-3 md:twc-flex lg:twc-col-span-4" href="/datasets/neuralwork/arxiver/commit/f45458526413c03c63fd9f8ef5114a2b8f3e5536">
                        <span className="twc-truncate">filter dataset based on licenses</span>
                      </span>
                      <span className="twc-col-span-2 twc-truncate twc-text-[14px] twc-text-right twc-text-[rgba(156,163,175,1)] md:twc-block" href="/datasets/neuralwork/arxiver/commit/f45458526413c03c63fd9f8ef5114a2b8f3e5536">
                        <time datetime="2024-10-23T22:11:25" title="Wed, 23 Oct 2024 22:11:25 GMT">6 days ago</time>
                      </span>
                    </li> 
                    <li className="twc-relative twc-grid twc-h-10 twc-grid-cols-12 twc-place-content-center twc-gap-x-3 twc-border-solid twc-border-0 twc-border-t twc-border-[#e5e7eb] twc-px-3 dark:twc-border-gray-800">
                      <div className="twc-col-span-6 twc-flex twc-items-center md:twc-col-span-4">
                        <span className="twc-group twc-flex twc-items-center twc-truncate" href="/datasets/neuralwork/arxiver/blob/main/.gitattributes">
                          <svg className="twc-flex-none twc-mr-2 twc-text-gray-300 twc-fill-current" aria-hidden="true" focusable="false" role="img" width="1em" height="1em" preserveAspectRatio="xMidYMid meet" viewBox="0 0 32 32">
                            <path d="M25.7 9.3l-7-7A.908.908 0 0 0 18 2H8a2.006 2.006 0 0 0-2 2v24a2.006 2.006 0 0 0 2 2h16a2.006 2.006 0 0 0 2-2V10a.908.908 0 0 0-.3-.7zM18 4.4l5.6 5.6H18zM24 28H8V4h8v6a2.006 2.006 0 0 0 2 2h6z" fill="currentColor"></path>
                          </svg>
                          <span className="twc-truncate group-hover:twc-underline">.gitattributes</span>
                        </span>
                        <div className="sm:twc-relative twc-ml-1.5">
                          <button className="twc-flex filesversions-btn twc-h-[1.125rem] twc-select-none twc-items-center twc-gap-0.5 twc-rounded twc-border twc-pl-0.5 twc-pr-0.5 twc-text-xs twc-leading-tight twc-text-[rgba(156,163,175,1)] hover:twc-cursor-pointer twc-text-[rgba(156,163,175,1)] hover:twc-border-gray-200 hover:twc-bg-gray-50 hover:twc-text-[rgb(107,114,128,1)] dark:twc-border-gray-800 dark:hover:twc-bg-gray-800 dark:hover:twc-text-gray-200 twc-translate-y-px">
                            <svg className="twc-flex-none" width="1em" height="1em" viewBox="0 0 22 28" fill="none" xmlns="http://www.w3.org/2000/svg">
                              <path fill-rule="evenodd" clip-rule="evenodd" d="M15.3634 10.3639C15.8486 10.8491 15.8486 11.6357 15.3634 12.1209L10.9292 16.5551C10.6058 16.8785 10.0814 16.8785 9.7579 16.5551L7.03051 13.8277C6.54532 13.3425 6.54532 12.5558 7.03051 12.0707C7.51569 11.5855 8.30234 11.5855 8.78752 12.0707L9.7579 13.041C10.0814 13.3645 10.6058 13.3645 10.9292 13.041L13.6064 10.3639C14.0916 9.8787 14.8782 9.8787 15.3634 10.3639Z" fill="currentColor"></path>
                              <path fill-rule="evenodd" clip-rule="evenodd" d="M10.6666 27.12C4.93329 25.28 0 19.2267 0 12.7867V6.52001C0 5.40001 0.693334 4.41334 1.73333 4.01334L9.73333 1.01334C10.3333 0.786673 11 0.786673 11.6 1.02667L19.6 4.02667C20.1083 4.21658 20.5465 4.55701 20.8562 5.00252C21.1659 5.44803 21.3324 5.97742 21.3333 6.52001V12.7867C21.3333 19.24 16.4 25.28 10.6666 27.12Z" fill="currentColor" fill-opacity="0.22"></path>
                              <path d="M10.0845 1.94967L10.0867 1.94881C10.4587 1.8083 10.8666 1.81036 11.2286 1.95515L11.2387 1.95919L11.2489 1.963L19.2489 4.963L19.25 4.96342C19.5677 5.08211 19.8416 5.29488 20.0351 5.57333C20.2285 5.85151 20.3326 6.18203 20.3333 6.52082C20.3333 6.52113 20.3333 6.52144 20.3333 6.52176L20.3333 12.7867C20.3333 18.6535 15.8922 24.2319 10.6666 26.0652C5.44153 24.2316 1 18.6409 1 12.7867V6.52001C1 5.82357 1.42893 5.20343 2.08883 4.94803L10.0845 1.94967Z" stroke="currentColor" stroke-opacity="0.30" stroke-width="2"></path>
                            </svg>
                            <span className="twc-mr-0.5 twc-text-[12px] max-sm:twc-hidden">Safe</span>
                          </button>
                        </div>
                      </div>
                      <span className="twc-group twc-col-span-1 twc-flex twc-items-center twc-justify-self-end twc-truncate twc-text-right twc-font-mono twc-text-[0.8rem] twc-leading-6 twc-text-[rgba(156,163,175,1)] md:twc-col-span-3 lg:twc-col-span-2 xl:twc-pr-10" title="Download file" download="" href="/datasets/neuralwork/arxiver/resolve/main/.gitattributes?download=true">
                        <span className="twc-truncate max-sm:twc-text-xs">2.42 kB</span>
                        <div className="twc-ml-2 twc-flex twc-h-5 twc-w-5 twc-items-center twc-justify-center twc-rounded twc-border twc-border-solid twc-border-[#e5e7eb] twc-text-[rgb(107,114,128,1)] group-hover:twc-bg-gray-50 group-hover:twc-text-gray-800 group-hover:twc-shadow-sm dark:twc-border-gray-800 dark:group-hover:twc-bg-gray-800 dark:group-hover:twc-text-gray-300 xl:twc-ml-4">
                          <svg aria-hidden="true" focusable="false" role="img" width="1em" height="1em" viewBox="0 0 32 32">
                            <path fill="currentColor" d="M26 24v4H6v-4H4v4a2 2 0 0 0 2 2h20a2 2 0 0 0 2-2v-4zm0-10l-1.41-1.41L17 20.17V2h-2v18.17l-7.59-7.58L6 14l10 10l10-10z"></path>
                          </svg>
                        </div>
                      </span>
                      <span className="twc-col-span-3 twc-items-center twc-font-mono twc-text-[14px] twc-text-[rgba(156,163,175,1)] hover:twc-underline md:twc-col-span-3 md:twc-flex lg:twc-col-span-4" href="/datasets/neuralwork/arxiver/commit/dd011079f2522891fc6898b2c2e1bcb58481bd69">
                        <span className="truncate">initial commit</span>
                      </span>
                      <span className="twc-col-span-2 twc-truncate twc-text-[14px] twc-text-right twc-text-[rgba(156,163,175,1)] md:twc-block" href="/datasets/neuralwork/arxiver/commit/dd011079f2522891fc6898b2c2e1bcb58481bd69">
                        <time datetime="2024-10-14T12:21:14" title="Mon, 14 Oct 2024 12:21:14 GMT">16 days ago</time>
                      </span>
                    </li>
                    <li className="twc-relative twc-grid twc-h-10 twc-grid-cols-12 twc-place-content-center twc-gap-x-3 twc-border-solid twc-border-0 twc-border-t twc-border-[#e5e7eb] twc-px-3 dark:twc-border-gray-800">
                      <div className="twc-col-span-6 twc-flex twc-items-center md:twc-col-span-4">
                        <span className="twc-group twc-flex twc-items-center twc-truncate" href="/datasets/neuralwork/arxiver/blob/main/README.md">
                          <svg className="twc-flex-none twc-mr-2 twc-text-gray-300 twc-fill-current" aria-hidden="true" focusable="false" role="img" width="1em" height="1em" preserveAspectRatio="xMidYMid meet" viewBox="0 0 32 32">
                            <path d="M25.7 9.3l-7-7A.908.908 0 0 0 18 2H8a2.006 2.006 0 0 0-2 2v24a2.006 2.006 0 0 0 2 2h16a2.006 2.006 0 0 0 2-2V10a.908.908 0 0 0-.3-.7zM18 4.4l5.6 5.6H18zM24 28H8V4h8v6a2.006 2.006 0 0 0 2 2h6z" fill="currentColor"></path>
                          </svg>
                          <span className="twc-truncate group-hover:twc-underline">README.md</span>
                        </span>
                        <div className="sm:twc-relative twc-ml-1.5">
                          <button className="twc-flex filesversions-btn twc-h-[1.125rem] twc-select-none twc-items-center twc-gap-0.5 twc-rounded twc-border twc-pl-0.5 twc-pr-0.5 twc-text-xs twc-leading-tight twc-text-[rgba(156,163,175,1)] hover:twc-cursor-pointer twc-text-[rgba(156,163,175,1)] hover:twc-border-gray-200 hover:twc-bg-gray-50 hover:twc-text-[rgb(107,114,128,1)] dark:twc-border-gray-800 dark:hover:twc-bg-gray-800 dark:hover:twc-text-gray-200 twc-translate-y-px">
                            <svg className="flex-none" width="1em" height="1em" viewBox="0 0 22 28" fill="none" xmlns="http://www.w3.org/2000/svg">
                              <path fill-rule="evenodd" clip-rule="evenodd" d="M15.3634 10.3639C15.8486 10.8491 15.8486 11.6357 15.3634 12.1209L10.9292 16.5551C10.6058 16.8785 10.0814 16.8785 9.7579 16.5551L7.03051 13.8277C6.54532 13.3425 6.54532 12.5558 7.03051 12.0707C7.51569 11.5855 8.30234 11.5855 8.78752 12.0707L9.7579 13.041C10.0814 13.3645 10.6058 13.3645 10.9292 13.041L13.6064 10.3639C14.0916 9.8787 14.8782 9.8787 15.3634 10.3639Z" fill="currentColor"></path>
                              <path fill-rule="evenodd" clip-rule="evenodd" d="M10.6666 27.12C4.93329 25.28 0 19.2267 0 12.7867V6.52001C0 5.40001 0.693334 4.41334 1.73333 4.01334L9.73333 1.01334C10.3333 0.786673 11 0.786673 11.6 1.02667L19.6 4.02667C20.1083 4.21658 20.5465 4.55701 20.8562 5.00252C21.1659 5.44803 21.3324 5.97742 21.3333 6.52001V12.7867C21.3333 19.24 16.4 25.28 10.6666 27.12Z" fill="currentColor" fill-opacity="0.22"></path>
                              <path d="M10.0845 1.94967L10.0867 1.94881C10.4587 1.8083 10.8666 1.81036 11.2286 1.95515L11.2387 1.95919L11.2489 1.963L19.2489 4.963L19.25 4.96342C19.5677 5.08211 19.8416 5.29488 20.0351 5.57333C20.2285 5.85151 20.3326 6.18203 20.3333 6.52082C20.3333 6.52113 20.3333 6.52144 20.3333 6.52176L20.3333 12.7867C20.3333 18.6535 15.8922 24.2319 10.6666 26.0652C5.44153 24.2316 1 18.6409 1 12.7867V6.52001C1 5.82357 1.42893 5.20343 2.08883 4.94803L10.0845 1.94967Z" stroke="currentColor" stroke-opacity="0.30" stroke-width="2"></path>
                            </svg>
                            <span className="twc-mr-0.5 twc-text-[12px] max-sm:twc-hidden">Safe</span>
                          </button>
                        </div>
                      </div>
                      <span className="twc-group twc-col-span-1 twc-flex twc-items-center twc-justify-self-end twc-truncate twc-text-right twc-font-mono twc-text-[0.8rem] twc-leading-6 twc-text-[rgba(156,163,175,1)] md:twc-col-span-3 lg:twc-col-span-2 xl:twc-pr-10" title="Download file" download="" href="/datasets/neuralwork/arxiver/resolve/main/README.md?download=true">
                      <span className="twc-truncate max-sm:twc-text-xs">2.06 kB</span>  
                      <div className="twc-ml-2 twc-flex twc-h-5 twc-w-5 twc-items-center twc-justify-center twc-rounded twc-border twc-border-solid twc-border-[#e5e7eb] twc-text-[rgb(107,114,128,1)] group-hover:twc-bg-gray-50 group-hover:twc-text-gray-800 group-hover:twc-shadow-sm dark:twc-border-gray-800 dark:group-hover:twc-bg-gray-800 dark:group-hover:twc-text-gray-300 xl:twc-ml-4">
                        <svg aria-hidden="true" focusable="false" role="img" width="1em" height="1em" viewBox="0 0 32 32">
                          <path fill="currentColor" d="M26 24v4H6v-4H4v4a2 2 0 0 0 2 2h20a2 2 0 0 0 2-2v-4zm0-10l-1.41-1.41L17 20.17V2h-2v18.17l-7.59-7.58L6 14l10 10l10-10z"></path>
                        </svg>
                      </div>
                      </span>
                      <span className="twc-col-span-3 twc-items-center  twc-text-[rgba(156,163,175,1)] hover:twc-underline md:twc-col-span-3 md:twc-flex lg:twc-col-span-4" href="/datasets/neuralwork/arxiver/commit/dd554f988a7b339e3405fed290e5907f31cc9adf">
                        <span className="twc-truncate twc-text-[14px]">Update readme</span>
                      </span>
                      <span className="twc-col-span-2 twc-truncate twc-text-[14px] twc-text-right twc-text-[rgba(156,163,175,1)] md:twc-block" href="/datasets/neuralwork/arxiver/commit/dd554f988a7b339e3405fed290e5907f31cc9adf">
                        <time datetime="2024-10-23T22:17:12" title="Wed, 23 Oct 2024 22:17:12 GMT">6 days ago</time>
                      </span>
                    </li>
                  </ul>
                </div>
              </section>
            </div>
          </div>
        </CustomTabPanel>
      )}

      <CustomTabPanel value='details' data-refer='tab-details-panel'>
        <DetailsView />
      </CustomTabPanel>
    </TabContext>
  )
}

export default TabsContent
