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

import Link from 'next/link'
import { Dropdown } from 'antd'
import { useAppSelector } from '@/lib/hooks/useStore'
import Icons from '@/components/Icons'

const GitHubInfo = () => {
  const githubUrl = 'https://github.com/apache/gravitino'
  const githubForkUrl = 'https://github.com/apache/gravitino/fork'
  const githubLogoUrl = (process.env.NEXT_PUBLIC_BASE_PATH ?? '') + '/icons/github-mark.svg'
  const forkLogoUrl = (process.env.NEXT_PUBLIC_BASE_PATH ?? '') + '/icons/git-fork.svg'
  const store = useAppSelector(state => state.sys)

  // Hide component if GitHub API request failed
  if (store.githubError) {
    return null
  }

  const dropdownItems = [
    {
      key: 'fork',
      label: (
        <a
          href={githubForkUrl}
          target='_blank'
          rel='noopener noreferrer'
          className='no-underline flex items-center justify-center'
        >
          <Icons.GitFork className={'text-customs-black size-4'} />
          <span className='ml-2'>{store.forks} Forks</span>
        </a>
      )
    },
    {
      key: 'stars',
      label: (
        <a
          href={githubUrl}
          target='_blank'
          rel='noopener noreferrer'
          className='no-underline flex items-center justify-center'
        >
          <Icons.Star className={'text-customs-black size-4'} />
          <span className='ml-2'>{store.stars} Stars</span>
        </a>
      )
    }
  ]

  return (
    <Dropdown menu={{ items: dropdownItems }} trigger={['hover']} placement='bottomLeft'>
      <div role={'button'} tabIndex={0} className='cursor-pointer rounded p-1.5 hover:bg-slate-700'>
        <Icons.iconify icon='octicon:mark-github-24' className='size-6' />
      </div>
    </Dropdown>
  )
}

export default GitHubInfo
