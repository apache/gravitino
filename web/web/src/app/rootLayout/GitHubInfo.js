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
import Image from 'next/image'
import { Box, Typography } from '@mui/material'
import { Star } from '@mui/icons-material'

const GitHubInfo = ({ stars, forks, username, repository }) => {
  const githubUrl = `https://github.com/${username}/${repository}`
  const githubForkUrl = `https://github.com/${username}/${repository}/fork`
  const githubLogoUrl = (process.env.NEXT_PUBLIC_BASE_PATH ?? '') + '/icons/github-mark.svg'
  const forkLogoUrl = (process.env.NEXT_PUBLIC_BASE_PATH ?? '') + '/icons/git-fork.svg'

  return (
    <Box className={'twc-flex  twc-gap-x-3 twc-bg-customs-lightBg twc-px-3 twc-py-2 twc-rounded-full'}>
      <Link href={githubUrl}>
        <Image className={'twc-align-middle'} src={githubLogoUrl} width={24} height={24} alt='logo' />
      </Link>
      <Box className={'twc-flex twc-items-center twc-gap-x-3 twc-ml-2'}>
        <Link href={githubForkUrl} className={'twc-no-underline  twc-bg-customs-dark twc-rounded-full '}>
          <Typography
            className={
              'twc-flex twc-items-center twc-gap-2 twc-text-customs-black twc-px-2.5 twc-py-1 twc-text-[0.75rem] twc-font-bold'
            }
          >
            <Image
              className={'twc-align-middle twc-text-customs-white'}
              src={forkLogoUrl}
              width={24}
              height={24}
              alt='logo'
            />
            {forks} Forks
          </Typography>
        </Link>
        <Link href={githubUrl} className={'twc-no-underline  twc-bg-customs-dark twc-rounded-full '}>
          <Typography
            className={
              'twc-flex twc-items-center twc-gap-2 twc-text-customs-black twc-px-2.5 twc-py-1 twc-text-[0.75rem] twc-font-bold'
            }
          >
            <Star className={'twc-text-customs-black'} />
            {stars} Stars
          </Typography>
        </Link>
      </Box>
    </Box>
  )
}

export default GitHubInfo
