#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

function choose_from_menu() {
    local prompt="$1" outvar="$2"
    shift
    shift
    local options=("$@") cur=0 count=${#options[@]} index=0
    local esc=$(printf '\033') # cache ESC as test doesn't allow esc codes
    printf "$prompt\n"
    while true
    do
        # list all options (option list is zero-based)
        index=0 
        for o in "${options[@]}"
        do
            if [ "$index" == "$cur" ]
            then printf " >\033[7m%s\033[0m\n" "$o" # mark & highlight the current option
            else printf "  %s\n" "$o"
            fi
            index=$(( $index + 1 ))
        done
        read -s -n3 key # wait for user to key in arrows or ENTER
        if [[ $key == $esc[A ]] # up arrow
        then cur=$(( $cur - 1 ))
            [ "$cur" -lt 0 ] && cur=0
        elif [[ $key == $esc[B ]] # down arrow
        then cur=$(( $cur + 1 ))
            [ "$cur" -ge $count ] && cur=$(( $count - 1 ))
        elif [[ $key == "" ]] # nothing, i.e the read delimiter - ENTER
        then break
        fi
        printf "\033[${count}A" # go up to the beginning to re-render
    done
    # export the selection to the requested output variable
    printf -v $outvar "$cur"
}

### Define build options  
buildOptions=(
"Only compile and skip the unit test and Integration test:  ./gradlew clean build -x test"
"Only execute the unit test and skip Integration test:  ./gradlew test -PskipITs"
"Run the integration tests in embedded mode: ./gradlew build -x test && ./gradlew test -PskipTests -PtestMode=embedded"
"Run the integration tests in deploy mode: ./gradlew build -x test && ./gradlew compileDistribution && ./gradlew test -PskipTests -PtestMode=deploy"
"Exit"
)

choose_from_menu "Decide the Building Option" selectedOption "${buildOptions[@]}"
echo "Selected option: ${buildOptions[$selectedOption]}"

case $selectedOption in
    "0")
        echo "excuting ./gradlew clean build -x test"
        ./gradlew clean build -x test
        ;;
    "1")
        echo "excuting ./gradlew test -PskipITs"
        ./gradlew test -PskipITs
        ;;
    "2")
        echo "excuting ./gradlew build -x test && ./gradlew test -PskipTests -PtestMode=embedded"
        ./gradlew build -x test && ./gradlew test -PskipTests -PtestMode=embedded
        ;;
    "3")
        echo "excuting ./gradlew build -x test && ./gradlew compileDistribution && ./gradlew test -PskipTests -PtestMode=deploy"
        ./gradlew build -x test && ./gradlew compileDistribution && ./gradlew test -PskipTests -PtestMode=deploy
        ;;
    "4")
        echo "Exiting without building."
        exit 0
        ;;
esac

