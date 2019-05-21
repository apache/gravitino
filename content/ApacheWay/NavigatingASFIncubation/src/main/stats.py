#
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
#

# Generate statistics from whimsey JSON file to update slides
# run ike so:
# python3 stats.py > asciidoc/projectstats.adoc

import urllib.request, json

print("""
////

  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

////
""")

with urllib.request.urlopen("https://whimsy.apache.org/public/committee-info.json") as url:
    data = json.loads(url.read().decode())
    pmcs = data["committees"]
    count = 0
    for committee in pmcs:
        if pmcs[committee]["pmc"]:
            count = count + 1
    print(":pmcs: " + str(count))

    ipmcs = pmcs["incubator"]["roster"]
    print(":ipmcs: " + str(len(ipmcs)))

    chair = pmcs["incubator"]["chair"]
    for apachename in chair:
         print(":ipmc_chair: " + str(chair[apachename]["name"])) 

with urllib.request.urlopen("https://whimsy.apache.org/public/pods-scan.json") as url:
    pods = json.loads(url.read().decode())
    print(":podlings: " + str(len(pods)))

with urllib.request.urlopen("https://whimsy.apache.org/public/member-info.json") as url:
    data = json.loads(url.read().decode())
    members = data["members"]
    print(":members: " + str(len(members)))

with urllib.request.urlopen("https://whimsy.apache.org/public/public_ldap_people.json") as url:
    data = json.loads(url.read().decode())
    people = data["people"]
    print(":committers: " + str(len(people)))
