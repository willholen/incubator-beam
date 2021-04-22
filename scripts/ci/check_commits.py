# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import sys
import requests

fixup_words = 'fixup', 'typo', 'lint', 'reviewer', 'spotless', 'mypy', 'yapf'


def main(url):
    url = sys.argv[-1]
    info = requests.get(url).json()
    if is_approved(info):
        print(merge_advice(requests.get(url + '/commits').json()))


def is_approved(info):
    for review in requests.get(info['_links']['self']['href'] +
                               '/reviews').json():
        if review['state'] == 'APPROVED':
            return True
        elif 'LGTM' in review['body']:
            return True
    for comment in requests.get(info['_links']['comments']['href']).json():
        if 'LGTM' in comment['body']:
            return True


def merge_advice(commits):
    if len(commits) == 1:
        return "In it's infinite wisdom, squashbot recommends the merge option."
    fixup_commits = sum(is_fixup_commit(c) for c in commits)
    if fixup_commits:
        return "Looks like there are some commits fixup commits. Squash and merge?"
    elif len(commits) > 5:
        return "That's a lot of commits, is squash and merge the right option?"
    else:
        return "Consider using the merge button on this one."


def is_fixup_commit(commit):
    msg = commit['commit']['message'].lower()
    return any(word in msg for word in fixup_words)


if __name__ == '__main__':
    main(sys.argv[-1])
