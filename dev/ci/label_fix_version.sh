#!/usr/bin/env bash
#
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
#
# Labels issues with the release their fix ships in. Used by
# .github/workflows/label-fix-version.yml.
#
#   label_fix_version.sh merge  A PR was merged. Label its issues with the
#                               release of the merge commit, and assign the
#                               PR and its issues to the PR author if they
#                               have no assignee.
#                               Env: REPO, PR_NUMBER, PR_TITLE, PR_BODY,
#                               MERGE_SHA, PR_AUTHOR, PR_AUTHOR_TYPE.
#   label_fix_version.sh rc     A release candidate tag was pushed. Move the
#                               issues fixed since the previous RC to this
#                               release. Needs full git history.
#                               Env: REPO, TAG.
#
# Issues are taken from "[#N]" in the PR title or commit subject, e.g.
# "[#1][#2]" or "[#1, #2]", and from closing keywords in the body, e.g.
# "Fix: #1". Set DRY_RUN=true to print writes instead of running them.

set -euo pipefail
# Fail on errors inside command substitutions too (bash >= 4.4). The API calls
# below also handle failures explicitly, so older bash still fails loudly.
shopt -s inherit_errexit 2> /dev/null || true

: "${REPO:?REPO must be set}"
DRY_RUN=${DRY_RUN:-false}

write() {
  if [ "$DRY_RUN" = "true" ]; then
    echo "[dry-run] $*"
  else
    "$@"
  fi
}

# Prints the issue ids referenced by a subject and a body, one per line.
extract_issues() {
  local subject=$1 body=$2
  {
    printf '%s\n' "$subject" \
      | grep -oE '\[[[:space:]]*#[0-9]+([[:space:]]*,?[[:space:]]*#[0-9]+)*[[:space:]]*\]' \
      | grep -oE '#[0-9]+' | tr -d '#' || true
    printf '%s\n' "$body" \
      | grep -oiE '(^|[^[:alnum:]_])(close[sd]?|fix(e[sd])?|resolve[sd]?):?[[:space:]]+#[0-9]+' \
      | grep -oE '[0-9]+$' || true
  } | sort -un
}

# Reads gradle.properties from stdin and prints the version without -SNAPSHOT.
parse_version() {
  sed -n 's/^version[[:space:]]*=[[:space:]]*//p' | tr -d '[:space:]' | sed 's/-SNAPSHOT$//'
}

is_version() {
  printf '%s' "$1" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+$'
}

version_lt() {
  [ "$1" != "$2" ] && [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | head -1)" = "$1" ]
}

# Creates the version label if missing, following the existing convention.
ensure_label() {
  local version=$1
  if ! gh label list --repo "$REPO" --search "$version" --limit 100 --json name --jq '.[].name' \
      | grep -qxF "$version"; then
    write gh label create "$version" --repo "$REPO" \
      --description "Release v$version" --color "$(openssl rand -hex 3)" \
      || echo "Label $version may already exist."
  fi
}

# Prints the release that contains the given merge commit.
release_of_commit() {
  local sha=$1 version tags tag status
  version=$(gh api "repos/$REPO/contents/gradle.properties?ref=$sha" --jq '.content' \
    | base64 -d | parse_version) || return 1
  if ! is_version "$version"; then
    echo "Unexpected version '$version' at $sha." >&2
    return 1
  fi

  # The release script bumps to the next SNAPSHOT right after tagging an RC,
  # so a commit is in an earlier release only if a later RC of that release
  # was cut after it. Check the latest RC of each earlier release on this line.
  tags=$(gh api --paginate "repos/$REPO/git/matching-refs/tags/v${version%.*}." \
      --jq '.[].ref | sub("^refs/tags/"; "") | select(test("-rc[0-9]+$"))' \
    | sort -V | awk -F'-rc' '!($1 in last) { order[++n] = $1 } { last[$1] = $0 }
        END { for (i = 1; i <= n; i++) print last[order[i]] }') || return 1
  for tag in $tags; do
    local tag_version=${tag#v}
    tag_version=${tag_version%-rc*}
    version_lt "$tag_version" "$version" || continue
    status=$(gh api "repos/$REPO/compare/$tag...$sha" --jq '.status') || return 1
    if [ "$status" = "behind" ] || [ "$status" = "identical" ]; then
      echo "$tag_version"
      return 0
    fi
  done
  echo "$version"
}

on_merge() {
  : "${PR_NUMBER:?}" "${PR_TITLE:?}" "${MERGE_SHA:?}" "${PR_AUTHOR:?}" "${PR_AUTHOR_TYPE:?}"
  local issues version issue info is_pr assignee_count
  assignee_count=$(gh api "repos/$REPO/issues/$PR_NUMBER" --jq '.assignees | length')
  assign_author_if_unassigned "$PR_NUMBER" "$assignee_count"

  issues=$(extract_issues "$PR_TITLE" "${PR_BODY:-}")
  if [ -z "$issues" ]; then
    echo "No linked issue found, skipping."
    return 0
  fi

  version=$(release_of_commit "$MERGE_SHA")
  echo "Merge commit $MERGE_SHA ships in $version."
  ensure_label "$version"

  for issue in $issues; do
    info=$(gh api "repos/$REPO/issues/$issue" --jq '"\(has("pull_request")) \(.assignees | length)"')
    read -r is_pr assignee_count <<< "$info"
    if [ "$is_pr" != "false" ]; then
      echo "#$issue is a pull request, skipping."
      continue
    fi
    write gh issue edit "$issue" --repo "$REPO" --add-label "$version"
    echo "Labeled issue #$issue with $version."
    assign_author_if_unassigned "$issue" "$assignee_count"
  done
}

# Assigns an issue or PR to the PR author if it has no assignee yet.
assign_author_if_unassigned() {
  local number=$1 assignee_count=$2 assigned
  if [ "$assignee_count" != "0" ]; then
    return 0
  fi
  if [ "$PR_AUTHOR_TYPE" = "Bot" ]; then
    echo "PR author $PR_AUTHOR is a bot, not assigning #$number."
    return 0
  fi
  if [ "$DRY_RUN" = "true" ]; then
    echo "[dry-run] assign #$number to $PR_AUTHOR"
    return 0
  fi
  # GitHub silently drops assignees it doesn't accept, so check the result.
  assigned=$(gh api -X POST "repos/$REPO/issues/$number/assignees" -f "assignees[]=$PR_AUTHOR" \
    --jq '[.assignees[].login] | index(env.PR_AUTHOR) != null')
  if [ "$assigned" = "true" ]; then
    echo "Assigned #$number to $PR_AUTHOR."
  else
    echo "Could not assign #$number to $PR_AUTHOR."
  fi
}

# Prints the issues referenced by the commits in the given git log range.
# Each commit is resolved to the PR that merged it, the same source the
# merge job uses, since rebase-merged commits don't carry the PR title.
# Commits pushed without a PR fall back to their own message.
issues_in_range() {
  local shas sha prs pr json
  local seen_prs=" "
  shas=$(git rev-list "$@") || return 1
  # The loop runs in a pipeline subshell, so a failed API call exits it and
  # pipefail fails the caller instead of falling back to the commit message.
  for sha in $shas; do
    json=$(gh api "repos/$REPO/commits/$sha/pulls" --jq '[.[] | select(.merged_at != null)]') \
      || exit 1
    prs=$(jq -r '.[].number' <<< "$json")
    if [ -z "$prs" ]; then
      extract_issues "$(git log -1 --format=%s "$sha")" "$(git log -1 --format=%b "$sha")"
      continue
    fi
    for pr in $prs; do
      [[ "$seen_prs" == *" $pr "* ]] && continue
      seen_prs="$seen_prs$pr "
      extract_issues "$(jq -r --argjson n "$pr" '.[] | select(.number == $n) | .title' <<< "$json")" \
        "$(jq -r --argjson n "$pr" '.[] | select(.number == $n) | .body // ""' <<< "$json")"
    done
  done | sort -un
}

on_rc() {
  : "${TAG:?}"
  if [[ ! "$TAG" =~ ^v([0-9]+\.[0-9]+\.[0-9]+)-rc([0-9]+)$ ]]; then
    echo "$TAG is not a release candidate tag, skipping."
    return 0
  fi
  local version=${BASH_REMATCH[1]} rc=${BASH_REMATCH[2]}
  local prev="v$version-rc$((rc - 1))"
  if [ "$rc" -lt 2 ] || ! git rev-parse -q --verify "refs/tags/$prev" > /dev/null; then
    echo "No previous RC for $TAG, nothing to relabel."
    return 0
  fi

  # Commits merged since the previous RC were labeled with the SNAPSHOT
  # version the release script bumped to, but they ship in this release.
  local next issues later_refs later_issues issue labels
  next=$(git show "$TAG^:gradle.properties" | parse_version)
  issues=$(issues_in_range "$prev..$TAG")
  if [ -z "$issues" ]; then
    echo "No issue fixed between $prev and $TAG."
    return 0
  fi
  # Issues fixed again after this RC still ship in the next release.
  later_refs=$(git for-each-ref --contains "$TAG" --format='%(refname)' refs/remotes)
  later_issues=""
  if [ -n "$later_refs" ]; then
    # shellcheck disable=SC2086
    later_issues=$(issues_in_range $later_refs --not "$TAG")
  fi

  echo "Issues fixed between $prev and $TAG ship in $version."
  ensure_label "$version"
  for issue in $issues; do
    labels=$(gh api "repos/$REPO/issues/$issue" \
      --jq 'if has("pull_request") then "PR" else ([.labels[].name] | join(",")) end')
    if [ "$labels" = "PR" ]; then
      echo "#$issue is a pull request, skipping."
      continue
    fi
    write gh issue edit "$issue" --repo "$REPO" --add-label "$version"
    echo "Labeled issue #$issue with $version."
    if [ "$next" != "$version" ] && [[ ",$labels," == *",$next,"* ]] \
        && ! grep -qxF "$issue" <<< "$later_issues"; then
      write gh issue edit "$issue" --repo "$REPO" --remove-label "$next"
      echo "Removed $next from issue #$issue."
    fi
  done
}

case "${1:-}" in
  merge) on_merge ;;
  rc) on_rc ;;
  *)
    echo "Usage: $0 merge|rc" >&2
    exit 1
    ;;
esac
