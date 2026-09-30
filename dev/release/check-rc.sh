#!/usr/bin/env bash
# Pre-vote checks for a Gravitino release candidate.
# Usage: ./check-rc.sh <version> <rc-dir-name>
#   e.g. ./check-rc.sh 1.3.1 v1.3.1-rc2
set -uo pipefail

VERSION="${1:?usage: check-rc.sh <version> <rc-dir>}"
RC="${2:?usage: check-rc.sh <version> <rc-dir>}"
BASE="https://dist.apache.org/repos/dist/dev/gravitino/${RC}"
WORK="$(mktemp -d)"
FAIL=0

note() { printf '\n== %s ==\n' "$1"; }
bad()  { printf 'FAIL: %s\n' "$1"; FAIL=1; }
ok()   { printf 'ok:   %s\n' "$1"; }

trap 'rm -rf "$WORK"' EXIT

note "Signing key: published in the RELEASE KEYS file?"
# The release KEYS file is what verifiers actually use. Being in dev/ is not enough.
mkdir -p "$WORK/gh"; chmod 700 "$WORK/gh"
curl -sSf --max-time 60 "https://dist.apache.org/repos/dist/release/gravitino/KEYS" \
  -o "$WORK/KEYS.rel" || bad "could not fetch release KEYS"
gpg --homedir "$WORK/gh" --batch --quiet --import "$WORK/KEYS.rel" 2>/dev/null

note "Artifacts: signature + checksum"
FILES=$(curl -sSf --max-time 60 "$BASE/" \
  | grep -oE 'href="[^"]+\.tar\.gz"' | cut -d'"' -f2 | sort -u)
[ -z "$FILES" ] && bad "no artifacts found at $BASE/"

for f in $FILES; do
  curl -sSf --max-time 900 -o "$WORK/$f"      "$BASE/$f"        || { bad "download $f";     continue; }
  curl -sSf --max-time 60  -o "$WORK/$f.asc"  "$BASE/$f.asc"    || { bad "missing $f.asc";  continue; }
  curl -sSf --max-time 60  -o "$WORK/$f.sha512" "$BASE/$f.sha512" || { bad "missing $f.sha512"; continue; }

  want=$(awk '{print $1; exit}' "$WORK/$f.sha512")
  got=$(shasum -a 512 "$WORK/$f" | awk '{print $1}')
  [ "$want" = "$got" ] && ok "sha512 $f" || bad "sha512 mismatch $f"

  if gpg --homedir "$WORK/gh" --status-fd 3 --verify "$WORK/$f.asc" "$WORK/$f" \
       3>"$WORK/st" >/dev/null 2>&1; then
    ok "signature $f"
    # VALIDSIG field 12 is the primary key fingerprint, field 3 the signing subkey.
    awk '$2=="VALIDSIG"{print ($12!="" ? $12 : $3)}' "$WORK/st" >> "$WORK/signers"
  else
    bad "signature $f (not verifiable with the RELEASE KEYS file)"
  fi

  # https://infra.apache.org/release-distribution.html - recommends <100MB, will not host >1GB
  mb=$(( $(wc -c < "$WORK/$f") / 1048576 ))
  if   [ "$mb" -ge 1024 ]; then bad "$f is ${mb}MB - over the 1GB hard limit"
  elif [ "$mb" -ge 100  ]; then printf 'WARN: %s is %sMB (Infra recommends <100MB)\n' "$f" "$mb"
  fi
done

note "Signing key algorithm"
# https://infra.apache.org/release-distribution.html - "Signing keys for new
# artifacts must be RSA and at least 2048 bit. New keys should be 4096 bit RSA."
# Only the key that signed this rc is in scope; other keys in KEYS are not.
if [ -s "$WORK/signers" ]; then
  for fpr in $(sort -u "$WORK/signers"); do
    pub=$(gpg --homedir "$WORK/gh" --with-colons --list-keys "$fpr" 2>/dev/null \
            | awk -F: '$1=="pub"{print; exit}')
    bits=$(printf '%s' "$pub" | cut -d: -f3)
    algo=$(printf '%s' "$pub" | cut -d: -f4)
    if   [ "$algo" != "1" ];   then bad "signing key $fpr is not RSA (gpg algo $algo)"
    elif [ "$bits" -lt 2048 ]; then bad "signing key $fpr is ${bits}-bit RSA, under the 2048 minimum"
    elif [ "$bits" -lt 4096 ]; then printf 'WARN: signing key %s is %s-bit RSA (4096 recommended)\n' "$fpr" "$bits"
    else ok "signing key $fpr is ${bits}-bit RSA"
    fi
  done
fi

note "Bundled jars vs LICENSE/NOTICE"
# Every bundled jar's artifact name should appear in the matching legal file.
# Shaded deps (FastDoubleParser in jackson-core, ASM in jersey-server) will not
# be caught here - those need the jar-content scan below.
for tgz in "$WORK"/*-bin.tar.gz; do
  [ -e "$tgz" ] || continue
  d="$WORK/x-$(basename "$tgz" .tar.gz)"; mkdir -p "$d"
  tar xzf "$tgz" -C "$d" 2>/dev/null
  printf -- '--- %s ---\n' "$(basename "$tgz")"
  find "$d" -name '*.jar' -exec basename {} \; \
    | sed -E 's/-[0-9][0-9A-Za-z._-]*\.jar$//' | sort -u \
    | while read -r lib; do
        grep -qiF "$lib" LICENSE.bin NOTICE.bin 2>/dev/null \
          || printf '  unlisted: %s\n' "$lib"
      done | head -40
done

note "Shaded code inside bundled jars"
# These are the ones that bit RC1: code that ships inside another project's jar.
find "$WORK" -name 'jackson-core-*.jar' -o -name 'jersey-server-*.jar' 2>/dev/null \
  | while read -r j; do
      unzip -l "$j" 2>/dev/null | grep -qi 'fastdoubleparser\|fdp/' \
        && printf '  %s shades FastDoubleParser\n' "$(basename "$j")"
      unzip -l "$j" 2>/dev/null | grep -qi 'repackaged/org/objectweb/asm' \
        && printf '  %s repackages ASM\n' "$(basename "$j")"
    done

note "Paths referenced by LICENSE/NOTICE actually exist"
for ref in $(grep -ohE '(web(-v2)?/)[A-Za-z0-9./_-]+|FastDoubleParser-NOTICE' \
             LICENSE NOTICE LICENSE.bin NOTICE.bin 2>/dev/null | sort -u); do
  [ -e "$ref" ] && ok "$ref" || bad "referenced but missing: $ref"
done

note "Release dist directory retention"
# https://infra.apache.org/release-distribution.html - the dist directory should hold the
# latest release of each branch still under development, not every release ever made.
published=$(curl -sSf --max-time 60 "https://dist.apache.org/repos/dist/release/gravitino/" \
  | grep -oE 'href="[0-9][^"]*/"' | cut -d'"' -f2 | tr -d '/' | sort -V)
printf 'published versions: %s\n' "$(echo "$published" | tr '\n' ' ')"
echo "$published" | awk -F. '
  { v[NR] = $0; b = $1 "." $2
    if (!(b in best) || $3 + 0 > patch[b]) { best[b] = $0; patch[b] = $3 + 0 } }
  END { for (i = 1; i <= NR; i++) {
          split(v[i], p, "."); b = p[1] "." p[2]
          if (v[i] != best[b])
            printf "WARN: %s is superseded by %s in the same branch\n", v[i], best[b] } }'

note "Docker Hub descriptions"
for r in gravitino gravitino-iceberg-rest gravitino-lance-rest \
         gravitino-playground gravitino-mcp-server; do
  curl -sSf --max-time 45 "https://hub.docker.com/v2/repositories/apache/$r/" \
    | python3 -c "
import sys, json
d = json.load(sys.stdin)
text = (str(d.get('description') or '') + (d.get('full_description') or '')).lower()
name = '$r'
if 'incubating' in text:
    print(f'FAIL: apache/{name} still says Incubating')
elif 'convenience' not in text:
    print(f'FAIL: apache/{name} does not state these are convenience releases')
else:
    print(f'ok:   apache/{name}')
" 2>/dev/null || printf 'WARN: could not read apache/%s\n' "$r"
done

printf '\n'
[ "$FAIL" -eq 0 ] && echo "PASS" || echo "FAILURES PRESENT - do not call a vote"
exit "$FAIL"
