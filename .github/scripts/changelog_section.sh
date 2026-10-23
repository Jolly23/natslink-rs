#!/usr/bin/env bash
# Print the CHANGELOG.md section for one version (the text between
# "## [X.Y.Z]" and the next "## [" heading), for use as the release body.
#
#   .github/scripts/changelog_section.sh 1.6.0 [CHANGELOG.md]
set -euo pipefail
version="${1:?usage: changelog_section.sh <version-without-v> [changelog]}"
file="${2:-CHANGELOG.md}"
awk -v v="$version" '
  /^## \[/ {
    if (found) exit
    if (index($0, "## [" v "]") == 1) { found = 1; next }
  }
  found { print }
' "$file" | sed -e '/./,$!d' -e :a -e '/^\n*$/{$d;N;ba' -e '}'
