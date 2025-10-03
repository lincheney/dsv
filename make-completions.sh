#!/usr/bin/env bash

set -eu -o pipefail
cd "$(dirname "$0")"
mkdir -p completions/
for shell in bash zsh; do
    output="$(
        ENABLE_SHTAB=1 PYTHONPATH= shtab --shell="$shell" _dsv.__main__.make_main_parser --error-unimportable --prog dsv \
        | if [[ "$shell" == zsh ]]; then
            sed -e 's/(\*)/*/' -e 's/"\*:command /"*:::command /'
        elif [[ "$shell" == bash ]]; then
            sed 's/\$pos_only = 0/& \&\& "$current_action_compgen" != _shtab__dsv_*_dsv_custom_complete/'
        fi
    )"
    if ! diff completions/dsv."$shell" <(echo "$output"); then
        echo "$output" >completions/dsv."$shell"
    fi
done
