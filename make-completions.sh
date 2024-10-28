#!/usr/bin/env bash

cd "$(dirname "$0")"
mkdir -p completions/
for shell in bash zsh; do
    output="$(shtab --shell="$shell" _dsv.__main__.make_main_parser --error-unimportable --prog dsv)"
    if ! diff completions/dsv."$shell" <(echo "$output"); then
        echo "$output" >completions/dsv."$shell"
    fi
done
