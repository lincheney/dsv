#!/usr/bin/env bash
set -eu -o pipefail

BUILD_TARGET="$1"
PACKAGE_NAME="$2"

echo "Building for target: $BUILD_TARGET"
cargo build --release --target "$BUILD_TARGET"

echo "Copying binary as: $PACKAGE_NAME"
cp "target/$BUILD_TARGET/release/dsv" "$PACKAGE_NAME"

echo "Build complete: $PACKAGE_NAME"