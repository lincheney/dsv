#!/usr/bin/env bash
set -eu -o pipefail

BUILD_TARGET="$1"
PACKAGE_NAME="$2"

echo "Building for target: $BUILD_TARGET"
cargo build --release --target "$BUILD_TARGET"

echo "Packaging binary as: $PACKAGE_NAME"
mkdir -p package
cp "target/$BUILD_TARGET/release/dsv" package/
cd package
tar czf "../$PACKAGE_NAME" *
cd ..
rm -rf package

echo "Build and package complete: $PACKAGE_NAME"