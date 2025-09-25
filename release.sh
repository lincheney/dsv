#!/usr/bin/env bash
set -eu -o pipefail

# Check if any artifacts exist
if [ ! -d "artifacts" ] || [ -z "$(find artifacts -name 'dsv-nightly-*' -type f)" ]; then
    echo "No successful build artifacts found. Skipping release creation."
    exit 1
fi

# Delete previous nightly release and tag
echo "Deleting previous nightly release..."
gh release delete nightly --yes || echo "No previous nightly release found"
git push origin --delete nightly || echo "No nightly tag found"

# Create nightly tag
echo "Creating nightly tag..."
git config user.name "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"
git tag -f nightly
git push origin nightly -f

# Generate release notes
echo "Generating release notes..."
cat > release_notes.md << EOF
# DSV Nightly Release

Automated nightly build from commit: \`${GITHUB_SHA}\`

Built on: $(date -u '+%Y-%m-%d %H:%M:%S UTC')

## Downloads

Choose the appropriate binary for your platform:

- **Linux x86_64**: \`dsv-nightly-x86_64-linux\`
- **Linux ARM64**: \`dsv-nightly-aarch64-linux\`

⚠️  **Note**: This is a pre-release build and may contain unstable features.
EOF

# Create nightly release with only available artifacts
echo "Creating nightly release..."
gh release create nightly \
  --title "DSV Nightly $(date -u '+%Y-%m-%d')" \
  --notes-file release_notes.md \
  --prerelease \
  artifacts/**/*

echo "Nightly release created successfully!"
