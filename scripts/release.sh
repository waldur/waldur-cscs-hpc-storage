#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Usage ──────────────────────────────────────────────────────────────────
if [ $# -lt 1 ]; then
    echo "Usage: $0 <VERSION>"
    echo "Example: $0 0.8.0"
    exit 1
fi

VERSION=$1

# ── Validate version format ───────────────────────────────────────────────
# Must match the CI release rule: $CI_COMMIT_TAG =~ /^\d+\.\d+\.\d+$/
if ! echo "$VERSION" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+$'; then
    echo "Error: version '$VERSION' must be in X.Y.Z format (e.g. 0.8.0)."
    echo "This is required for CI release jobs to trigger."
    exit 1
fi

# ── Pre-flight checks ────────────────────────────────────────────────────
cd "$PROJECT_DIR"

BRANCH=$(git branch --show-current)
if [ "$BRANCH" != "main" ]; then
    echo "Warning: you are on branch '$BRANCH', not 'main'."
    read -p "Continue anyway? [y/N] " choice
    [ "$choice" = "y" ] || [ "$choice" = "Y" ] || exit 1
fi

if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "Error: working tree is not clean. Commit or stash changes first."
    exit 1
fi

if git rev-parse "$VERSION" >/dev/null 2>&1; then
    echo "Error: tag '$VERSION' already exists."
    exit 1
fi

# ── Show current version ─────────────────────────────────────────────────
CURRENT=$(sed -n 's/^version = "\(.*\)"/\1/p' pyproject.toml)
echo "=== Releasing waldur-cscs-hpc-storage $VERSION (current: $CURRENT) ==="
echo ""

# ── Step 1: Bump version in pyproject.toml ───────────────────────────────
echo "[1/5] Bumping version in pyproject.toml..."
sed -i.bak "s/^version = \".*\"$/version = \"$VERSION\"/" pyproject.toml
rm -f pyproject.toml.bak
echo "  $CURRENT -> $VERSION"
echo ""

# ── Step 2: Regenerate lockfile ──────────────────────────────────────────
echo "[2/5] Regenerating uv.lock..."
uv lock
echo ""

# ── Step 3: Generate changelog ───────────────────────────────────────────
echo "[3/5] Generating changelog..."
"$SCRIPT_DIR/changelog.sh" "$VERSION"
echo ""

# ── Step 4: Commit ───────────────────────────────────────────────────────
echo "[4/5] Committing release..."
git add pyproject.toml uv.lock
git add CHANGELOG.md
git commit -m "Release $VERSION"
echo ""

# ── Step 5: Tag ──────────────────────────────────────────────────────────
echo "[5/5] Tagging $VERSION..."
git tag "$VERSION"
echo ""

echo "=== Release $VERSION prepared ==="
echo ""
echo "Review the commit and tag, then push with:"
echo "  git push origin main --tags"
