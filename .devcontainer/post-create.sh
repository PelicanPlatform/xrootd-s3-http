#!/bin/bash
# Post-create script for dev container setup

set -e

echo "=== Setting up XRootD S3/HTTP Plugin Development Environment ==="

# Navigate to workspace
cd /workspaces/xrootd-s3-http

# Initialize pre-commit hooks
echo "Installing pre-commit hooks..."
pre-commit install

# Configure and build the project
echo "Configuring project with CMake..."

# Clean build directory contents if it exists (may be a volume mount)
if [ -d "build" ]; then
    sudo rm -rf build/* build/.[!.]* 2>/dev/null || true
    sudo chown -R vscode:vscode build 2>/dev/null || true
fi
mkdir -p build
cd build

cmake .. \
    -DENABLE_TESTS=ON \
    -DCMAKE_BUILD_TYPE=Debug \
    -G Ninja

echo "Building project..."
ninja -j$(nproc)

echo "=== Dev container setup complete ==="
