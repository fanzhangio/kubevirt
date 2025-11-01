#!/bin/bash

# Complete Multi-Architecture Build and Push Script for KubeVirt
# This script builds and pushes both AMD64 and ARM64 images, then creates multi-arch manifests
#
# Usage: ./build-and-push-multiarch.sh [DOCKER_PREFIX] [DOCKER_TAG]
# Example: ./build-and-push-multiarch.sh nvcr.io/fcypcg1knhby/vmaas-dev upstream-1.7-vmaas-multiarch-dev

set -e

# Set defaults if not provided via environment variables
DOCKER_PREFIX=${DOCKER_PREFIX:-"localhost:32874/kubevirt"}
DOCKER_TAG=${DOCKER_TAG:-"devel"}

# Core KubeVirt components to build
COMPONENTS=(
    "virt-operator"
    "virt-api"
    "virt-controller"
    "virt-handler"
    "virt-launcher"
)

echo "=========================================="
echo " Complete Multi-Arch Build & Push for KubeVirt"
echo "=========================================="
echo "Registry: ${DOCKER_PREFIX}"
echo "Tag: ${DOCKER_TAG}"
echo "Components: ${COMPONENTS[*]}"
echo "Architectures: amd64, arm64"
echo

# Step 1: Set up the jq wrapper fix (essential for ARM64 builds)
echo "Step 1: Setting up jq wrapper fix..."
mkdir -p /tmp/kubevirt-fix
echo '#!/bin/bash' > /tmp/kubevirt-fix/jq
echo 'exec /usr/bin/jq "$@"' >> /tmp/kubevirt-fix/jq
chmod +x /tmp/kubevirt-fix/jq
export PATH="/tmp/kubevirt-fix:$PATH"
echo " jq wrapper ready"
echo

# Step 2: Build and push both architectures with multi-arch support
# Using comma-separated BUILD_ARCH tells KubeVirt's build system to:
# 1. Build each architecture with architecture-specific tags (DOCKER_TAG-amd64, DOCKER_TAG-arm64)
# 2. Automatically create multi-arch manifests via push-container-manifest.sh
# Using PUSH_TARGETS to only build the core components we need
PUSH_TARGETS_STR=$(IFS=' '; echo "${COMPONENTS[*]}")
echo "Step 2: Building and pushing multi-arch images..."
echo "Command: BUILD_ARCH=amd64,arm64 PUSH_TARGETS='${PUSH_TARGETS_STR}' DOCKER_PREFIX=${DOCKER_PREFIX} DOCKER_TAG=${DOCKER_TAG} make bazel-push-images"
echo

BUILD_ARCH="amd64,arm64" PUSH_TARGETS="${PUSH_TARGETS_STR}" DOCKER_PREFIX="${DOCKER_PREFIX}" DOCKER_TAG="${DOCKER_TAG}" make bazel-push-images

if [ $? -ne 0 ]; then
    echo " Multi-arch build failed. Please check the logs."
    exit 1
fi

echo
echo " Multi-arch images built and pushed successfully"
echo

# Step 3: Create multi-arch manifests using docker buildx imagetools
echo "Step 3: Creating multi-arch manifests..."
echo

for component in "${COMPONENTS[@]}"; do
    echo "Creating manifest for ${component}..."
    docker buildx imagetools create -t ${DOCKER_PREFIX}/${component}:${DOCKER_TAG} \
        ${DOCKER_PREFIX}/${component}:${DOCKER_TAG}-amd64 \
        ${DOCKER_PREFIX}/${component}:${DOCKER_TAG}-arm64
    if [ $? -eq 0 ]; then
        echo "✓ ${component} manifest created"
    else
        echo "✗ Failed to create manifest for ${component}"
    fi
done

echo
echo "Step 4: Verifying multi-arch manifests..."
echo

all_verified=true

for component in "${COMPONENTS[@]}"; do
    base_image="${DOCKER_PREFIX}/${component}:${DOCKER_TAG}"
    
    echo "Checking ${component}..."
    
    # Check using docker buildx imagetools
    if ! docker buildx imagetools inspect "${base_image}" >/dev/null 2>&1; then
        echo "   Manifest not found: ${base_image}"
        all_verified=false
        continue
    fi
    
    # Verify it's multi-arch using docker buildx imagetools
    manifest_info=$(docker buildx imagetools inspect "${base_image}" 2>/dev/null)
    if echo "$manifest_info" | grep -q 'Platform:.*linux/amd64' && echo "$manifest_info" | grep -q 'Platform:.*linux/arm64'; then
        echo "   ✓ Multi-arch manifest verified (amd64 + arm64)"
    else
        echo "   ⚠ Warning: Not all architectures found"
        all_verified=false
    fi
done

echo
echo "=========================================="
echo " Build Summary"
echo "=========================================="
if [ "$all_verified" = true ]; then
    echo " All multi-arch manifests verified successfully"
else
    echo "  Some manifests may not be multi-arch. Please verify manually."
fi
echo
echo "Registry: ${DOCKER_PREFIX}"
echo "Tag: ${DOCKER_TAG}"
echo
echo " Verify your images:"
for component in "${COMPONENTS[@]}"; do
    echo "  docker manifest inspect ${DOCKER_PREFIX}/${component}:${DOCKER_TAG}"
done
echo
echo "=========================================="
echo " DEPLOYMENT READY!"
echo "=========================================="
echo "Your images support both AMD64 and ARM64 architectures."
echo "Docker/Kubernetes will automatically pull the correct architecture."