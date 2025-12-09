FROM mcr.microsoft.com/devcontainers/cpp:1-ubuntu-24.04

# Install build dependencies
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get install -y --no-install-recommends \
        cmake \
        libz-dev \
        uuid-dev \
        libcurl4-openssl-dev \
        libcurl4 \
        pkg-config \
        libssl-dev \
        g++ \
        libscitokens-dev \
        libgtest-dev \
        ninja-build \
        clang-format \
        python3-pip \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Install pre-commit
RUN pip3 install --break-system-packages pre-commit

# Build XRootD from the Pelican fork with patches
ARG XROOTD_VERSION=v5.8.4-pelican
RUN cd /tmp \
    && git clone https://github.com/xrootd/xrootd.git \
    && cd xrootd \
    && git remote add github_pelican https://github.com/PelicanPlatform/xrootd.git \
    && git fetch github_pelican \
    && git checkout -b ${XROOTD_VERSION} -t github_pelican/${XROOTD_VERSION} \
    && mkdir -p build \
    && cd build \
    && cmake .. -DCMAKE_INSTALL_PREFIX=/opt/xrootd -G Ninja \
    && ninja -j$(nproc) \
    && ninja install \
    && cd / \
    && rm -rf /tmp/xrootd

# Set up environment for XRootD
ENV CMAKE_PREFIX_PATH=/opt/xrootd/lib/cmake/XRootD
ENV LD_LIBRARY_PATH=/opt/xrootd/lib:${LD_LIBRARY_PATH}
ENV PATH=/opt/xrootd/bin:${PATH}
