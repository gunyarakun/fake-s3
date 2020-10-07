#!/bin/bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get -y upgrade
apt-get -y install --no-install-recommends build-essential
#apt-get -y install --no-install-recommends linux-kernel-headers
apt-get clean
rm -rf /var/lib/apt/lists/*
