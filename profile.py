# -*- coding: utf-8 -*-
#!/usr/bin/env python
"""
CloudLab profile for the Prequal load-balancer replication.
Provisions one node and installs Docker + the full compose stack.

Upload at: https://www.cloudlab.us/manage_profile.php
"""

import geni.portal as portal
import geni.rspec.pg as rspec
import geni.rspec.emulab as emulab

pc = portal.Context()

pc.defineParameter(
    "hardware_type", "Hardware Type",
    portal.ParameterType.NODETYPE, "c220g1",
    longDescription="c220g1 (Wisconsin), m510 (Utah), or d430 (Clemson)."
)

pc.defineParameter(
    "disk_image", "Disk Image",
    portal.ParameterType.IMAGE,
    "urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU22-64-STD",
    longDescription="Ubuntu 22.04 LTS - required for Docker compatibility."
)

pc.defineParameter(
    "repo_url", "Git Repository URL",
    portal.ParameterType.STRING,
    "https://github.com/YOUR_USERNAME/YOUR_REPO.git",
    longDescription="HTTPS URL of your Prequal repo."
)

params = pc.bindParameters()
pc.verifyParameters()

request = pc.makeRequestRSpec()

node = request.RawPC("prequal-node")
node.hardware_type = params.hardware_type
node.disk_image    = params.disk_image

# Reserve extra disk space for Docker images
bs = node.Blockstore("bs0", "/mydata")
bs.size = "50GB"

SETUP = """#!/bin/bash
set -eux

# --- Docker ------------------------------------------------------------------
curl -fsSL https://get.docker.com | bash
systemctl enable --now docker
usermod -aG docker "$(id -un 1000)"

# Docker Compose v2 (plugin)
apt-get install -y docker-compose-plugin

# --- hey (HTTP load tester used by compare.sh) -------------------------------
wget -qO /tmp/hey https://hey-release.s3.us-east-2.amazonaws.com/hey_linux_amd64
install -m755 /tmp/hey /usr/local/bin/hey

# --- bc, awk (used by compare.sh) --------------------------------------------
apt-get install -y bc gawk

# --- Clone repo and start stack ----------------------------------------------
REPO="{repo}"
USER_HOME=$(getent passwd 1000 | cut -d: -f6)
UNAME=$(id -un 1000)

sudo -u "$UNAME" bash -c "
    cd $USER_HOME
    git clone $REPO prequal
    cd prequal
    docker compose up -d --build
"

touch /tmp/prequal-ready
""".format(repo=params.repo_url)

node.addService(rspec.Execute(shell="bash", command=SETUP))

pc.printRequestRSpec(request)
