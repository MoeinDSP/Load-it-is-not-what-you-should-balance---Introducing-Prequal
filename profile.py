# -*- coding: utf-8 -*-
#!/usr/bin/env python
"""
CloudLab distributed profile for the Prequal load-balancer replication.

Topology (6 dedicated c220g1 nodes, 1 Gbps LAN):

  server1  10.10.1.1  -- loaded backend  CPU_LOAD=60  MAX_CONCURRENCY=3
  server2  10.10.1.2  -- loaded backend  CPU_LOAD=60  MAX_CONCURRENCY=3
  server3  10.10.1.3  -- clean  backend  CPU_LOAD=0   MAX_CONCURRENCY=20
  lb       10.10.1.10 -- lb-prequal :8080  +  lb-weightedrr :8081  +  hey
  bgload   10.10.1.20 -- antagonist (20 req/s direct to server1/server2)
  monitor  10.10.1.30 -- Prometheus :9090  +  Grafana :3000

After provisioning (~10 min):
  1. SSH into lb node and run: cd /opt/prequal && ./scripts/compare.sh --duration 30
  2. Forward Grafana:  ssh -L 3000:10.10.1.30:3000 FreeIRAN@<lb-hostname>
  3. Open http://localhost:3000  (admin/admin)
"""

import geni.portal as portal
import geni.rspec.pg as rspec
import geni.rspec.emulab as emulab

# ---------------------------------------------------------------------------
REPO  = "https://github.com/MoeinDSP/Load-it-is-not-what-you-should-balance---Introducing-Prequal"
HW    = "c220g1"
IMAGE = "urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU22-64-STD"

S1_IP  = "10.10.1.1"
S2_IP  = "10.10.1.2"
S3_IP  = "10.10.1.3"
LB_IP  = "10.10.1.10"
BG_IP  = "10.10.1.20"
MON_IP = "10.10.1.30"
# ---------------------------------------------------------------------------

pc = portal.Context()
request = pc.makeRequestRSpec()

lan = request.LAN("lan")

def make_node(name, ip):
    node = request.RawPC(name)
    node.hardware_type = HW
    node.disk_image = IMAGE
    iface = node.addInterface("if1")
    iface.addAddress(rspec.IPv4Address(ip, "255.255.255.0"))
    lan.addInterface(iface)
    return node

# Common preamble: install Docker, clone repo
PREAMBLE = (
    "#!/bin/bash\n"
    "set -eux\n"
    "curl -fsSL https://get.docker.com | bash\n"
    "systemctl enable --now docker\n"
    "usermod -aG docker $(getent passwd 1000 | cut -d: -f1)\n"
    "apt-get install -y docker-compose-plugin bc gawk\n"
    "git clone " + REPO + " /opt/prequal\n"
    "chmod -R 755 /opt/prequal\n"
    "cd /opt/prequal\n"
)

# ---- server1 ---------------------------------------------------------------
n_s1 = make_node("server1", S1_IP)
n_s1.addService(rspec.Execute(shell="bash", command=
    PREAMBLE +
    "docker build -f backend/Dockerfile -t backend .\n"
    "docker run -d --network=host"
    " -e PORT=80 -e SERVER_ID=server1 -e CPU_LOAD=60 -e MAX_CONCURRENCY=3"
    " backend\n"
    "touch /tmp/prequal-ready\n"
))

# ---- server2 ---------------------------------------------------------------
n_s2 = make_node("server2", S2_IP)
n_s2.addService(rspec.Execute(shell="bash", command=
    PREAMBLE +
    "docker build -f backend/Dockerfile -t backend .\n"
    "docker run -d --network=host"
    " -e PORT=80 -e SERVER_ID=server2 -e CPU_LOAD=60 -e MAX_CONCURRENCY=3"
    " backend\n"
    "touch /tmp/prequal-ready\n"
))

# ---- server3 ---------------------------------------------------------------
n_s3 = make_node("server3", S3_IP)
n_s3.addService(rspec.Execute(shell="bash", command=
    PREAMBLE +
    "docker build -f backend/Dockerfile -t backend .\n"
    "docker run -d --network=host"
    " -e PORT=80 -e SERVER_ID=server3 -e CPU_LOAD=0 -e MAX_CONCURRENCY=20"
    " backend\n"
    "touch /tmp/prequal-ready\n"
))

# ---- lb (prequal + weightedrr + hey + compare.sh) --------------------------
LB_SERVERS = "server1=" + S1_IP + ":80,server2=" + S2_IP + ":80,server3=" + S3_IP + ":80"

n_lb = make_node("lb", LB_IP)
n_lb.addService(rspec.Execute(shell="bash", command=
    PREAMBLE +
    "docker build -f Dockerfile -t lb .\n"
    # lb-prequal on :8080
    "docker run -d --network=host"
    " -e LB_PORT=8080 -e LB_ALGORITHM=prequal"
    " -e 'LB_SERVERS=" + LB_SERVERS + "'"
    " -e LB_QRIF=0.84 -e LB_PROBE_RATE=2"
    " lb\n"
    # lb-weightedrr on :8081
    "docker run -d --network=host"
    " -e LB_PORT=8081 -e LB_ALGORITHM=weightedrr"
    " -e 'LB_SERVERS=" + LB_SERVERS + "'"
    " -e LB_WEIGHT_INTERVAL=3600s"
    " lb\n"
    # hey load tester
    "wget -qO /tmp/hey https://hey-release.s3.us-east-2.amazonaws.com/hey_linux_amd64\n"
    "install -m755 /tmp/hey /usr/local/bin/hey\n"
    "touch /tmp/prequal-ready\n"
))

# ---- bgload (antagonist: 20 req/s direct to server1 + server2) -------------
n_bg = make_node("bgload", BG_IP)
n_bg.addService(rspec.Execute(shell="bash", command=
    PREAMBLE +
    "docker build -f cmd/bgload/Dockerfile -t bgload .\n"
    "docker run -d --network=host"
    " -e BG_TARGETS=" + S1_IP + ":80," + S2_IP + ":80"
    " -e BG_RATE=20"
    " bgload\n"
    "touch /tmp/prequal-ready\n"
))

# ---- monitor (Prometheus + Grafana) ----------------------------------------
# Fix prometheus.yml: replace Docker service names with the real lb IP/ports
n_mon = make_node("monitor", MON_IP)
n_mon.addService(rspec.Execute(shell="bash", command=
    PREAMBLE +
    # Rewrite prometheus scrape targets to point at lb node
    "sed -i 's|lb-prequal:8080|" + LB_IP + ":8080|g' config/prometheus/prometheus.yml\n"
    "sed -i 's|lb-weightedrr:8080|" + LB_IP + ":8081|g' config/prometheus/prometheus.yml\n"
    # Prometheus
    "docker run -d --network=host"
    " -v /opt/prequal/config/prometheus:/etc/prometheus:ro"
    " prom/prometheus:v2.51.0"
    " --config.file=/etc/prometheus/prometheus.yml"
    " --storage.tsdb.path=/prometheus\n"
    # Grafana
    "docker run -d --network=host"
    " -e GF_SECURITY_ADMIN_PASSWORD=admin"
    " -e GF_USERS_ALLOW_SIGN_UP=false"
    " -e GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/var/lib/grafana/dashboards/loadbalancer.json"
    " -v /opt/prequal/config/grafana/provisioning:/etc/grafana/provisioning:ro"
    " -v /opt/prequal/config/grafana/dashboards:/var/lib/grafana/dashboards:ro"
    " grafana/grafana:10.4.2\n"
    "touch /tmp/prequal-ready\n"
))

pc.printRequestRSpec(request)
