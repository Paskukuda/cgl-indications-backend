#!/usr/bin/env bash
# One-command deploy: pull latest backend code and restart the API service.
# Run this on the server from /srv/cgl after every update.
set -e
cd /srv/cgl
git pull
sudo systemctl restart cgl-api.service
sleep 1
sudo systemctl status cgl-api.service --no-pager -l | head -5
echo "--- health check ---"
curl -s http://127.0.0.1:8090/api/health && echo
