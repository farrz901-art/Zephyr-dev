# uns-api (Unstructured API) local setup

## Start
```bash
docker compose -f docker-compose.uns-api.yml up -d

## Stop
```bash
docker compose -f docker-compose.uns-api.yml down

Partition endpoint (used by HttpUnsApiBackend)
http://localhost:8001/general/v0/general
