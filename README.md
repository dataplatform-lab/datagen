[![Codacy Badge](https://app.codacy.com/project/badge/Grade/50357a4321da4972a58d387c6c821eb1)](https://app.codacy.com/gh/dataplatform-lab/datagen/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)

# Description

Big Data Generator for testing

## Pre-requisites

```bash
# Install uv and dependencies
brew install uv
uv sync

# Install pre-commit
pre-commit install

# Install trivy
brew install trivy

# Install gitleaks
brew install gitleaks
```

## Validate

### Various validations

```bash
./validate.sh

or

uv run pre-commit run -a
```

### SonarQube (need token)

```bash
docker run \
 --rm \
 -e SONAR_TOKEN="$SONAR_TOKEN" \
 -v $HOME/.sonar/cache:/opt/sonar-scanner/.sonar/cache \
 -v ".:/usr/src" \
 sonarsource/sonar-scanner-cli
```

online results: <https://sonarcloud.io/project/overview?id=dtonic%3Adhub-dashboard-collector>

### CodeQL CLI

```bash
brew install --cask codeql
codeql database create /tmp/codeql-db --language=python --overwrite --source-root src
codeql database analyze /tmp/codeql-db codeql/python-queries:codeql-suites/python-security-and-quality.qls --format=sarif-latest --output=/tmp/codeql-results.sarif

# need vscode sarif viewer extensions
code /tmp/codeql-results.sarif
```

### Snyk (need token)

```bash
brew install snyk-cli
snyk auth
uv run pip freeze -l > /tmp/requirements.txt && \
    uv run snyk test --severity-threshold=high --fail-fast --file=/tmp/requirements.txt
uv run snyk code test --severity-threshold=high --fail-fast
uv run snyk code iac --severity-threshold=high --fail-fast
uv run snyk code container test dhub-dashboard-collector:test --file=Dockerfile --severity-threshold=high --fail-fast
```

### Dependency-check (need nvdApiKey)

```bash
# Get nvdApiKey https://nvd.nist.gov/developers/request-an-api-key
brew install dependency-check
uv run dependency-check --scan src -failOnCVSS 7 \
    --nvdApiKey "$nvdApiKey" \
    --prettyPrint  -f JSON -o /tmp/dependency-check.json \
    && cat /tmp/dependency-check.json
```

You can save nvdApiKey to ~/.config/dependency-check/dependencycheck.properties and avoid passing it every time

## Run on local

Set up python environment

```bash
uv sync
```

Run Kafka producer

```bash
# Produce fake data
uv run src/produce_fake.py \
--kafka-bootstrap-servers BOOTSTRAP_SERVER --kafka-security-protocol SASL_PLAINTEXT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD \
--kafka-topic test-kafka-topic \
--nz-schema-file samples/test/schema/fake.csv --nz-schema-file-type csv \
--report-interval 1 \
--output-type json

# Post fake data to pandas http
uv run python3 src/pandas_http_fake.py \
--host PANDAS_PROXY_HOST --port PANDAS_PROXY_PORT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD --ssl --kafka-topic test-kafka-topic \
--nz-schema-file samples/test/schema/fake.csv --nz-schema-file-type csv \
--output-type json

# Produce a file
uv run python3 src/produce_file.py \
--kafka-bootstrap-servers BOOTSTRAP_SERVER --kafka-security-protocol SASL_PLAINTEXT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD \
--kafka-topic fake_test \
--input-filepath samples/test/schema/fake.jsonl --input-type jsonl --output-type json \
--report-interval 1 \
--loglevel DEBUG

# Post a file to pandas http
uv run python3 src/pandas_http_file.py \
--host PANDAS_PROXY_HOST --port PANDAS_PROXY_PORT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD --ssl --kafka-topic test-kafka-topic \
--nz-schema-file samples/test/schema/fake.csv --nz-schema-file-type csv \
--input-filepath samples/test/schema/fake.json --input-type json --output-type json
```

Consumer Kafka data

```bash
python src/consumer_loop.py \
--kafka-bootstrap-servers BOOTSTRAP_SERVER --kafka-security-protocol SASL_PLAINTEXT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD \
--kafka-topic fake_test --input-type json \
--loglevel DEBUG
```

Run MQTT publisher

```bash
# Publish fake data
uv run python3 src/publish_fake.py --mqtt-host MQTT_HOST --mqtt-port MQTT_PORT --mqtt-username MQTT_USERNAME --mqtt-password MQTT_PASSWORD  --mqtt-kafka-topic MQTT_TOPIC --mqtt-tls --mqtt-tls-insecure \
--nz-schema-file samples/test/schema/fake.csv --nz-schema-file-type csv \
--output-type json

# Publish a file
uv run python3 src/publish_file.py --mqtt-host MQTT_HOST --mqtt-port MQTT_PORT --mqtt-username MQTT_USERNAME --mqtt-password MQTT_PASSWORD  --mqtt-kafka-topic MQTT_TOPIC --mqtt-tls --mqtt-tls-insecure \
--nz-schema-file samples/test/schema/fake.csv --nz-schema-file-type csv \
--input-filepath samples/test/schema/fake.json --input-type json --output-type json
```

Create, Delete a Nazare pipeline

```bash
# Create pipeline
uv run python3 src/nazare_pipeline_create.py \
--nz-api-url STORE_API_URL --nz-api-username STORE_API_USERNAME --nz-api-password STORE_API_PASSWORD \
--nz-pipeline-name PIPELINE_NAME --nz-pipeline-type PIPELINE_TYPE -no-pipeline-deltasync --pipeline-retention '60,d' \
--nz-schema-file SCHEMA_FILE --nz-schema-file-type SCHEMA_FILE_TYPE

# Delete pipeline
uv run python3 src/nazare_pipeline_delete.py \
--nz-api-url STORE_API_URL --nz-api-username STORE_API_USERNAME --nz-api-password STORE_API_PASSWORD \
--nz-pipeline-name PIPELINE_NAME \
```

## Run on docker

```bash
# Produce fake data
docker run --rm -it bluezery/datagen:test uv run src/produce_fake.py --kafka-bootstrap-servers BOOTSTRAP_SERVER --kafka-security-protocol SASL_PLAINTEXT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD --kafka-topic test-kafka-topic --rate 1  --report-interval 1
```

## Run on K8s

```bash

```

## Build

Create buildx docker-container driver for multi-target build

```bash
docker buildx create --name multi-builder --driver docker-container --bootstrap
```

Build and load

```bash
docker buildx build -t bluezery/datagen:test --platform linux/arm64 --load .
```

Build and push

```bash
docker buildx build -t bluezery/datagen:test --platform linux/amd64,linux/arm64 --push .
```
