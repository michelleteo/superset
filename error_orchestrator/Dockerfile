# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Self-contained demo image: simulator + orchestrator + dashboard on one port.
# Build from the repository root:
#   docker build -f error_orchestrator/Dockerfile -t error-orchestrator-demo .
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    ERROR_ORCHESTRATOR_HOST=0.0.0.0 \
    ERROR_ORCHESTRATOR_PORT=8099

WORKDIR /app

COPY error_orchestrator/requirements.txt /app/error_orchestrator/requirements.txt
RUN pip install --no-cache-dir -r /app/error_orchestrator/requirements.txt

COPY error_orchestrator /app/error_orchestrator

RUN useradd --create-home --uid 1000 demo && chown -R demo /app
USER demo

EXPOSE 8099

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s \
    CMD python -c "import urllib.request as r; r.urlopen('http://127.0.0.1:8099/healthz')"

# Override the stream shape at run time, e.g.
#   docker run -p 8099:8099 error-orchestrator-demo --rate 3 --speed 2
ENTRYPOINT ["python", "-m", "error_orchestrator.demo"]
CMD ["--rate", "2", "--speed", "2"]
