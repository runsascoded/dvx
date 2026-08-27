# dvx base image for AWS Batch (Fargate Spot).
#
# Build from the repo root:
#   docker build -t dvx:$(git rev-parse --short HEAD) --platform linux/arm64 .
#
# Push to ECR (creates the repo if missing, ECR-login handled):
#   dvx batch push <acct>.dkr.ecr.<region>.amazonaws.com/dvx:$(git rev-parse --short HEAD)
#
# Run shape (AWS Batch containerOverrides supplies the args):
#   dvx run --commit never --push each [-j <vcpus>] [targets...]
#
# Storage + config creds via env: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
# (or S3-compatible R2_* pair).
#
# **App images derive from this** — dvx reads .dvc files + git_deps from the
# checkout, so the concrete DAG lives in the app image:
#
#   FROM dvx:<rev>
#   WORKDIR /work
#   COPY . .
#   RUN uv sync                              # if the app uses uv
#   # then submit with:
#   #   dvx batch submit -j <vcpus> [-e AWS_ACCESS_KEY_ID=... ] [targets...]
#
# See specs/done/batch-executor.md for the full design.

FROM python:3.13-slim

WORKDIR /opt/dvx

# System deps for `dvc` (uses `git` for git_deps hashing) and a minimal
# toolchain for building any C-extension deps `pip install` might pull.
RUN apt-get update \
    && apt-get install --no-install-recommends -y git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install dvx + the S3 remote backend. Downstream app images layer their own
# code on top; they don't reinstall dvx.
RUN pip install --no-cache-dir 'dvx[s3]'

# Fargate/aarch64 runtime hardening (inherited from pyrmts-engine ctbk first-
# smoke findings): faulthandler turns mute SIGSEGVs (exit 139) into tracebacks.
# Overridable via job-def / submit env.
ENV PYTHONFAULTHANDLER=1

ENTRYPOINT ["dvx"]
