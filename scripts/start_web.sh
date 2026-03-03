#!/usr/bin/env bash
set -euo pipefail

REPO_SLUG="${GITHUB_REPO:-byee4/yeolab-publications-db}"
REPO_BRANCH="${GITHUB_BRANCH:-main}"
REPO_DIR="${CODE_EXAMPLES_REPO_DIR:-/app/yeolab-publications-db}"
CODE_EXAMPLES_PATH="${REPO_DIR}/code_examples"
GUNICORN_WORKERS="${GUNICORN_WORKERS:-4}"
GUNICORN_TIMEOUT="${GUNICORN_TIMEOUT:-600}"
GUNICORN_BIND="${GUNICORN_BIND:-0.0.0.0:8000}"
export GIT_TERMINAL_PROMPT=0

sync_repo() {
  local repo_url="https://github.com/${REPO_SLUG}.git"
  local auth_cfg=()

  if [[ -n "${GITHUB_PAT:-}" ]]; then
    local basic
    basic="$(printf "x-access-token:%s" "${GITHUB_PAT}" | base64 | tr -d '\n')"
    auth_cfg=(-c "http.extraHeader=Authorization: Basic ${basic}")
  fi

  if [[ -d "${REPO_DIR}/.git" ]]; then
    echo "[start_web] Updating ${REPO_SLUG} (${REPO_BRANCH})..."
    git -C "${REPO_DIR}" remote set-url origin "${repo_url}"
    git "${auth_cfg[@]}" -C "${REPO_DIR}" fetch origin "${REPO_BRANCH}" --depth 1
    git -C "${REPO_DIR}" checkout "${REPO_BRANCH}"
    git -C "${REPO_DIR}" reset --hard "origin/${REPO_BRANCH}"
  else
    echo "[start_web] Cloning ${REPO_SLUG} (${REPO_BRANCH}) into ${REPO_DIR}..."
    mkdir -p "$(dirname "${REPO_DIR}")"
    git "${auth_cfg[@]}" clone --depth 1 --branch "${REPO_BRANCH}" "${repo_url}" "${REPO_DIR}"
  fi
}

if sync_repo 2>&1; then
  if [[ -d "${CODE_EXAMPLES_PATH}" ]]; then
    export CODE_EXAMPLES_DIR="${CODE_EXAMPLES_PATH}"
    echo "[start_web] Using CODE_EXAMPLES_DIR=${CODE_EXAMPLES_DIR}"
  else
    echo "[start_web] WARNING: ${CODE_EXAMPLES_PATH} not found; falling back to local code_examples directory."
  fi
else
  echo "[start_web] WARNING: failed to sync ${REPO_SLUG}; continuing with fallback code_examples directory."
fi

cd /app/yeolab_search
exec gunicorn \
  --bind "${GUNICORN_BIND}" \
  --workers "${GUNICORN_WORKERS}" \
  --timeout "${GUNICORN_TIMEOUT}" \
  --access-logfile - \
  --error-logfile - \
  yeolab_search.wsgi:application
