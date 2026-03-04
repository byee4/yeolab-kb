"""
GitHub API integration for syncing per-dataset code example files.

Each dataset has its own JSON file organized by year/month:
  ``code_examples/2020/Oct/GSE137810.json``

This module provides per-file fetch, push, list, and delete operations
via the GitHub REST API v3.

Configuration (env vars):
    GITHUB_PAT              - Personal access token with repo write access
    GITHUB_REPO             - "owner/repo" (default: "byee4/yeolab-publications-db")
    GITHUB_BRANCH           - Branch name (default: "main")
"""

import base64
import json
import os

_DEFAULT_REPO = "byee4/yeolab-publications-db"
_DEFAULT_BRANCH = "main"
_CODE_EXAMPLES_DIR = "code_examples"

# Try requests first, fallback to urllib
try:
    import requests as _requests
except ImportError:
    _requests = None


def _get_config():
    """Return (repo, branch, pat) from environment."""
    repo = os.environ.get("GITHUB_REPO", _DEFAULT_REPO)
    branch = os.environ.get("GITHUB_BRANCH", _DEFAULT_BRANCH)
    pat = os.environ.get("GITHUB_PAT", "")
    return repo, branch, pat


def _api_get(url, pat):
    """GET request to GitHub API."""
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "yeolab-sync/1.0",
    }
    if pat:
        headers["Authorization"] = f"token {pat}"

    if _requests:
        resp = _requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()
    else:
        import urllib.request
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))


def _api_put(url, pat, data):
    """PUT request to GitHub API."""
    if not pat:
        raise RuntimeError("GITHUB_PAT environment variable is required for push operations")

    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "yeolab-sync/1.0",
        "Authorization": f"token {pat}",
        "Content-Type": "application/json",
    }

    body = json.dumps(data).encode("utf-8")

    if _requests:
        resp = _requests.put(url, headers=headers, data=body, timeout=30)
        resp.raise_for_status()
        return resp.json()
    else:
        import urllib.request
        req = urllib.request.Request(url, data=body, headers=headers, method="PUT")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))


def _api_delete(url, pat, data):
    """DELETE request to GitHub API."""
    if not pat:
        raise RuntimeError("GITHUB_PAT environment variable is required for delete operations")

    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "yeolab-sync/1.0",
        "Authorization": f"token {pat}",
        "Content-Type": "application/json",
    }

    body = json.dumps(data).encode("utf-8")

    if _requests:
        resp = _requests.delete(url, headers=headers, data=body, timeout=30)
        resp.raise_for_status()
        return resp.json()
    else:
        import urllib.request
        req = urllib.request.Request(url, data=body, headers=headers, method="DELETE")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _build_remote_path(accession: str, rel_path: str | None = None) -> str:
    """Build the full GitHub path for a dataset file."""
    if rel_path:
        return f"{_CODE_EXAMPLES_DIR}/{rel_path}/{accession}.json"
    return f"{_CODE_EXAMPLES_DIR}/{accession}.json"


def _parse_tree_path(tree_path: str) -> dict | None:
    """
    Parse a git tree path like 'code_examples/2020/Oct/GSE120023.json'.
    Returns {"accession": "GSE120023", "rel_path": "2020/Oct"} or None.
    """
    if not tree_path.startswith(_CODE_EXAMPLES_DIR + "/"):
        return None
    if not tree_path.endswith(".json"):
        return None

    # Strip prefix and .json
    inner = tree_path[len(_CODE_EXAMPLES_DIR) + 1:-5]  # e.g. "2020/Oct/GSE120023"
    parts = inner.split("/")

    accession = parts[-1]
    if accession.startswith("_") or accession.startswith("."):
        return None

    rel_path = "/".join(parts[:-1]) if len(parts) > 1 else ""

    return {"accession": accession, "rel_path": rel_path}


# ---------------------------------------------------------------------------
# Per-dataset operations
# ---------------------------------------------------------------------------

def _list_via_trees_api(repo, branch, pat):
    """
    List datasets using the Git Trees API (efficient, single request).
    May fail with 404 on some repo configurations.
    """
    url = f"https://api.github.com/repos/{repo}/git/trees/{branch}?recursive=1"
    data = _api_get(url, pat)

    datasets = []
    for item in data.get("tree", []):
        if item.get("type") != "blob":
            continue
        parsed = _parse_tree_path(item["path"])
        if parsed:
            datasets.append({
                "accession": parsed["accession"],
                "rel_path": parsed["rel_path"],
                "sha": item["sha"],
                "size": item.get("size", 0),
            })
    return datasets


def _list_via_contents_api(repo, branch, pat, dir_path=_CODE_EXAMPLES_DIR):
    """
    List datasets using the Contents API (recursive directory walk).
    Slower (one request per directory) but works reliably on all repos.
    """
    url = f"https://api.github.com/repos/{repo}/contents/{dir_path}?ref={branch}"
    data = _api_get(url, pat)

    if not isinstance(data, list):
        return []

    datasets = []
    for item in data:
        if item.get("type") == "dir":
            # Recurse into subdirectories (year/month)
            sub = _list_via_contents_api(repo, branch, pat, item["path"])
            datasets.extend(sub)
        elif item.get("type") == "file" and item["name"].endswith(".json"):
            parsed = _parse_tree_path(item["path"])
            if parsed:
                datasets.append({
                    "accession": parsed["accession"],
                    "rel_path": parsed["rel_path"],
                    "sha": item["sha"],
                    "size": item.get("size", 0),
                })
    return datasets


def list_remote_datasets():
    """
    List all dataset JSON files in the remote code_examples/ directory (recursive).

    Tries the Git Trees API first (single request, efficient). Falls back to
    the Contents API (one request per directory) if Trees API returns 404.
    Returns list of dicts: [{"accession": "GSE...", "rel_path": "2020/Oct", "sha": "abc"}, ...]
    """
    repo, branch, pat = _get_config()

    # Try Git Trees API first (fast, single request)
    try:
        datasets = _list_via_trees_api(repo, branch, pat)
        return sorted(datasets, key=lambda d: d["accession"])
    except Exception:
        pass

    # Fallback: Contents API (recursive directory walk)
    try:
        datasets = _list_via_contents_api(repo, branch, pat)
        return sorted(datasets, key=lambda d: d["accession"])
    except Exception as e:
        raise RuntimeError(f"Failed to list remote datasets: {e}")


def list_remote_json_files(remote_dir: str):
    """
    List JSON files under an arbitrary remote directory (recursive).
    Returns: [{"path": "...", "name": "...", "sha": "...", "size": N}, ...]
    """
    repo, branch, pat = _get_config()
    target = (remote_dir or "").strip("/").strip()
    if not target:
        raise RuntimeError("remote_dir is required")

    # Try trees API first (single call)
    try:
        url = f"https://api.github.com/repos/{repo}/git/trees/{branch}?recursive=1"
        data = _api_get(url, pat)
        out = []
        prefix = f"{target}/"
        for item in data.get("tree", []):
            if item.get("type") != "blob":
                continue
            path = str(item.get("path", ""))
            if not path.startswith(prefix) or not path.endswith(".json"):
                continue
            out.append({
                "path": path,
                "name": os.path.basename(path),
                "sha": item.get("sha", ""),
                "size": item.get("size", 0),
            })
        return sorted(out, key=lambda x: x["path"])
    except Exception:
        pass

    # Fallback to contents API recursion
    def _walk(dir_path):
        url = f"https://api.github.com/repos/{repo}/contents/{dir_path}?ref={branch}"
        data = _api_get(url, pat)
        if not isinstance(data, list):
            return []
        out = []
        for item in data:
            if item.get("type") == "dir":
                out.extend(_walk(item["path"]))
            elif item.get("type") == "file" and str(item.get("name", "")).endswith(".json"):
                out.append({
                    "path": item.get("path", ""),
                    "name": item.get("name", ""),
                    "sha": item.get("sha", ""),
                    "size": item.get("size", 0),
                })
        return out

    try:
        return sorted(_walk(target), key=lambda x: x["path"])
    except Exception as e:
        raise RuntimeError(f"Failed to list remote JSON files under {target}: {e}")


def fetch_remote_json_file(path: str):
    """
    Fetch an arbitrary remote JSON file by full repository path.
    Returns (content_str, sha).
    """
    repo, branch, pat = _get_config()
    clean = (path or "").strip("/")
    if not clean:
        raise RuntimeError("path is required")

    url = f"https://api.github.com/repos/{repo}/contents/{clean}?ref={branch}"
    try:
        data = _api_get(url, pat)
    except Exception as e:
        raise RuntimeError(f"Failed to fetch {clean}: {e}")
    if data.get("encoding") != "base64":
        raise RuntimeError(f"Unexpected encoding for {clean}: {data.get('encoding')}")
    content = base64.b64decode(data["content"]).decode("utf-8")
    return content, data.get("sha", "")


def fetch_dataset(accession: str, rel_path: str | None = None):
    """
    Fetch a single dataset file from GitHub.

    If rel_path not provided, tries to find it via list_remote_datasets.
    Returns (content_str, sha).
    Raises RuntimeError on failure.
    """
    repo, branch, pat = _get_config()

    candidates = []
    seen = set()

    def _add_candidate(path):
        clean = (path or "").strip("/")
        if clean in seen:
            return
        seen.add(clean)
        candidates.append(clean)

    # 1) Explicit path from caller (e.g. editor-selected year/month)
    _add_candidate(rel_path)

    # 2) Discover from remote index if not explicitly provided
    if rel_path is None:
        try:
            remote = list_remote_datasets()
            for d in remote:
                if d["accession"] == accession:
                    _add_candidate(d.get("rel_path", ""))
                    break
        except Exception:
            # Keep going and try root-level fallback below.
            pass

    # 3) Legacy/root fallback
    _add_candidate("")

    errors = []
    for candidate in candidates:
        path = _build_remote_path(accession, candidate or None)
        url = f"https://api.github.com/repos/{repo}/contents/{path}?ref={branch}"
        try:
            data = _api_get(url, pat)
            if data.get("encoding") != "base64":
                raise RuntimeError(f"Unexpected encoding: {data.get('encoding')}")
            content = base64.b64decode(data["content"]).decode("utf-8")
            sha = data["sha"]
            return content, sha
        except Exception as e:
            errors.append(f"{path}: {e}")

    raise RuntimeError(
        f"Failed to fetch {accession}. Tried: " + " | ".join(errors)
    )


def push_dataset(accession: str, content: str, rel_path: str | None = None, message: str = None):
    """
    Push a single dataset file to GitHub.

    Returns dict with commit info.
    """
    repo, branch, pat = _get_config()

    if not pat:
        raise RuntimeError(
            "GITHUB_PAT environment variable is not set. "
            "Set it to a Personal Access Token with repo write access."
        )

    if not message:
        path_label = f"{rel_path}/{accession}" if rel_path else accession
        message = f"Update {path_label}.json via admin panel"

    path = _build_remote_path(accession, rel_path)
    url = f"https://api.github.com/repos/{repo}/contents/{path}"

    # Get current SHA if file exists
    sha = None
    try:
        current = _api_get(f"{url}?ref={branch}", pat)
        sha = current["sha"]
    except Exception:
        pass  # File doesn't exist yet — will create

    encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")

    put_data = {
        "message": message,
        "content": encoded,
        "branch": branch,
    }
    if sha:
        put_data["sha"] = sha

    try:
        result = _api_put(url, pat, put_data)
    except Exception as e:
        raise RuntimeError(f"Failed to push {accession}: {e}")

    return {
        "commit_sha": result.get("commit", {}).get("sha", ""),
        "html_url": result.get("content", {}).get("html_url", ""),
    }


def delete_remote_dataset(accession: str, rel_path: str | None = None, message: str = None):
    """Delete a dataset file from GitHub."""
    repo, branch, pat = _get_config()

    if not message:
        path_label = f"{rel_path}/{accession}" if rel_path else accession
        message = f"Delete {path_label}.json via admin panel"

    path = _build_remote_path(accession, rel_path)
    url = f"https://api.github.com/repos/{repo}/contents/{path}"

    # Get current SHA
    try:
        current = _api_get(f"{url}?ref={branch}", pat)
        sha = current["sha"]
    except Exception as e:
        raise RuntimeError(f"File {accession}.json not found on GitHub: {e}")

    delete_data = {
        "message": message,
        "sha": sha,
        "branch": branch,
    }

    try:
        result = _api_delete(url, pat, delete_data)
    except Exception as e:
        raise RuntimeError(f"Failed to delete {accession}: {e}")

    return {
        "commit_sha": result.get("commit", {}).get("sha", ""),
    }


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

def get_pat_status() -> dict:
    """Check if GITHUB_PAT is configured and valid."""
    repo, branch, pat = _get_config()

    result = {
        "configured": bool(pat),
        "repo": repo,
        "branch": branch,
        "valid": False,
        "error": None,
    }

    if not pat:
        result["error"] = "GITHUB_PAT environment variable is not set"
        return result

    try:
        url = f"https://api.github.com/repos/{repo}"
        data = _api_get(url, pat)
        result["valid"] = True
        result["repo_name"] = data.get("full_name", repo)
        result["permissions"] = data.get("permissions", {})
    except Exception as e:
        result["error"] = str(e)

    return result
