from __future__ import annotations

import asyncio
import logging
import os
import sys
from base64 import b64decode
from dataclasses import dataclass
from pathlib import Path

import yaml
from httpx import AsyncClient, HTTPStatusError, Response, TimeoutException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("regen-crowdin-config")

MAX_CONCURRENT_REQUESTS = 8
MAX_RETRIES = 3
RETRY_BASE_DELAY_SECONDS = 2.0

REPOS_CONFIG_PATH = Path("repos.yaml")
SOURCE_DIR = Path("source")
CROWDIN_CONFIG_PATH = Path("crowdin.yml")


@dataclass(frozen=True)
class FileTask:
    repo_name: str
    repo_path: str
    file_path: str
    branch: str | None
    translate_attributes: bool


def load_repos_config(path: Path) -> tuple[str | None, list[dict]]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    repos = data.get("repos")
    if not repos:
        raise ValueError(f"{path} has no entries under 'repos'")

    return data.get("default_branch"), repos


def build_tasks(default_branch: str | None, repos: list[dict]) -> list[FileTask]:
    tasks: list[FileTask] = []
    for repo in repos:
        name = repo.get("name")
        path = repo.get("path")
        files = repo.get("files") or []
        if not name or not path or not files:
            raise ValueError(f"Repo entry missing 'name'/'path'/'files': {repo!r}")

        branch = repo.get("branch", default_branch)
        translate_attributes = repo.get("translate_attributes", True)

        for file_path in files:
            tasks.append(
                FileTask(
                    repo_name=name,
                    repo_path=path,
                    file_path=file_path,
                    branch=branch,
                    translate_attributes=translate_attributes,
                )
            )

    tasks.sort(key=lambda t: (t.repo_name, t.file_path))
    return tasks


async def fetch_with_retry(client: AsyncClient, task: FileTask) -> Response:
    params = {"ref": task.branch} if task.branch else {}
    url = f"{task.repo_name}/contents/{task.file_path}"

    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            res = await client.get(url, params=params)
            if res.status_code == 200:
                return res
            if res.status_code in (403, 429) or res.status_code >= 500:
                raise HTTPStatusError(
                    f"HTTP {res.status_code}", request=res.request, response=res
                )
            raise RuntimeError(
                f"Failed to fetch {task.file_path} from {task.repo_name} "
                f"(branch: {task.branch}): HTTP {res.status_code} - {res.text}"
            )
        except (HTTPStatusError, TimeoutException) as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
                log.warning(
                    "Transient error fetching %s/%s (attempt %d/%d), retrying in %.1fs: %s",
                    task.repo_name,
                    task.file_path,
                    attempt,
                    MAX_RETRIES,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)

    raise RuntimeError(
        f"Failed to fetch {task.file_path} from {task.repo_name} after {MAX_RETRIES} attempts: {last_exc}"
    )


async def fetch_all(tasks: list[FileTask], github_token: str) -> list[Response]:
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def bound_fetch(client: AsyncClient, task: FileTask) -> Response:
        async with semaphore:
            return await fetch_with_retry(client, task)

    async with AsyncClient(
        base_url="https://api.github.com/repos",
        headers={
            "Accept": "application/vnd.github.object",
            "X-GitHub-Api-Version": "2022-11-28",
            "Authorization": f"Bearer {github_token}",
        },
        timeout=30,
    ) as client:
        results = await asyncio.gather(
            *(bound_fetch(client, task) for task in tasks),
            return_exceptions=True,
        )

    errors = [r for r in results if isinstance(r, BaseException)]
    if errors:
        for err in errors:
            log.error(str(err))
        raise RuntimeError(f"{len(errors)}/{len(tasks)} files failed to download, see log above.")

    return results  # type: ignore[return-value]


def write_source_file(task: FileTask, response: Response) -> None:
    content = response.json()["content"]
    decoded = b64decode(content)

    dest = SOURCE_DIR / task.repo_path / task.file_path
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(decoded)
    log.info("Saved %s", dest)


def build_crowdin_entry(task: FileTask) -> dict[str, str | int]:
    translation_file = task.file_path.replace("values", "values-%android_code%")
    entry: dict[str, str | int] = {
        "source": f"/source/{task.repo_path}/{task.file_path}",
        "translation": f"/overlay/{task.repo_path}/{translation_file}",
    }
    if not task.translate_attributes:
        entry["translate_attributes"] = 0
    return entry


async def main() -> None:
    github_token = os.environ.get("X_GITHUB_TOKEN")
    if not github_token:
        log.error("Missing X_GITHUB_TOKEN environment variable")
        sys.exit(1)

    default_branch, repos = load_repos_config(REPOS_CONFIG_PATH)
    tasks = build_tasks(default_branch, repos)
    log.info("Fetching %d files from %d repos...", len(tasks), len(repos))

    responses = await fetch_all(tasks, github_token)

    files_entries = []
    for task, response in zip(tasks, responses):
        write_source_file(task, response)
        files_entries.append(build_crowdin_entry(task))

    CROWDIN_CONFIG_PATH.write_text(
        yaml.safe_dump({"files": files_entries}, sort_keys=False),
        encoding="utf-8",
    )
    log.info("Wrote %s (%d entries)", CROWDIN_CONFIG_PATH, len(files_entries))


if __name__ == "__main__":
    asyncio.run(main())