"""Extract (E): download police.uk monthly archives and land the street CSVs untouched.

Landing layout: <LANDING_DIR>/archive=YYYY-MM/month=YYYY-MM/<original file name>.
The archive month is kept because silver picks, per (force, data month), the file from
the newest archive. Only finished CSVs live under landing/; zips go to _downloads/ and
in-progress work to _tmp/.
"""

import argparse
import logging
import re
import shutil
import time
import zipfile
from collections import defaultdict
from collections.abc import Callable, Sequence
from pathlib import Path

import requests

from uk_crime import config

logger = logging.getLogger(__name__)

MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
# Member names look like 2026-08/2026-08-metropolitan-street.csv
STREET_MEMBER_RE = re.compile(r"^(\d{4}-\d{2})/\1-(?P<force>[a-z0-9-]+)-street\.csv$")
LATEST_REDIRECT_RE = re.compile(r"/archive/(\d{4}-\d{2})\.zip$")
CHUNK_SIZE = 1024 * 1024
RETRYABLE = (
    requests.Timeout,
    requests.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
)


class ExtractError(RuntimeError):
    """An expected, reportable extract failure (bad archive, missing force, 4xx)."""


# --------------------------------------------------------------------------- HTTP


def with_retries[T](action: Callable[[], T], what: str) -> T:
    """Run action, retrying with exponential backoff on 5xx, timeouts and connection errors.

    Inputs: a zero-argument callable and a label for logs.
    Output: the callable's result. 4xx responses are never retried (raised as-is).
    Side effects: sleeps between attempts; raises the last error when attempts run out.
    """
    for attempt in range(1, config.HTTP_MAX_ATTEMPTS + 1):
        try:
            return action()
        except requests.HTTPError as err:
            status = err.response.status_code if err.response is not None else 0
            if status < 500 or attempt == config.HTTP_MAX_ATTEMPTS:
                raise
            reason = f"HTTP {status}"
        except RETRYABLE as err:
            if attempt == config.HTTP_MAX_ATTEMPTS:
                raise
            reason = type(err).__name__
        delay = 2**attempt
        logger.warning("%s failed (%s); retry %d in %ds", what, reason, attempt, delay)
        time.sleep(delay)
    raise AssertionError("unreachable")  # loop always returns or raises


def resolve_latest_archive_month(session: requests.Session) -> str:
    """Return the newest archive month (YYYY-MM) by following the latest.zip redirect.

    Inputs: an HTTP session. Output: e.g. "2026-08". Side effects: one HEAD request.
    """
    url = f"{config.ARCHIVE_BASE_URL}/latest.zip"

    def head() -> requests.Response:
        response = session.head(url, allow_redirects=False, timeout=config.HTTP_TIMEOUT)
        response.raise_for_status()
        return response

    response = with_retries(head, "HEAD latest.zip")
    location = response.headers.get("Location", "")
    match = LATEST_REDIRECT_RE.search(location)
    if not (300 <= response.status_code < 400 and match):
        raise ExtractError(f"latest.zip did not redirect to an archive: {location!r}")
    return match.group(1)


# ------------------------------------------------------------------- download/verify


def verify_zip(path: Path) -> None:
    """Raise ExtractError unless path is a non-empty, readable, intact zip file."""
    if not path.is_file() or path.stat().st_size == 0:
        raise ExtractError(f"{path} is missing or empty")
    try:
        with zipfile.ZipFile(path) as archive:
            bad_member = archive.testzip()
    except zipfile.BadZipFile as err:
        raise ExtractError(f"{path} is not a valid zip: {err}") from err
    if bad_member is not None:
        raise ExtractError(f"{path} failed CRC check on member {bad_member}")


def download_archive(
    session: requests.Session, archive_month: str, downloads_dir: Path
) -> Path:
    """Download YYYY-MM.zip into downloads_dir unless a verified copy is already there.

    Output: path of the verified zip. Side effects: streams to a .part file (never the
    whole archive in memory), verifies it, then atomically renames it into place.
    """
    final = downloads_dir / f"{archive_month}.zip"
    if final.exists():
        try:
            verify_zip(final)
            logger.info("Reusing downloaded %s", final)
            return final
        except ExtractError as err:
            logger.warning("Discarding bad cached zip: %s", err)
            final.unlink()

    downloads_dir.mkdir(parents=True, exist_ok=True)
    part = final.with_suffix(".zip.part")
    url = f"{config.ARCHIVE_BASE_URL}/{archive_month}.zip"

    def fetch() -> None:
        logger.info("Downloading %s", url)
        with session.get(url, stream=True, timeout=config.HTTP_TIMEOUT) as response:
            response.raise_for_status()
            with part.open("wb") as out:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    out.write(chunk)

    try:
        with_retries(fetch, f"GET {archive_month}.zip")
        verify_zip(part)
    except (requests.RequestException, ExtractError):
        part.unlink(missing_ok=True)
        raise
    part.rename(final)  # atomic: a half-written zip is never visible as <month>.zip
    logger.info("Downloaded %s (%d bytes)", final, final.stat().st_size)
    return final


# ----------------------------------------------------------------------- plan/land


def plan_members(
    member_names: Sequence[str], forces: Sequence[str]
) -> list[tuple[str, str, str]]:
    """Choose the street CSVs to land (pure function).

    Inputs: all zip member names and the configured force slugs.
    Output: sorted (member_name, force, data_month) for configured forces only.
    Raises ExtractError if a configured force has no street file at all; logs a warning
    for each (force, month) gap relative to the months present in the archive.
    """
    by_force: dict[str, dict[str, str]] = defaultdict(dict)
    all_months: set[str] = set()
    for name in member_names:
        match = STREET_MEMBER_RE.match(name)
        if match:
            month = match.group(1)
            all_months.add(month)
            by_force[match.group("force")][month] = name

    selected: list[tuple[str, str, str]] = []
    for force in forces:
        files = by_force.get(force, {})
        if not files:
            raise ExtractError(f"Configured force {force!r} has no street files")
        for month in sorted(all_months - files.keys()):
            logger.warning("No street file for %s in %s", force, month)
        selected += [(name, force, month) for month, name in files.items()]
    return sorted(selected)


def build_archive_folder(
    zip_path: Path,
    members: Sequence[tuple[str, str, str]],
    work_dir: Path,
) -> int:
    """Stream the chosen members out of the zip into work_dir/archive=YYYY-MM/.

    Output: number of files written. Files are copied byte-for-byte under their original
    names; each written size is checked against the zip's recorded size. Destination
    paths are built from validated parts, never from raw member paths.
    """
    if work_dir.exists():
        shutil.rmtree(work_dir)
    with zipfile.ZipFile(zip_path) as archive:
        for name, _force, month in members:
            target_dir = work_dir / f"month={month}"
            target_dir.mkdir(parents=True, exist_ok=True)
            target = target_dir / Path(name).name
            with archive.open(name) as src, target.open("wb") as dst:
                shutil.copyfileobj(src, dst, CHUNK_SIZE)
            expected = archive.getinfo(name).file_size
            if target.stat().st_size != expected:
                raise ExtractError(
                    f"{name}: wrote {target.stat().st_size}, expected {expected}"
                )
    return len(members)


def publish_folder(built: Path, final: Path, tmp_dir: Path) -> None:
    """Move a finished archive folder into landing, replacing any previous copy.

    Both paths must be on one filesystem (they share DATA_DIR). The old copy is renamed
    aside first, so landing never holds a half-written folder.
    """
    final.parent.mkdir(parents=True, exist_ok=True)
    old = tmp_dir / f"_replaced-{final.name}"
    if old.exists():
        shutil.rmtree(old)
    if final.exists():
        final.rename(old)
    built.rename(final)
    shutil.rmtree(old, ignore_errors=True)


def extract_archive(
    session: requests.Session,
    archive_month: str,
    force: bool = False,
    forces: Sequence[str] | None = None,
) -> int:
    """Land the street CSVs of one archive. Returns files landed (0 if skipped).

    Skips when landing/archive=YYYY-MM already exists unless force=True. Side effects:
    network download, writes under DOWNLOADS_DIR, TMP_DIR and LANDING_DIR.
    """
    forces = forces if forces is not None else config.FORCES
    final = config.LANDING_DIR / f"archive={archive_month}"
    if final.exists() and not force:
        logger.info(
            "Archive %s already landed at %s; skipping (use --force)",
            archive_month,
            final,
        )
        return 0

    zip_path = download_archive(session, archive_month, config.DOWNLOADS_DIR)
    with zipfile.ZipFile(zip_path) as archive:
        members = plan_members(archive.namelist(), forces)
    built = config.TMP_DIR / f"archive={archive_month}"
    count = build_archive_folder(zip_path, members, built)
    publish_folder(built, final, config.TMP_DIR)

    size = sum(p.stat().st_size for p in final.rglob("*.csv"))
    logger.info(
        "Landed %d street files (%d bytes) from archive %s for %s",
        count,
        size,
        archive_month,
        ", ".join(forces),
    )
    if not config.KEEP_ARCHIVES:
        zip_path.unlink()
        logger.info("Deleted %s (KEEP_ARCHIVES=false)", zip_path)
    return count


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point. Returns the process exit code (non-zero on failure)."""
    parser = argparse.ArgumentParser(
        prog="python -m uk_crime.extract", description=__doc__
    )
    which = parser.add_mutually_exclusive_group(required=True)
    which.add_argument("--archive", metavar="YYYY-MM", help="archive month to land")
    which.add_argument("--latest", action="store_true", help="resolve via latest.zip")
    parser.add_argument("--force", action="store_true", help="re-land even if present")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    if args.archive and not MONTH_RE.match(args.archive):
        parser.error("--archive must look like YYYY-MM")

    with requests.Session() as session:
        try:
            month = args.archive or resolve_latest_archive_month(session)
            extract_archive(session, month, force=args.force)
        except (ExtractError, requests.RequestException) as err:
            logger.error("Extract failed: %s", err)
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
