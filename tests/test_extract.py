"""Extract tests: tiny zip built in tmp_path, fake HTTP session, no network."""

import logging
import zipfile
from pathlib import Path
from typing import Self

import pytest
import requests

from uk_crime import config, extract

CSV_A = b"Crime ID,Month\nabc,2026-07\n"
CSV_B = b"Crime ID,Month\ndef,2026-08\n"


def make_zip(path: Path, members: dict[str, bytes]) -> Path:
    with zipfile.ZipFile(path, "w") as z:
        for name, data in members.items():
            z.writestr(name, data)
    return path


def tiny_members() -> dict[str, bytes]:
    return {
        "2026-07/2026-07-metropolitan-street.csv": CSV_A,
        "2026-08/2026-08-metropolitan-street.csv": CSV_B,
        "2026-08/2026-08-city-of-london-street.csv": CSV_B,
        "2026-08/2026-08-metropolitan-outcomes.csv": b"outcomes",
        "2026-08/2026-08-metropolitan-stop-and-search.csv": b"sas",
        "2026-08/2026-08-kent-street.csv": b"kent",
    }


class FakeResponse:
    def __init__(
        self,
        status: int = 200,
        body: bytes = b"",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status
        self._body = body
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}", response=self)  # type: ignore[arg-type]

    def iter_content(self, chunk_size: int) -> list[bytes]:
        return [self._body[i : i + 7] for i in range(0, len(self._body), 7)]

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


class FakeSession:
    """Returns queued responses in order and records the calls."""

    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, str]] = []

    def _next(self, verb: str, url: str) -> FakeResponse:
        self.calls.append((verb, url))
        return self.responses.pop(0)

    def get(self, url: str, **_: object) -> FakeResponse:
        return self._next("GET", url)

    def head(self, url: str, **_: object) -> FakeResponse:
        return self._next("HEAD", url)


@pytest.fixture(autouse=True)
def isolated_dirs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "LANDING_DIR", tmp_path / "landing")
    monkeypatch.setattr(config, "DOWNLOADS_DIR", tmp_path / "_downloads")
    monkeypatch.setattr(config, "TMP_DIR", tmp_path / "_tmp")
    monkeypatch.setattr(config, "KEEP_ARCHIVES", True)
    monkeypatch.setattr(extract.time, "sleep", lambda _s: None)


def zip_session(tmp_path: Path, members: dict[str, bytes] | None = None) -> FakeSession:
    body = make_zip(tmp_path / "src.zip", members or tiny_members()).read_bytes()
    return FakeSession([FakeResponse(200, body)])


# ------------------------------------------------------------------ plan_members


def test_plan_members_selects_only_configured_street_files() -> None:
    plan = extract.plan_members(
        list(tiny_members()), ["metropolitan", "city-of-london"]
    )
    assert [(f, m) for _n, f, m in plan] == [
        ("metropolitan", "2026-07"),  # sorted by member name
        ("city-of-london", "2026-08"),
        ("metropolitan", "2026-08"),
    ]


def test_plan_members_warns_on_month_gap(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        extract.plan_members(list(tiny_members()), ["city-of-london"])
    assert "No street file for city-of-london in 2026-07" in caplog.text


def test_plan_members_fails_when_force_has_no_files() -> None:
    with pytest.raises(extract.ExtractError, match="greater-manchester"):
        extract.plan_members(
            list(tiny_members()), ["metropolitan", "greater-manchester"]
        )


def test_plan_members_ignores_unexpected_names() -> None:
    names = [
        "../evil-street.csv",
        "2026-08/2026-07-metropolitan-street.csv",
        "readme.txt",
    ]
    with pytest.raises(extract.ExtractError):
        extract.plan_members(names, ["metropolitan"])


# --------------------------------------------------------------------- zip checks


def test_verify_zip_rejects_empty_and_corrupt(tmp_path: Path) -> None:
    empty = tmp_path / "empty.zip"
    empty.write_bytes(b"")
    junk = tmp_path / "junk.zip"
    junk.write_bytes(b"not a zip")
    for bad in (empty, junk, tmp_path / "missing.zip"):
        with pytest.raises(extract.ExtractError):
            extract.verify_zip(bad)


def test_verify_zip_accepts_valid_zip(tmp_path: Path) -> None:
    extract.verify_zip(make_zip(tmp_path / "ok.zip", tiny_members()))


# -------------------------------------------------------------------------- HTTP


def test_resolve_latest_reads_redirect_location() -> None:
    redirect = FakeResponse(
        302, headers={"Location": "https://bucket.s3.amazonaws.com/archive/2026-08.zip"}
    )
    assert extract.resolve_latest_archive_month(FakeSession([redirect])) == "2026-08"  # type: ignore[arg-type]


def test_resolve_latest_fails_without_redirect() -> None:
    with pytest.raises(extract.ExtractError):
        extract.resolve_latest_archive_month(FakeSession([FakeResponse(200)]))  # type: ignore[arg-type]


def test_retries_5xx_then_succeeds(tmp_path: Path) -> None:
    body = make_zip(tmp_path / "src.zip", tiny_members()).read_bytes()
    session = FakeSession([FakeResponse(503), FakeResponse(200, body)])
    path = extract.download_archive(session, "2026-08", tmp_path / "dl")  # type: ignore[arg-type]
    assert path.read_bytes() == body
    assert len(session.calls) == 2


def test_does_not_retry_4xx(tmp_path: Path) -> None:
    session = FakeSession([FakeResponse(404), FakeResponse(200)])
    with pytest.raises(requests.HTTPError):
        extract.download_archive(session, "2026-08", tmp_path / "dl")  # type: ignore[arg-type]
    assert len(session.calls) == 1
    assert list((tmp_path / "dl").iterdir()) == []  # no .part left behind


def test_gives_up_after_max_attempts(tmp_path: Path) -> None:
    session = FakeSession([FakeResponse(500)] * config.HTTP_MAX_ATTEMPTS)
    with pytest.raises(requests.HTTPError):
        extract.download_archive(session, "2026-08", tmp_path / "dl")  # type: ignore[arg-type]
    assert len(session.calls) == config.HTTP_MAX_ATTEMPTS


def test_corrupt_download_is_rejected_and_not_kept(tmp_path: Path) -> None:
    session = FakeSession([FakeResponse(200, b"not a zip at all")])
    with pytest.raises(extract.ExtractError):
        extract.download_archive(session, "2026-08", tmp_path / "dl")  # type: ignore[arg-type]
    assert list((tmp_path / "dl").iterdir()) == []


# ------------------------------------------------------------------ extract_archive


def landed_files() -> dict[str, bytes]:
    root = config.LANDING_DIR
    return {
        str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob("*.csv"))
    }


def test_extract_lands_files_untouched_in_expected_layout(tmp_path: Path) -> None:
    count = extract.extract_archive(
        zip_session(tmp_path),  # type: ignore[arg-type]
        "2026-08",
        forces=["metropolitan", "city-of-london"],
    )
    assert count == 3
    assert landed_files() == {
        "archive=2026-08/month=2026-07/2026-07-metropolitan-street.csv": CSV_A,
        "archive=2026-08/month=2026-08/2026-08-city-of-london-street.csv": CSV_B,
        "archive=2026-08/month=2026-08/2026-08-metropolitan-street.csv": CSV_B,
    }
    assert not config.TMP_DIR.exists() or not any(config.TMP_DIR.iterdir())


def test_rerun_is_skipped_without_network(tmp_path: Path) -> None:
    extract.extract_archive(zip_session(tmp_path), "2026-08", forces=["metropolitan"])  # type: ignore[arg-type]
    before = landed_files()
    offline = FakeSession([])  # any request would raise IndexError
    assert extract.extract_archive(offline, "2026-08", forces=["metropolitan"]) == 0  # type: ignore[arg-type]
    assert offline.calls == []
    assert landed_files() == before


def test_force_replaces_previous_copy(tmp_path: Path) -> None:
    extract.extract_archive(zip_session(tmp_path), "2026-08", forces=["metropolitan"])  # type: ignore[arg-type]
    stale = config.LANDING_DIR / "archive=2026-08" / "month=2026-08" / "stale.csv"
    stale.write_bytes(b"stale")
    before = {k: v for k, v in landed_files().items() if "stale" not in k}

    extract.extract_archive(
        FakeSession([]),  # type: ignore[arg-type]
        "2026-08",
        force=True,
        forces=["metropolitan"],  # cached zip is reused: no network needed
    )
    assert not stale.exists()
    assert landed_files() == before


def test_failed_extract_leaves_landing_untouched(tmp_path: Path) -> None:
    session = zip_session(tmp_path)
    with pytest.raises(extract.ExtractError):
        extract.extract_archive(session, "2026-08", forces=["greater-manchester"])  # type: ignore[arg-type]
    assert not config.LANDING_DIR.exists()


def test_keep_archives_false_deletes_zip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(config, "KEEP_ARCHIVES", False)
    extract.extract_archive(zip_session(tmp_path), "2026-08", forces=["metropolitan"])  # type: ignore[arg-type]
    assert not (config.DOWNLOADS_DIR / "2026-08.zip").exists()


def test_keep_archives_true_keeps_zip(tmp_path: Path) -> None:
    extract.extract_archive(zip_session(tmp_path), "2026-08", forces=["metropolitan"])  # type: ignore[arg-type]
    assert (config.DOWNLOADS_DIR / "2026-08.zip").exists()


def test_main_rejects_bad_archive_month() -> None:
    with pytest.raises(SystemExit) as exc:
        extract.main(["--archive", "2026-13"])
    assert exc.value.code == 2


def test_main_requires_archive_or_latest() -> None:
    with pytest.raises(SystemExit) as exc:
        extract.main([])
    assert exc.value.code == 2
