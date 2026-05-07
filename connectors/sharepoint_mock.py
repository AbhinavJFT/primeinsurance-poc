"""Mock SharePoint client.

Exposes the same surface a real Microsoft Graph SharePoint client would
(`list_files`, `download_file`, `upload_file`, `ensure_folder`), but reads
and writes a local directory tree under data/mock_sharepoint/. Swapping
to a real SharePoint connection later means replacing this one class.

Pattern mirrors dpdp/generate_salesforce_data.py: a self-contained module
that pretends to be the external system so the rest of the pipeline can
be developed and demoed without external credentials.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MOCK_ROOT = REPO_ROOT / "data" / "mock_sharepoint"


@dataclass
class SharePointFile:
    name: str
    path: str
    size_bytes: int
    site: str
    folder: str


class SharePointMock:
    """Local stand-in for a SharePoint document library."""

    def __init__(self, root: Path | str | None = None, site: str = "QualityTeam"):
        self.root = Path(root) if root else DEFAULT_MOCK_ROOT
        self.site = site
        self.root.mkdir(parents=True, exist_ok=True)

    def _resolve(self, folder: str) -> Path:
        target = self.root / folder.strip("/")
        target.mkdir(parents=True, exist_ok=True)
        return target

    def ensure_folder(self, folder: str) -> str:
        return str(self._resolve(folder))

    def list_files(self, folder: str, suffix: str | None = None) -> list[SharePointFile]:
        folder_path = self._resolve(folder)
        results: list[SharePointFile] = []
        for p in sorted(folder_path.iterdir()):
            if not p.is_file():
                continue
            if p.name.startswith("."):
                continue
            if suffix and not p.name.lower().endswith(suffix.lower()):
                continue
            results.append(
                SharePointFile(
                    name=p.name,
                    path=str(p),
                    size_bytes=p.stat().st_size,
                    site=self.site,
                    folder=folder,
                )
            )
        return results

    def download_file(self, folder: str, name: str, dest: Path | str) -> Path:
        src = self._resolve(folder) / name
        if not src.exists():
            raise FileNotFoundError(f"SharePoint mock: {folder}/{name} not found")
        dest_path = Path(dest)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest_path)
        return dest_path

    def upload_file(self, folder: str, src: Path | str, name: str | None = None) -> SharePointFile:
        src_path = Path(src)
        if not src_path.exists():
            raise FileNotFoundError(f"Local file not found: {src_path}")
        dest_dir = self._resolve(folder)
        target = dest_dir / (name or src_path.name)
        # Allow `name` to include a subfolder (e.g. "cleaned/foo.xlsx").
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_path, target)
        return SharePointFile(
            name=target.name,
            path=str(target),
            size_bytes=target.stat().st_size,
            site=self.site,
            folder=folder,
        )

    def write_bytes(self, folder: str, name: str, payload: bytes) -> SharePointFile:
        target = self._resolve(folder) / name
        target.write_bytes(payload)
        return SharePointFile(
            name=target.name,
            path=str(target),
            size_bytes=target.stat().st_size,
            site=self.site,
            folder=folder,
        )

    def __repr__(self) -> str:
        return f"SharePointMock(site={self.site!r}, root={self.root})"


def default_client() -> SharePointMock:
    return SharePointMock()
