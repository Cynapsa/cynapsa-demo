"""Bounded, read-only UTF-8 access beneath one trusted database directory."""
from __future__ import annotations

import os
import re
import stat
from pathlib import Path, PurePosixPath

EXTENSIONS = {".txt", ".md", ".csv", ".json"}
MAX_FILE_BYTES = 1_048_576
MAX_SEARCH_BYTES = 4_194_304
MAX_ENTRIES = 4_000
MAX_FILES = 1_000


class FileDatabase:
    def __init__(self, directory: str | Path):
        self.root = Path(directory)
        if not self.root.is_absolute() or not stat.S_ISDIR(self.root.lstat().st_mode):
            raise ValueError("database must be an absolute directory, not a symlink")

    def _open(self, relative: str, *, directory: bool = False) -> int:
        # Descriptor-relative traversal rejects symlinks at every component,
        # including replacements between listing and reading. Never open FIFOs.
        path = PurePosixPath(relative)
        parts = path.parts
        if (path.is_absolute() or "\\" in relative or "\x00" in relative
                or any(p in {"..", "."} or p.startswith(".") for p in parts)):
            raise ValueError("invalid database path")
        if not directory and (not parts or path.suffix.lower() not in EXTENSIONS):
            raise ValueError("unsupported database file")
        fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for index, part in enumerate(parts):
                is_dir = directory or index < len(parts) - 1
                flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
                if is_dir:
                    flags |= os.O_DIRECTORY
                next_fd = os.open(part, flags, dir_fd=fd)
                os.close(fd)
                fd = next_fd
            return fd
        except BaseException:
            os.close(fd)
            raise

    def list_files(self) -> dict:
        pending = [""]
        files = []
        count = 0
        truncated = False
        while pending and count < MAX_ENTRIES and len(files) < MAX_FILES:
            relative = pending.pop()
            try:
                fd = self._open(relative, directory=True)
            except OSError:
                continue
            try:
                with os.scandir(fd) as entries:
                    for entry in entries:
                        count += 1
                        if count > MAX_ENTRIES or len(files) >= MAX_FILES:
                            truncated = True
                            break
                        if entry.name.startswith(".") or "\\" in entry.name or entry.is_symlink():
                            continue
                        path = f"{relative}/{entry.name}".lstrip("/")
                        if entry.is_dir(follow_symlinks=False):
                            if len(PurePosixPath(path).parts) < 8:
                                pending.append(path)
                            else:
                                truncated = True
                        elif (entry.is_file(follow_symlinks=False)
                              and Path(entry.name).suffix.lower() in EXTENSIONS):
                            files.append(path)
            finally:
                os.close(fd)
        return {"files": sorted(files), "truncated": truncated or bool(pending)}

    def _bytes(self, path: str, budget: int = MAX_FILE_BYTES) -> bytes:
        try:
            fd = self._open(path)
            with os.fdopen(fd, "rb") as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_size > min(MAX_FILE_BYTES, budget):
                    raise ValueError("file is not regular or exceeds the read budget")
                data = stream.read(min(MAX_FILE_BYTES, budget) + 1)
                if len(data) > min(MAX_FILE_BYTES, budget):
                    raise ValueError("file exceeds the read budget")
            return data
        except OSError as exc:
            raise ValueError("file unavailable") from exc

    @staticmethod
    def _decode(data: bytes) -> str:
        try:
            text = data.decode("utf-8")
        except UnicodeError as exc:
            raise ValueError("file is not UTF-8 text") from exc
        if "\x00" in text:
            raise ValueError("binary file is unsupported")
        return text

    def read_file(self, path: str, start_line: int = 1, end_line: int = 40) -> dict:
        if (type(start_line) is not int or type(end_line) is not int
                or start_line < 1 or end_line < start_line or end_line - start_line >= 100):
            raise ValueError("read at most 100 lines using positive line numbers")
        text = self._decode(self._bytes(path))
        lines = text.splitlines()
        selected = lines[start_line - 1:end_line]
        excerpt = "\n".join(f"{i}: {line[:500]}" for i, line in enumerate(selected, start_line))
        return {"path": path, "start_line": start_line,
                "end_line": min(end_line, len(lines)), "excerpt": excerpt[:8_000],
                "truncated": len(excerpt) > 8_000 or any(len(line) > 500 for line in selected)}

    def search_files(self, query: str) -> dict:
        if not isinstance(query, str) or not 1 <= len(query.strip()) <= 200:
            raise ValueError("search query must contain 1–200 characters")
        terms = set(re.findall(r"\w+", query.casefold()))
        if not terms:
            raise ValueError("search query must contain letters or numbers")
        listing = self.list_files()
        matches = []
        budget = MAX_SEARCH_BYTES
        skipped = 0
        for path in listing["files"]:
            if budget <= 0 or len(matches) >= 20:
                break
            try:
                data = self._bytes(path, budget)
                # Charge invalid UTF-8/binary reads too: malformed databases
                # must not evade the total read budget.
                budget -= len(data)
                text = self._decode(data)
            except ValueError:
                skipped += 1
                continue
            for number, line in enumerate(text.splitlines(), 1):
                score = sum(term in line.casefold() for term in terms)
                if score:
                    matches.append({"path": path, "line": number,
                                    "excerpt": line[:500], "score": score})
                    if len(matches) >= 20:
                        break
        matches.sort(key=lambda item: -item["score"])
        return {"matches": matches, "skipped_files": skipped,
                "truncated": listing["truncated"] or budget <= 0 or len(matches) >= 20}
