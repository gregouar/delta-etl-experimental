from collections.abc import Iterable
from pathlib import Path
import gzip


def upload_file(folder: str, filename: str, file_content: bytes) -> None:
    """Upload file to datalake."""
    path = Path("local/bronze") / folder / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(file_content))


def download_file(folder: str, filename: str) -> bytes:
    """Download file from datalake."""
    return gzip.decompress((Path("local/bronze") / folder / filename).read_bytes())

def glob_folder(folder: str) -> Iterable[str]:
    """Glob """
    for filename in (Path("local/bronze") / folder).glob("*"):
        yield filename.name