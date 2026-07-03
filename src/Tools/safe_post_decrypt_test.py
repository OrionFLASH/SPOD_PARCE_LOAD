# -*- coding: utf-8 -*-
"""
Безопасная проверка pack → decrypt в .work/ (не трогает IN/ и OUT/).

Запуск из корня:
  python src/Tools/safe_post_decrypt_test.py
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PACK = ROOT / "src" / "Tools" / "pack_post_encrypted_program.py"
DECRYPT = ROOT / "decrypt_post_program.py"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.path_data_guard import post_decrypt_test_dirs, project_work_root  # noqa: E402


def _reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def main() -> int:
    if not PACK.is_file() or not DECRYPT.is_file():
        print("Не найдены pack/decrypt скрипты", file=sys.stderr)
        return 1

    project_work_root(ROOT).mkdir(parents=True, exist_ok=True)
    in_post, out_post = post_decrypt_test_dirs(ROOT)

    py = sys.executable
    rc = subprocess.call([py, str(PACK)], cwd=ROOT)
    if rc != 0:
        return rc

    post_dir = ROOT / "POST"
    if not post_dir.is_dir():
        print("POST/ не создан после pack", file=sys.stderr)
        return 1

    _reset_dir(in_post)
    for item in post_dir.iterdir():
        dest = in_post / item.name
        if item.is_file():
            shutil.copy2(item, dest)
        elif item.is_dir():
            shutil.copytree(item, dest, dirs_exist_ok=True)

    _reset_dir(out_post)
    rc = subprocess.call(
        [py, str(DECRYPT), "--input", str(in_post), "--output", str(out_post)],
        cwd=ROOT,
    )
    if rc != 0:
        return rc

    restored = sum(1 for _ in out_post.rglob("*") if _.is_file())
    if restored < 10:
        print(f"Слишком мало файлов в {out_post}: {restored}", file=sys.stderr)
        return 1

    print(f"OK: roundtrip в {out_post.relative_to(ROOT)} — {restored} файлов")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
