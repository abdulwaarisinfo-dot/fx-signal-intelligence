"""
apply_hn_patch.py
=================
flintel_service.py mein Hacker News ki SIRF 3 chhoti changes lagata hai aur
phir asli unified diff print karta hai (taake aap verify kar sakein).

Usage:
    python apply_hn_patch.py path/to/flintel_service.py

Safety:
  - Har anchor file mein EXACTLY ek baar milna chahiye, warna kuch bhi likhe
    baghair ruk jata hai (file untouched).
  - Pehle backup `flintel_service.py.bak` banta hai.
  - Dobara chalane par ("run_hackernews_for_job" pehle se maujood) kuch nahi karta.
  - File ka line-ending style (LF / CRLF) preserve hota hai.
"""

import difflib
import shutil
import sys

# ── Change 1: imports (numpy try/except ke baad) ────────────────────────────
IMPORT_ANCHOR = (
    "try:  # numpy is optional; array.array fallback is used if missing\n"
    "    import numpy as _np\n"
    "except ImportError:  # pragma: no cover\n"
    "    _np = None\n"
)
IMPORT_ADD = (
    "\n"
    "try:\n"
    "    from hackernews import run_hackernews_for_job\n"
    "    from hackernews import config as hn_config\n"
    "except Exception:  # pragma: no cover\n"
    "    run_hackernews_for_job = None\n"
    "    hn_config = None\n"
)

# ── Change 2: process_job() — Twitter block ke baad, [JOB] DONE se pehle ────
DONE_ANCHOR = '    log.info(f"[JOB] DONE | topic_key={topic_key} | new_messages_saved={matched_count}'
DONE_ADD = (
    "    if run_hackernews_for_job is not None:\n"
    "        try:\n"
    "            matched_count += run_hackernews_for_job(job, _save_signal, cutoff)\n"
    "        except Exception as exc:\n"
    '            log.error(f"[JOB] hackernews error | topic_key={topic_key} | {exc}")\n'
    "\n"
)

# ── Change 3: __main__ banner — Twitter/X line ke baad ──────────────────────
BANNER_ANCHOR = (
    "(checked live from .env every job, no restart needed to change)\")\n"
)
BANNER_ADD = (
    "    log.info(f\"  Hacker News       : {'ENABLED' if (hn_config and hn_config.is_hackernews_enabled()) "
    "else 'DISABLED'} (HACKERNEWS_ENABLED, live-checked, job-driven via Algolia API)\")\n"
)


def _once(text: str, anchor: str, label: str) -> int:
    n = text.count(anchor)
    if n != 1:
        sys.exit(f"ABORT: anchor for '{label}' found {n} time(s), expected exactly 1. File untouched.")
    return text.index(anchor)


def main():
    if len(sys.argv) != 2:
        sys.exit("Usage: python apply_hn_patch.py path/to/flintel_service.py")
    path = sys.argv[1]

    with open(path, "r", encoding="utf-8", newline="") as f:
        original = f.read()

    if "run_hackernews_for_job" in original:
        sys.exit("Already patched (run_hackernews_for_job found). Nothing to do.")

    nl = "\r\n" if "\r\n" in original else "\n"
    fix = lambda s: s.replace("\n", nl)

    text = original

    # 1. imports
    anchor = fix(IMPORT_ANCHOR)
    pos = _once(text, anchor, "imports (numpy block)") + len(anchor)
    text = text[:pos] + fix(IMPORT_ADD) + text[pos:]

    # 2. process_job
    anchor = fix(DONE_ANCHOR)
    pos = _once(text, anchor, "process_job [JOB] DONE")
    text = text[:pos] + fix(DONE_ADD) + text[pos:]

    # 3. banner
    anchor = fix(BANNER_ANCHOR)
    pos = _once(text, anchor, "startup banner (Twitter/X line)") + len(anchor)
    text = text[:pos] + fix(BANNER_ADD) + text[pos:]

    shutil.copyfile(path, path + ".bak")
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(text)

    diff = difflib.unified_diff(
        original.splitlines(keepends=True),
        text.splitlines(keepends=True),
        fromfile=path + ".bak",
        tofile=path,
        n=3,
    )
    sys.stdout.writelines(diff)
    print(f"\nDone. Backup: {path}.bak")


if __name__ == "__main__":
    main()
