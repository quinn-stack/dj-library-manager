"""
DJ Library Manager — Tag Utilities  (v0.5.3)

Shared tag-validation helpers used by multiple engines.

Extracted from engine/tagging.py and engine/duplicate_finder.py at v0.5.3
to eliminate a duplicated `_looks_reasonable()` implementation that was
diverging silently between the two files.

── USED BY OTHER ENGINES ────────────────────────────────────────────────────
looks_reasonable() is imported by:
  - engine/tagging.py          →  _looks_reasonable() (via alias)
  - engine/duplicate_finder.py →  _looks_reasonable() (via alias)

If you add or remove placeholder values from the PLACEHOLDER_TAGS set, or
change the fullmatch pattern, update and test BOTH callers. The function is
the single gating check that decides whether a tag is real enough to use for
file renaming (tagging.py) and duplicate bucketing (duplicate_finder.py).
─────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations

import re

# Canonical set of tag values that are considered meaningless placeholders.
# Matched case-insensitively against stripped tag text.
#
# Maintenance note: keep this list in sync with _PLACEHOLDER_STEMS in
# duplicate_finder.py. The two lists serve different purposes (PLACEHOLDER_STEMS
# is applied to *filename stems*, PLACEHOLDER_TAGS to *tag values*) but they
# should agree on what counts as a known placeholder string.
PLACEHOLDER_TAGS: frozenset[str] = frozenset({
    "unknown",
    "various",
    "various artists",
    "untagged",
    "track",
    "-",
    "?",
    "untitled",
    "no title",
    "unknown artist",
    "unknown track",
})


def looks_reasonable(tag: str) -> bool:
    """Return True if `tag` looks like real metadata rather than a placeholder.

    A tag is considered unreasonable (returns False) if it is:
      - Falsy / empty / whitespace-only
      - A known placeholder string (see PLACEHOLDER_TAGS)
      - A pure digit string (e.g. "1", "04") — these are track numbers, not names
      - Fewer than 2 characters after stripping

    This is the single authoritative implementation. Previously duplicated
    between engine/tagging.py and engine/duplicate_finder.py — extracted here
    at v0.5.3 so both callers share one definition.

    Args:
        tag: Raw tag value string from mutagen or similar reader.

    Returns:
        True if the tag looks like a real artist/title value worth using.
    """
    if not tag:
        return False
    t = tag.strip().lower()
    if not t:
        return False
    if t in PLACEHOLDER_TAGS:
        return False
    if re.fullmatch(r"\d+", t):
        return False
    if len(t) < 2:
        return False
    return True
