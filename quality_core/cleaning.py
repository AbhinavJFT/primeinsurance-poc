"""Value-level coercion and normalization.

Every coercion returns (cleaned_value, dq_rule_or_none) so the caller
can log a DQIssue. Returning the rule (not the full DQIssue) keeps this
module decoupled from workbook/sheet/row context.
"""

from __future__ import annotations

import re
from datetime import date, datetime, time
from typing import Any, Optional

NULL_SENTINELS = {"null", "?", "n/a", "na", "--", "-", "none"}
PASS_TOKENS = {"ok", "pass", "passed", "yes", "y", "true", "complies", "complied"}
FAIL_TOKENS = {"fail", "failed", "no", "n", "false", "out of spec", "oos"}


def coerce_str(value: Any) -> tuple[Optional[str], Optional[str]]:
    if value is None:
        return None, None
    s = str(value)
    stripped = s.strip()
    if not stripped:
        return None, None
    if stripped.lower() in NULL_SENTINELS:
        return None, "null_sentinel"
    rule = "whitespace" if stripped != s else None
    return stripped, rule


def coerce_appearance(value: Any) -> tuple[Optional[str], Optional[str]]:
    """Title-case appearance strings while preserving known multi-word phrases."""
    cleaned, rule = coerce_str(value)
    if cleaned is None:
        return None, rule
    canonical = " ".join(w.capitalize() if w.islower() or w.isupper() else w
                         for w in cleaned.split())
    final_rule = rule
    if canonical != cleaned and rule is None:
        final_rule = "case_variant"
    return canonical, final_rule


def coerce_pass_fail(value: Any) -> tuple[Optional[bool], Optional[str]]:
    cleaned, rule = coerce_str(value)
    if cleaned is None:
        return None, rule
    low = cleaned.lower()
    if low in PASS_TOKENS:
        return True, rule
    if low in FAIL_TOKENS:
        return False, rule
    return None, "unrecognized_token"


def coerce_date(value: Any) -> tuple[Optional[date], Optional[str]]:
    if value is None:
        return None, None
    if isinstance(value, datetime):
        return value.date(), None
    if isinstance(value, date):
        return value, None
    s = str(value).strip()
    if not s:
        return None, None
    if s.lower() in NULL_SENTINELS:
        return None, "null_sentinel"
    formats = [
        "%Y-%m-%d", "%Y/%m/%d",
        "%d/%m/%Y", "%d-%m-%Y",
        "%m/%d/%Y", "%m-%d-%Y",
        "%b %d %Y", "%d %b %Y",
        "%B %d %Y", "%d %B %Y",
        "%d-%b-%Y", "%d-%B-%Y",
    ]
    for fmt in formats:
        try:
            parsed = datetime.strptime(s, fmt).date()
            rule = None if fmt in ("%Y-%m-%d", "%Y/%m/%d") else "non_standard_date"
            return parsed, rule
        except ValueError:
            continue
    return None, "malformed_date"


def coerce_time(value: Any) -> tuple[Optional[time], Optional[str]]:
    if value is None:
        return None, None
    if isinstance(value, time):
        return value, None
    if isinstance(value, datetime):
        return value.time(), None
    s = str(value).strip()
    if not s:
        return None, None
    if s.lower() in NULL_SENTINELS or "??" in s:
        return None, "malformed_time"
    formats = ["%H:%M:%S", "%H:%M", "%I:%M %p", "%I:%M:%S %p"]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).time(), None
        except ValueError:
            continue
    # Things like "27:00.0" which Excel may have stored as a string
    if re.match(r"^\d+:\d+(\.\d+)?$", s):
        return None, "malformed_time"
    return None, "malformed_time"


def coerce_float(value: Any) -> tuple[Optional[float], Optional[str]]:
    """Parse a numeric value. Negative impurity values are flagged but kept null."""
    if value is None:
        return None, None
    if isinstance(value, bool):
        return None, "wrong_type"
    if isinstance(value, (int, float)):
        if value < 0:
            return None, "negative_value"
        return float(value), None
    s = str(value).strip()
    if not s:
        return None, None
    if s.lower() in NULL_SENTINELS:
        return None, "null_sentinel"
    rule = "whitespace" if s != str(value) else None
    try:
        f = float(s)
    except ValueError:
        return None, "non_numeric"
    if f < 0:
        return None, "negative_value"
    return f, rule


def evaluate_pass(value: Optional[float],
                  spec_min: Optional[float],
                  spec_max: Optional[float]) -> Optional[bool]:
    """True if the observation meets specification, False if it violates, None if undecidable."""
    if value is None:
        return None
    if spec_min is not None and value < spec_min:
        return False
    if spec_max is not None and value > spec_max:
        return False
    if spec_min is None and spec_max is None:
        return None
    return True
