"""Minimal ADF expression evaluator.

Supports the subset used by this POC's pipelines:

    @pipeline().parameters.<name>
    @activity('<name>').output.<path...>
    @item()
    @item().<field>
    @dataset().<param>
    @{ ... }                        # interpolation inside string literals
    @replace(s, old, new)
    @concat(a, b, ...)

Anything fancier (control flow, arithmetic) is out of scope — keep the
runner readable. Add functions here if a new pipeline needs them.
"""

from __future__ import annotations

import re
from typing import Any


# Recognise both bare expressions ("@activity(...)") and string
# interpolations ("foo@{activity(...)}bar").
_INTERP_RE = re.compile(r"@\{([^}]+)\}")


class ExpressionContext:
    """Holds everything @-expressions can reference inside an activity run."""

    def __init__(
        self,
        *,
        pipeline_parameters: dict[str, Any],
        activity_outputs: dict[str, Any],
        item: Any | None = None,
        dataset_parameters: dict[str, Any] | None = None,
    ):
        self.pipeline_parameters = pipeline_parameters
        self.activity_outputs = activity_outputs
        self.item = item
        self.dataset_parameters = dataset_parameters or {}

    def with_item(self, item: Any) -> "ExpressionContext":
        return ExpressionContext(
            pipeline_parameters=self.pipeline_parameters,
            activity_outputs=self.activity_outputs,
            item=item,
            dataset_parameters=self.dataset_parameters,
        )

    def with_dataset_parameters(self, params: dict[str, Any]) -> "ExpressionContext":
        return ExpressionContext(
            pipeline_parameters=self.pipeline_parameters,
            activity_outputs=self.activity_outputs,
            item=self.item,
            dataset_parameters=params,
        )


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def evaluate(value: Any, ctx: ExpressionContext) -> Any:
    """Resolve an ADF expression value (string or {value, type} wrapper)."""
    if isinstance(value, dict) and value.get("type") == "Expression":
        return _eval_string(value["value"], ctx)
    if isinstance(value, str):
        return _eval_string(value, ctx)
    return value


def evaluate_parameters(params: dict[str, Any], ctx: ExpressionContext) -> dict[str, Any]:
    return {k: evaluate(v, ctx) for k, v in (params or {}).items()}


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------

def _eval_string(s: str, ctx: ExpressionContext) -> Any:
    if not isinstance(s, str):
        return s

    # Bare expression: starts with @ but is not @@-escaped
    if s.startswith("@") and not s.startswith("@@") and not s.startswith("@{"):
        return _eval_expr(s[1:], ctx)

    # String with @{...} interpolations
    if "@{" in s:
        def _sub(match: re.Match[str]) -> str:
            value = _eval_expr(match.group(1), ctx)
            return str(value) if value is not None else ""
        return _INTERP_RE.sub(_sub, s)

    return s


def _eval_expr(expr: str, ctx: ExpressionContext) -> Any:
    expr = expr.strip()

    # Function calls: replace(..), concat(..)
    if expr.startswith("replace(") and expr.endswith(")"):
        args = _split_args(expr[len("replace("):-1])
        if len(args) != 3:
            raise ValueError(f"replace() expects 3 args, got: {expr}")
        a, b, c = (_eval_expr(_unwrap(x), ctx) for x in args)
        return str(a).replace(str(b), str(c))

    if expr.startswith("concat(") and expr.endswith(")"):
        args = _split_args(expr[len("concat("):-1])
        return "".join(str(_eval_expr(_unwrap(x), ctx)) for x in args)

    # String literal
    if (expr.startswith("'") and expr.endswith("'")) or \
       (expr.startswith('"') and expr.endswith('"')):
        return expr[1:-1]

    # Member access chains: pipeline().parameters.x, activity('a').output.b.c, item().x
    return _resolve_chain(expr, ctx)


def _resolve_chain(expr: str, ctx: ExpressionContext) -> Any:
    head, *rest = _split_dots(expr)

    if head == "pipeline()":
        cur: Any = {"parameters": ctx.pipeline_parameters}
    elif head == "item()":
        cur = ctx.item
    elif head == "dataset()":
        cur = ctx.dataset_parameters
    elif head.startswith("activity("):
        inner = head[len("activity("):-1].strip().strip("'").strip('"')
        cur = ctx.activity_outputs.get(inner, {})
    else:
        raise ValueError(f"unsupported expression head: {head!r} (in {expr!r})")

    for part in rest:
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            cur = getattr(cur, part, None)
    return cur


# Split a dotted path while respecting parentheses (so "activity('x').output.y"
# splits into ["activity('x')", "output", "y"], not on dots inside the call).
def _split_dots(expr: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    buf = ""
    for ch in expr:
        if ch in "([":
            depth += 1
            buf += ch
        elif ch in ")]":
            depth -= 1
            buf += ch
        elif ch == "." and depth == 0:
            parts.append(buf)
            buf = ""
        else:
            buf += ch
    if buf:
        parts.append(buf)
    return parts


def _split_args(args: str) -> list[str]:
    """Split comma-separated argument list, respecting nested parens/quotes."""
    out: list[str] = []
    depth = 0
    in_quote: str | None = None
    buf = ""
    for ch in args:
        if in_quote:
            buf += ch
            if ch == in_quote:
                in_quote = None
            continue
        if ch in "'\"":
            in_quote = ch
            buf += ch
        elif ch in "([":
            depth += 1
            buf += ch
        elif ch in ")]":
            depth -= 1
            buf += ch
        elif ch == "," and depth == 0:
            out.append(buf.strip())
            buf = ""
        else:
            buf += ch
    if buf.strip():
        out.append(buf.strip())
    return out


def _unwrap(s: str) -> str:
    return s.strip()
