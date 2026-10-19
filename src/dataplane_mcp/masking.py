import functools
import json
import re

MASK = "***"
SECRET_WORDS = r"pass(?:word|wd)?|pwd|secret|token|api[-_]?key|authorization|credential"
SECRET_KEY = re.compile(SECRET_WORDS, re.I)
URL_CREDENTIALS = re.compile(r"(\b[a-z][a-z0-9+.-]*://[^/\s:@]+:)[^@\s/]+@", re.I)


@functools.cache
def _pair_patterns(masked_fields: frozenset[str]) -> tuple[re.Pattern, re.Pattern]:
    """key=value pairs (form bodies, query strings) and key: value pairs (YAML, headers, broken JSON)."""
    names = [rf"[\w.-]*(?:{SECRET_WORDS})[\w.-]*"] + [re.escape(f) for f in sorted(masked_fields)]
    key = "|".join(names)
    equals = re.compile(rf"(?<![\w.-])((?:{key}))=([^&\s]*)", re.I)
    colon = re.compile(rf"""(?<![\w.-])(["']?)((?:{key}))\1(\s*:\s*)("(?:[^"\\]|\\.)*"|'[^']*'|[^\r\n,;&}}]*)""", re.I)
    return equals, colon


def mask_text(text: str, masked_fields: frozenset[str] = frozenset()) -> str:
    """Blank secret-looking (and configured) key=value and key: value pairs, and passwords in URLs."""
    equals, colon = _pair_patterns(masked_fields)
    # The pair regexes backtrack on every string; without "=" or ":" they cannot match.
    if "=" in text:
        text = equals.sub(rf"\1={MASK}", text)
    if ":" in text:
        text = URL_CREDENTIALS.sub(rf"\1{MASK}@", text)
        text = colon.sub(lambda m: f"{m[1]}{m[2]}{m[1]}{m[3]}{_masked_like(m[4])}", text)
    return text


def _masked_like(value: str) -> str:
    if value[:1] in ("'", '"'):
        return f"{value[0]}{MASK}{value[0]}"
    return MASK if value.strip() else value


def mask(value, masked_fields: frozenset[str]):
    """Mask secret-looking keys and the configured field names at any depth of a JSON value."""
    if isinstance(value, dict):
        return {
            k: MASK if k.lower() in masked_fields or SECRET_KEY.search(k) else mask(v, masked_fields)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [mask(v, masked_fields) for v in value]
    if isinstance(value, str):
        return mask_document(value, masked_fields)
    return value


def mask_document(text: str, masked_fields: frozenset[str]) -> str:
    """Mask text that may hold JSON (whole, or one document per line) or plain key/value text."""
    stripped = text.strip()
    if stripped[:1] in ("{", "["):
        try:
            return json.dumps(mask(json.loads(stripped), masked_fields), ensure_ascii=False)
        except ValueError:
            pass
        lines = text.splitlines()
        if len(lines) > 1:
            try:
                parsed = [json.loads(line) if line.strip() else None for line in lines]
            except ValueError:
                pass
            else:
                return "\n".join(
                    "" if p is None else json.dumps(mask(p, masked_fields), ensure_ascii=False) for p in parsed
                )
    return mask_text(text, masked_fields)


def decode_text(raw: bytes, complete: bool = True) -> str | None:
    """UTF-8, or Latin-1 when the bytes look like text, so masking still applies; None for binary."""
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as e:
        # A multi-byte character cut by the read limit is still text.
        if not complete and e.start >= len(raw) - 3:
            return raw[: e.start].decode("utf-8")
    head = raw[:4096]
    controls = sum(1 for b in head if b < 9 or 13 < b < 32)
    high = sum(1 for b in head if b > 127)
    # Mostly-ASCII text with a few accented bytes is Latin-1; anything else is treated as binary.
    return raw.decode("latin-1") if controls <= len(head) // 100 and high <= len(head) // 5 else None
