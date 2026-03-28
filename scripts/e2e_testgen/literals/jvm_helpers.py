"""Shared helpers for JVM-based literal renderers (Java and Kotlin)."""

from __future__ import annotations


def jvm_str(s: object) -> str:
    """Render a string value as a Java/Kotlin double-quoted string literal."""
    return '"' + str(s).replace('\\', '\\\\').replace('"', '\\"') + '"'


def strip_java_time(lang_type: str) -> str:
    """Strip java.time. prefix for easier type matching."""
    return lang_type.replace("java.time.", "")


def jvm_local_datetime(s: str) -> str:
    """Parse a datetime string and return a LocalDateTime.of(...) expression."""
    s_clean = s.rstrip('Z').replace('T', ' ')
    date_part, time_part = s_clean.split(' ') if ' ' in s_clean else (s_clean, '0:0:0')
    y, mo, d = date_part.split('-')
    h, mi, sec = time_part.split(':')
    return f"LocalDateTime.of({int(y)}, {int(mo)}, {int(d)}, {int(h)}, {int(mi)}, {int(sec)})"


def jvm_offset_datetime(s: str) -> str:
    """Parse a datetime string and return an OffsetDateTime.of(...) expression."""
    s_clean = s.rstrip('Z').replace('T', ' ')
    date_part, time_part = s_clean.split(' ') if ' ' in s_clean else (s_clean, '0:0:0')
    y, mo, d = date_part.split('-')
    h, mi, sec = time_part.split(':')
    return (
        f"OffsetDateTime.of({int(y)}, {int(mo)}, {int(d)}, "
        f"{int(h)}, {int(mi)}, {int(sec)}, 0, ZoneOffset.UTC)"
    )


def jvm_local_date(s: str) -> str:
    """Parse a date string (YYYY-MM-DD) and return a LocalDate.of(...) expression."""
    y, mo, d = str(s).split('-')
    return f"LocalDate.of({int(y)}, {int(mo)}, {int(d)})"


def jvm_local_time(s: str) -> str:
    """Parse a time string (HH:MM:SS) and return a LocalTime.of(...) expression."""
    parts = str(s).split(':')
    h, mi = int(parts[0]), int(parts[1])
    sec = int(parts[2]) if len(parts) > 2 else 0
    return f"LocalTime.of({h}, {mi}, {sec})"
