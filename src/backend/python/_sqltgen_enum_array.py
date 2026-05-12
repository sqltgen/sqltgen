

def _parse_enum_array(raw: Any, enum_cls: type[Any]) -> list[Any]:
    """Parse a PostgreSQL enum array literal into a list of enum values.

    psycopg3 returns enum[] columns as raw text (e.g. '{low,medium,high}')
    because it does not know the enum type OID. This helper parses the text
    representation into a Python list of the given enum class.
    """
    if raw is None:
        return []
    if isinstance(raw, list):
        return [enum_cls(v) for v in raw]
    s = str(raw).strip("{}")
    if not s:
        return []
    return [enum_cls(v) for v in s.split(",")]
