"""PowerCenter transformation-language expression -> Spark SQL translator.

``translate("IIF(ISNULL(A), 'x', B)")`` returns a :class:`Translation` whose
``sql`` is usable with ``F.expr``.  Parameter references (``$$NAME``) are
emitted as ``${NAME}`` placeholders that generated code resolves at run time
via ``RunContext.param``.  Anything the translator cannot convert faithfully
yields ``ok=False`` with a reason; callers emit a TODO quoting the original.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional


class Unsupported(Exception):
    pass


STRING_TYPES = {"string", "nstring", "text", "ntext", "char", "nchar", "varchar", "varchar2", "nvarchar2"}
DATE_TYPES = {"date/time", "date", "timestamp", "datetime"}

RESERVED = {
    "ALL", "AND", "ANY", "AS", "BETWEEN", "BY", "CASE", "CAST", "CHECK", "CREATE", "CROSS", "CURRENT",
    "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "DATE", "DEFAULT", "DISTINCT",
    "ELSE", "END", "EXCEPT", "FALSE", "FOR", "FROM", "FULL", "GROUP", "HAVING", "IN", "INNER", "INTERVAL",
    "INTO", "IS", "JOIN", "LEFT", "LIKE", "LIMIT", "NOT", "NULL", "ON", "OR", "ORDER", "OUTER", "RIGHT",
    "SELECT", "SESSION_USER", "SOME", "TABLE", "THEN", "TIME", "TIMESTAMP", "TO", "TRUE", "UNION", "UNIQUE",
    "USER", "USING", "WHEN", "WHERE", "WITH", "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "POSITION",
}

DD_CODES = {"DD_INSERT": "0", "DD_UPDATE": "1", "DD_DELETE": "2", "DD_REJECT": "3"}

_TOKEN = re.compile(
    r"""
    (?P<ws>\s+)
  | (?P<comment>(--|//)[^\n]*)
  | (?P<string>'[^']*')
  | (?P<number>\d+\.\d*|\.\d+|\d+)
  | (?P<lkp>:(LKP|SP|MCR)\.[A-Za-z_][A-Za-z0-9_]*)
  | (?P<ident>\$\$?[A-Za-z_][A-Za-z0-9_]*|[A-Za-z_@#][A-Za-z0-9_$#@]*(\.[A-Za-z_][A-Za-z0-9_]*)?)
  | (?P<op>\|\||<>|!=|\^=|<=|>=|[-+*/%=<>(),])
    """,
    re.VERBOSE | re.IGNORECASE,
)


def tokenize(text: str) -> list[tuple[str, str]]:
    pos, out = 0, []
    while pos < len(text):
        m = _TOKEN.match(text, pos)
        if not m:
            raise Unsupported(f"cannot tokenize near {text[pos:pos + 20]!r}")
        pos = m.end()
        kind = m.lastgroup
        if kind in ("ws", "comment"):
            continue
        out.append((kind, m.group(kind)))
    return out


def param_key(name: str) -> str:
    """Key used with ``RunContext.param`` (same prefix stripping)."""
    return name.removeprefix("$$").removeprefix("$Param_").lstrip("$")


def sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def spark_type(datatype: str, precision: int = 0, scale: int = 0) -> str:
    dt = (datatype or "string").lower()
    if dt in STRING_TYPES or dt.startswith("varchar") or dt.startswith("char"):
        return "string"
    if dt in DATE_TYPES:
        return "timestamp"
    if dt in ("integer", "int", "small integer", "smallint"):
        return "int"
    if dt == "bigint":
        return "bigint"
    if dt in ("double", "real", "float"):
        return "double"
    if dt in ("decimal", "number", "numeric", "number(p,s)", "packed", "zoned", "num"):
        if precision <= 0:
            return "decimal(38,10)"
        p = min(precision, 38)
        return f"decimal({p},{min(scale, p)})"
    if dt == "binary":
        return "binary"
    return "string"


def kind_of(datatype: str) -> str:
    st = spark_type(datatype)
    if st == "string":
        return "str"
    if st == "timestamp":
        return "date"
    return "num"


# PowerCenter date format tokens -> Java DateTimeFormatter, longest first.
_DATE_TOKENS = [
    ("MONTH", "MMMM"), ("HH24", "HH"), ("HH12", "hh"), ("YYYY", "yyyy"), ("RRRR", "yyyy"), ("YYY", "yyy"),
    ("MON", "MMM"), ("DDD", "DDD"), ("DAY", "EEEE"), ("YY", "yy"), ("RR", "yy"), ("MM", "MM"), ("DD", "dd"),
    ("DY", "EEE"), ("HH", "hh"), ("MI", "mm"), ("SS", "ss"), ("MS", "SSS"), ("US", "SSSSSS"),
    ("NS", "SSSSSSSSS"), ("AM", "a"), ("PM", "a"), ("A.M.", "a"), ("P.M.", "a"), ("Y", "y"), ("Q", "Q"),
]


def java_date_format(fmt: str) -> str:
    out, i, upper = [], 0, fmt.upper()
    while i < len(fmt):
        for tok, java in _DATE_TOKENS:
            if upper.startswith(tok, i):
                out.append(java)
                i += len(tok)
                break
        else:
            ch = fmt[i]
            if ch == "J":
                raise Unsupported("Julian date format 'J' has no Spark equivalent")
            if ch == '"':
                end = fmt.find('"', i + 1)
                end = len(fmt) if end < 0 else end
                out.append("'" + fmt[i + 1:end].replace("'", "''") + "'")
                i = end + 1
                continue
            out.append(f"'{ch}'" if ch.isalpha() else ch)
            i += 1
    return "".join(out)


DEFAULT_DATE_FORMAT = "MM/DD/YYYY HH24:MI:SS"

_DATE_UNITS = {
    "Y": "YEAR", "YY": "YEAR", "YYY": "YEAR", "YYYY": "YEAR", "Q": "QUARTER", "MM": "MONTH", "MON": "MONTH",
    "MONTH": "MONTH", "D": "DAY", "DD": "DAY", "DDD": "DAY", "DY": "DAY", "DAY": "DAY", "HH": "HOUR",
    "HH12": "HOUR", "HH24": "HOUR", "MI": "MINUTE", "SS": "SECOND", "MS": "MILLISECOND", "US": "MICROSECOND",
}
_DATE_PART_FN = {"YEAR": "year", "QUARTER": "quarter", "MONTH": "month", "DAY": "dayofmonth", "HOUR": "hour",
                 "MINUTE": "minute", "SECOND": "second"}
_DIFF_SECONDS = {"DAY": 86400, "HOUR": 3600, "MINUTE": 60, "SECOND": 1}

AGGREGATES = {"SUM": "sum", "AVG": "avg", "MAX": "max", "MIN": "min", "COUNT": "count", "FIRST": "first",
              "LAST": "last", "MEDIAN": "median", "STDDEV": "stddev", "VARIANCE": "variance"}
SIMPLE = {"LENGTH": "length", "UPPER": "upper", "LOWER": "lower", "ABS": "abs", "SIGN": "signum",
          "CHR": "chr", "ASCII": "ascii", "LAST_DAY": "last_day", "INITCAP": "initcap", "CEIL": "ceil",
          "FLOOR": "floor", "POWER": "power", "SQRT": "sqrt", "EXP": "exp", "LN": "ln", "LOG": "log",
          "REVERSE": "reverse", "SOUNDEX": "soundex", "GREATEST": "greatest", "LEAST": "least",
          "REG_REPLACE": "regexp_replace", "REG_EXTRACT": "regexp_extract", "MAKE_DATE_TIME": "make_timestamp"}
KIND = {"LENGTH": "num", "ABS": "num", "SIGN": "num", "ASCII": "num", "CEIL": "num", "FLOOR": "num",
        "POWER": "num", "SQRT": "num", "EXP": "num", "LN": "num", "LOG": "num", "LAST_DAY": "date",
        "MAKE_DATE_TIME": "date"}


@dataclass
class Node:
    op: str
    value: str = ""
    args: list["Node"] = field(default_factory=list)


class _Parser:
    def __init__(self, tokens):
        self.toks, self.i = tokens, 0

    def peek(self, k=0):
        j = self.i + k
        return self.toks[j] if j < len(self.toks) else ("eof", "")

    def take(self):
        t = self.peek()
        self.i += 1
        return t

    def kw(self, word):
        k, v = self.peek()
        return k == "ident" and v.upper() == word

    def expect(self, val):
        k, v = self.take()
        if v != val:
            raise Unsupported(f"expected {val!r}, got {v!r}")

    def parse(self):
        node = self.or_()
        if self.peek()[0] != "eof":
            raise Unsupported(f"unexpected token {self.peek()[1]!r}")
        return node

    def or_(self):
        n = self.and_()
        while self.kw("OR"):
            self.take()
            n = Node("bin", "OR", [n, self.and_()])
        return n

    def and_(self):
        n = self.not_()
        while self.kw("AND"):
            self.take()
            n = Node("bin", "AND", [n, self.not_()])
        return n

    def not_(self):
        if self.kw("NOT"):
            self.take()
            return Node("not", args=[self.not_()])
        return self.cmp()

    def cmp(self):
        n = self.concat()
        while self.peek()[1] in ("=", "<>", "!=", "^=", "<", ">", "<=", ">="):
            op = self.take()[1]
            n = Node("bin", "<>" if op in ("!=", "^=") else op, [n, self.concat()])
        return n

    def concat(self):
        n = self.add()
        while self.peek()[1] == "||":
            self.take()
            n = Node("bin", "||", [n, self.add()])
        return n

    def add(self):
        n = self.mul()
        while self.peek()[1] in ("+", "-"):
            op = self.take()[1]
            n = Node("bin", op, [n, self.mul()])
        return n

    def mul(self):
        n = self.unary()
        while self.peek()[1] in ("*", "/", "%"):
            op = self.take()[1]
            n = Node("bin", op, [n, self.unary()])
        return n

    def unary(self):
        if self.peek()[1] in ("-", "+"):
            op = self.take()[1]
            inner = self.unary()
            return inner if op == "+" else Node("neg", args=[inner])
        return self.atom()

    def args(self):
        self.expect("(")
        out = []
        if self.peek()[1] == ")":
            self.take()
            return out
        while True:
            if self.peek()[1] == "*":
                self.take()
                out.append(Node("star"))
            else:
                out.append(self.or_())
            k, v = self.take()
            if v == ")":
                return out
            if v != ",":
                raise Unsupported(f"expected ',' or ')', got {v!r}")

    def atom(self):
        k, v = self.take()
        if k == "string":
            return Node("str", v[1:-1])
        if k == "number":
            return Node("num", v)
        if v == "(":
            n = self.or_()
            self.expect(")")
            return n
        if k == "lkp":
            return Node("lkp", v.split(".", 1)[1], self.args() if self.peek()[1] == "(" else [])
        if k == "ident":
            if v.startswith("$"):
                return Node("param", v)
            if self.peek()[1] == "(":
                return Node("call", v.upper(), self.args())
            return Node("id", v)
        raise Unsupported(f"unexpected token {v!r}")


@dataclass
class Translation:
    original: str
    sql: Optional[str]
    ok: bool
    reason: str = ""
    kind: str = "any"
    params: set = field(default_factory=set)
    lookups: list = field(default_factory=list)
    setvars: list = field(default_factory=list)
    aggregate: bool = False
    refs: set = field(default_factory=set)
    notes: list = field(default_factory=list)


class Translator:
    def __init__(self, types: dict[str, str] | None = None, dd_tokens: bool = False, lookup_offset: int = 0):
        self.types = {k.upper(): v for k, v in (types or {}).items()}
        self.dd_tokens = dd_tokens
        self.lookup_offset = lookup_offset
        self.t: Translation | None = None

    def translate(self, text: str) -> Translation:
        self.t = Translation(original=text, sql=None, ok=False)
        try:
            tokens = tokenize(text)
            if not tokens:
                self.t.sql, self.t.kind, self.t.ok = "NULL", "any", True
                self.t.notes.append("expression is empty or fully commented out")
                return self.t
            node = _Parser(tokens).parse()
            sql, kind = self.emit(node)
            self.t.sql, self.t.kind, self.t.ok = sql, kind, True
        except Unsupported as exc:
            self.t.reason = str(exc)
        except RecursionError:
            self.t.reason = "expression too deeply nested"
        return self.t

    def condition(self, text: str) -> Translation:
        tr = self.translate(text)
        if tr.ok:
            tr.sql = self.as_bool(tr.sql, tr.kind)
            tr.kind = "bool"
        return tr

    # --- helpers -----------------------------------------------------------
    @staticmethod
    def as_bool(sql: str, kind: str) -> str:
        if kind == "bool":
            return sql
        if kind == "str":
            return f"(CAST({sql} AS DOUBLE) <> 0)"
        return f"({sql} <> 0)"

    def lit(self, node: Node) -> Optional[str]:
        if node.op in ("str", "num"):
            return node.value
        if node.op == "neg" and node.args[0].op == "num":
            return "-" + node.args[0].value
        return None

    def str_lit(self, node: Node, what: str) -> str:
        if node.op != "str":
            raise Unsupported(f"{what} must be a string literal")
        return node.value

    def ident(self, name: str) -> str:
        self.t.refs.add(name)
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) and name.upper() not in RESERVED:
            return name
        return f"`{name}`"

    # --- emitter -------------------------------------------------------------
    def emit(self, n: Node) -> tuple[str, str]:
        op = n.op
        if op == "str":
            return sql_string(n.value), "str"
        if op == "num":
            return n.value, "num"
        if op == "star":
            return "*", "any"
        if op == "param":
            key = param_key(n.value)
            self.t.params.add(key)
            return "${" + key + "}", "any"
        if op == "id":
            u = n.value.upper()
            if u in ("TRUE", "FALSE"):
                return u.lower(), "bool"
            if u == "NULL":
                return "NULL", "any"
            if u in ("SYSDATE", "SESSSTARTTIME", "WORKFLOWSTARTTIME", "SYSTIMESTAMP"):
                return "current_timestamp()", "date"
            if u in DD_CODES:
                return (u if self.dd_tokens else DD_CODES[u]), "num"
            if "." in n.value:
                raise Unsupported(f"qualified reference {n.value} is not supported")
            return self.ident(n.value), kind_of(self.types[u]) if u in self.types else "any"
        if op == "neg":
            s, _ = self.emit(n.args[0])
            return f"(-{s})", "num"
        if op == "not":
            s, k = self.emit(n.args[0])
            return f"(NOT {self.as_bool(s, k)})", "bool"
        if op == "bin":
            return self.binary(n)
        if op == "lkp":
            if n.args and any(a.op == "star" for a in n.args):
                raise Unsupported("unconnected lookup with * argument")
            args = [self.emit(a)[0] for a in n.args]
            col = f"_lkp{self.lookup_offset + len(self.t.lookups)}"
            self.t.lookups.append({"lookup": n.value, "args": args, "column": col})
            return col, "any"
        if op == "call":
            return self.call(n.value, n.args)
        raise Unsupported(f"node {op}")

    def binary(self, n: Node) -> tuple[str, str]:
        op = n.value
        if op == "||":
            parts: list[Node] = []

            def flat(x):
                if x.op == "bin" and x.value == "||":
                    flat(x.args[0])
                    flat(x.args[1])
                else:
                    parts.append(x)

            flat(n)
            return "concat_ws('', " + ", ".join(self.emit(p)[0] for p in parts) + ")", "str"
        (ls, lk), (rs, rk) = self.emit(n.args[0]), self.emit(n.args[1])
        if op in ("AND", "OR"):
            return f"({self.as_bool(ls, lk)} {op} {self.as_bool(rs, rk)})", "bool"
        if op in ("=", "<>", "<", ">", "<=", ">="):
            if lk == "bool" and rk in ("num", "any") and ls in ("true", "false"):
                ls = "1" if ls == "true" else "0"
            if rk == "bool" and lk in ("num", "any") and rs in ("true", "false"):
                rs = "1" if rs == "true" else "0"
            return f"({ls} {op} {rs})", "bool"
        if op in ("+", "-") and "date" in (lk, rk):
            raise Unsupported("date arithmetic with +/- (use ADD_TO_DATE)")
        return f"({ls} {op} {rs})", "num"

    def call(self, name: str, a: list[Node]) -> tuple[str, str]:  # noqa: C901 - dispatch table
        e = lambda x: self.emit(x)[0]  # noqa: E731
        n = len(a)

        def need(lo, hi=None):
            if n < lo or n > (hi if hi is not None else lo):
                raise Unsupported(f"{name} with {n} arguments")

        if name == "IIF":
            need(2, 3)
            c, ck = self.emit(a[0])
            t, tk = self.emit(a[1])
            f, fk = self.emit(a[2]) if n == 3 else ("NULL", "any")
            return f"IF({self.as_bool(c, ck)}, {t}, {f})", tk if tk != "any" else fk
        if name == "DECODE":
            if n < 3:
                raise Unsupported("DECODE needs at least 3 arguments")
            first = a[0]
            pairs, default = a[1:], None
            if len(pairs) % 2:
                default = pairs[-1]
                pairs = pairs[:-1]
            kinds = []
            if first.op == "id" and first.value.upper() == "TRUE":
                whens = []
                for i in range(0, len(pairs), 2):
                    c, ck = self.emit(pairs[i])
                    r, rk = self.emit(pairs[i + 1])
                    kinds.append(rk)
                    whens.append(f"WHEN {self.as_bool(c, ck)} THEN {r}")
                head = "CASE"
            else:
                head = f"CASE {e(first)}"
                whens = []
                for i in range(0, len(pairs), 2):
                    r, rk = self.emit(pairs[i + 1])
                    kinds.append(rk)
                    whens.append(f"WHEN {e(pairs[i])} THEN {r}")
            tail = f" ELSE {e(default)}" if default is not None else ""
            kind = next((k for k in kinds if k != "any"), "any")
            return f"{head} {' '.join(whens)}{tail} END", kind
        if name == "ISNULL":
            need(1)
            return f"({e(a[0])} IS NULL)", "bool"
        if name == "IS_SPACES":
            need(1)
            return f"(rtrim({e(a[0])}) = '')", "bool"
        if name == "IS_NUMBER":
            need(1)
            pat = r"^[+-]?([0-9]+\\.?[0-9]*|\\.[0-9]+)([eEdD][+-]?[0-9]+)?$"
            return f"(trim({e(a[0])}) RLIKE '{pat}')", "bool"
        if name == "IS_DATE":
            need(1, 2)
            fmt = java_date_format(self.str_lit(a[1], "IS_DATE format") if n == 2 else DEFAULT_DATE_FORMAT)
            return f"(to_timestamp({e(a[0])}, {sql_string(fmt)}) IS NOT NULL)", "bool"
        if name == "TO_DATE":
            need(1, 2)
            fmt = java_date_format(self.str_lit(a[1], "TO_DATE format") if n == 2 else DEFAULT_DATE_FORMAT)
            return f"to_timestamp({e(a[0])}, {sql_string(fmt)})", "date"
        if name == "TO_CHAR":
            need(1, 2)
            s, k = self.emit(a[0])
            if n == 2:
                return f"date_format({s}, {sql_string(java_date_format(self.str_lit(a[1], 'TO_CHAR format')))})", "str"
            if k == "date":
                return f"date_format({s}, {sql_string(java_date_format(DEFAULT_DATE_FORMAT))})", "str"
            return f"CAST({s} AS STRING)", "str"
        if name == "TO_DECIMAL":
            need(1, 2)
            scale = self.lit(a[1]) if n == 2 else None
            if n == 2 and scale is None:
                raise Unsupported("TO_DECIMAL scale must be a literal")
            return (f"CAST({e(a[0])} AS DECIMAL(28, {scale}))" if scale is not None
                    else f"CAST({e(a[0])} AS DECIMAL(38, 10))"), "num"
        if name in ("TO_INTEGER", "TO_BIGINT"):
            need(1, 2)
            typ = "INT" if name == "TO_INTEGER" else "BIGINT"
            s = e(a[0])
            if n == 2 and a[1].op == "id" and a[1].value.upper() == "FALSE":
                s = f"round({s})"
            return f"CAST({s} AS {typ})", "num"
        if name == "TO_FLOAT":
            need(1)
            return f"CAST({e(a[0])} AS DOUBLE)", "num"
        if name == "SUBSTR":
            need(2, 3)
            return f"substring({', '.join(e(x) for x in a)})", "str"
        if name == "INSTR":
            need(2, 4)
            if n == 2:
                return f"instr({e(a[0])}, {e(a[1])})", "num"
            start = self.lit(a[2])
            if start == "-1" and n == 2 + 1:
                s, sub = e(a[0]), e(a[1])
                return (f"IF(instr(reverse({s}), reverse({sub})) = 0, 0, length({s}) - "
                        f"instr(reverse({s}), reverse({sub})) - length({sub}) + 2)"), "num"
            if start is None:
                self.t.notes.append("INSTR with non-literal start: assumed positive (PowerCenter searches "
                                    "backwards for negative starts)")
                start = e(a[2])
            elif int(float(start)) < 0:
                raise Unsupported("INSTR with negative start position other than -1")
            if n == 4 and self.lit(a[3]) not in ("1",):
                raise Unsupported("INSTR occurrence argument other than 1")
            return f"locate({e(a[1])}, {e(a[0])}, {start})", "num"
        if name in ("LPAD", "RPAD"):
            need(2, 3)
            return f"{name.lower()}({', '.join(e(x) for x in a)})", "str"
        if name in ("LTRIM", "RTRIM"):
            need(1, 2)
            if n == 1:
                return f"{name.lower()}({e(a[0])})", "str"
            side = "LEADING" if name == "LTRIM" else "TRAILING"
            return f"TRIM({side} {e(a[1])} FROM {e(a[0])})", "str"
        if name == "CONCAT":
            need(2)
            return f"concat_ws('', {e(a[0])}, {e(a[1])})", "str"
        if name == "REPLACECHR":
            need(4)
            chars = self.str_lit(a[2], "REPLACECHR search characters")
            if a[3].op == "id" and a[3].value.upper() == "NULL":
                new = ""
            else:
                new = self.str_lit(a[3], "REPLACECHR replacement")[:1]
            klass = "".join("\\" + c if c in "\\^]-[" else c for c in chars)
            pat = ("(?i)" if self.lit(a[0]) == "0" else "") + f"[{klass}]"
            return f"regexp_replace({e(a[1])}, {sql_string(pat)}, {sql_string(_repl(new))})", "str"
        if name == "REPLACESTR":
            if n < 4:
                raise Unsupported("REPLACESTR needs at least 4 arguments")
            if any(x.op != "str" for x in a[2:-1]):
                if n != 4:
                    raise Unsupported("REPLACESTR with several non-literal search strings")
                s, old, new = e(a[1]), e(a[2]), e(a[3])
                if self.lit(a[0]) == "1":
                    return f"replace({s}, {old}, coalesce({new}, ''))", "str"
                self.t.notes.append("REPLACESTR non-literal search: regex-quoted, replacement must not contain $ or \\")
                return f"regexp_replace({s}, concat('(?i)\\\\Q', {old}, '\\\\E'), coalesce({new}, ''))", "str"
            olds = [self.str_lit(x, "REPLACESTR search string") for x in a[2:-1]]
            if a[-1].op == "id" and a[-1].value.upper() == "NULL":
                new = ""
            else:
                new = self.str_lit(a[-1], "REPLACESTR replacement")
            pat = ("(?i)" if self.lit(a[0]) == "0" else "") + "|".join(re.escape(o) for o in olds if o)
            return f"regexp_replace({e(a[1])}, {sql_string(pat)}, {sql_string(_repl(new))})", "str"
        if name == "IN":
            if n < 2:
                raise Unsupported("IN needs at least 2 arguments")
            vals = a[1:]
            if len(vals) > 1 and self.lit(vals[-1]) in ("0", "1") and vals[-1].op == "num" and all(
                v.op == "str" for v in vals[:-1]
            ):
                if vals[-1].value == "0":
                    items = ", ".join(f"upper({e(v)})" for v in vals[:-1])
                    return f"(upper({e(a[0])}) IN ({items}))", "bool"
                vals = vals[:-1]
            return f"({e(a[0])} IN ({', '.join(e(v) for v in vals)}))", "bool"
        if name == "ROUND":
            need(1, 2)
            s, k = self.emit(a[0])
            if k == "date":
                raise Unsupported("ROUND on a date")
            return f"round({', '.join([s] + [e(x) for x in a[1:]])})", "num"
        if name == "TRUNC":
            need(1, 2)
            s, k = self.emit(a[0])
            if k == "date" or (n == 2 and a[1].op == "str"):
                unit = _DATE_UNITS.get((a[1].value if n == 2 else "DD").upper())
                if unit is None or unit in ("MILLISECOND", "MICROSECOND"):
                    raise Unsupported("TRUNC date format")
                return f"date_trunc('{unit}', {s})", "date"
            digits = self.lit(a[1]) if n == 2 else "0"
            if digits is None:
                raise Unsupported("TRUNC precision must be a literal")
            d = int(float(digits))
            if d == 0:
                return f"CAST({s} AS BIGINT)", "num"
            if d < 0:
                raise Unsupported("TRUNC with negative precision")
            return f"CAST(CAST({s} * 1e{d} AS BIGINT) / 1e{d} AS DECIMAL(38, {d}))", "num"
        if name == "GET_DATE_PART":
            need(2)
            unit = _DATE_UNITS.get(self.str_lit(a[1], "GET_DATE_PART format").upper())
            if a[1].value.upper() == "DDD":
                return f"dayofyear({e(a[0])})", "num"
            if unit not in _DATE_PART_FN:
                raise Unsupported("GET_DATE_PART format")
            return f"{_DATE_PART_FN[unit]}({e(a[0])})", "num"
        if name == "ADD_TO_DATE":
            need(3)
            unit = _DATE_UNITS.get(self.str_lit(a[1], "ADD_TO_DATE format").upper())
            if unit is None:
                raise Unsupported("ADD_TO_DATE format")
            return f"timestampadd({unit}, {e(a[2])}, {e(a[0])})", "date"
        if name == "DATE_DIFF":
            need(3)
            unit = _DATE_UNITS.get(self.str_lit(a[2], "DATE_DIFF format").upper())
            d1, d2 = e(a[0]), e(a[1])
            if unit in _DIFF_SECONDS:
                return f"((unix_timestamp({d1}) - unix_timestamp({d2})) / {_DIFF_SECONDS[unit]})", "num"
            if unit == "MONTH":
                return f"months_between({d1}, {d2})", "num"
            if unit == "YEAR":
                return f"(months_between({d1}, {d2}) / 12)", "num"
            raise Unsupported("DATE_DIFF format")
        if name == "DATE_COMPARE":
            need(2)
            d1, d2 = e(a[0]), e(a[1])
            return f"CASE WHEN {d1} < {d2} THEN -1 WHEN {d1} > {d2} THEN 1 ELSE 0 END", "num"
        if name == "MOD":
            need(2)
            return f"({e(a[0])} % {e(a[1])})", "num"
        if name in ("REG_MATCH",):
            need(2)
            return f"({e(a[0])} RLIKE {e(a[1])})", "bool"
        if name in ("ERROR", "ABORT"):
            need(1)
            self.t.notes.append(f"{name}() mapped to raise_error(); fires only when the column is evaluated")
            return f"raise_error({e(a[0])})", "any"
        if name in ("SETVARIABLE", "SETMAXVARIABLE", "SETMINVARIABLE", "SETCOUNTVARIABLE"):
            need(1, 2)
            if a[0].op != "param":
                raise Unsupported(f"{name} first argument must be a $$variable")
            key = param_key(a[0].value)
            if name == "SETCOUNTVARIABLE":
                value, kind = "1", "num"
            else:
                need(2)
                value, kind = self.emit(a[1])
            agg = {"SETMAXVARIABLE": "MAX", "SETMINVARIABLE": "MIN", "SETCOUNTVARIABLE": "COUNT"}.get(name, "")
            self.t.setvars.append({"variable": key, "sql": value, "aggregate": agg})
            return value, kind
        if name in AGGREGATES:
            need(1, 2)
            self.t.aggregate = True
            fn = AGGREGATES[name]
            if a[0].op == "star":
                if name != "COUNT":
                    raise Unsupported(f"{name}(*)")
                if n == 1:
                    return "count(*)", "num"
                c, ck = self.emit(a[1])
                return f"count(IF({self.as_bool(c, ck)}, 1, NULL))", "num"
            s, k = self.emit(a[0])
            if n == 2:
                c, ck = self.emit(a[1])
                s = f"IF({self.as_bool(c, ck)}, {s}, NULL)"
            if fn in ("first", "last"):
                return f"{fn}({s}, true)", k
            return f"{fn}({s})", "num" if fn not in ("max", "min") else k
        if name in SIMPLE:
            return f"{SIMPLE[name]}({', '.join(e(x) for x in a)})", KIND.get(name, "str")
        raise Unsupported(f"function {name} is not supported")


def _repl(s: str) -> str:
    """Escape a Java regex replacement string."""
    return s.replace("\\", "\\\\").replace("$", "\\$")


def translate(text: str, types: dict[str, str] | None = None, dd_tokens: bool = False) -> Translation:
    return Translator(types, dd_tokens).translate(text)


def translate_condition(text: str, types: dict[str, str] | None = None) -> Translation:
    return Translator(types).condition(text)
