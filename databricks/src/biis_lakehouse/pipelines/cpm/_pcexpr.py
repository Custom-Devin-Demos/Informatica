"""PowerCenter transformation-language support for the CPM pipelines.

``parse`` turns a PowerCenter expression into a small AST; ``SqlGen`` renders it as a Spark SQL expression
(used for every stateless port, filter, router group and aggregate), and ``compile_py`` renders it as a Python
callable (used for Expression transformations whose variable ports carry state from row to row, which Spark
SQL cannot express).  Both follow PowerCenter semantics where they differ from SQL: ``||`` ignores NULLs,
``TRUE``/``FALSE`` are 1/0, ``IIF`` without a false value returns 0/''/NULL by type, ``TO_DECIMAL`` of a
non-numeric string is 0 (leading numeric prefix kept), ``TO_CHAR`` of a number drops trailing zeros,
aggregate functions skip NULLs, and ``ABORT`` raises.
"""
from __future__ import annotations

import datetime as _dt
import re
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Callable

# --------------------------------------------------------------------------- parsing
_TOKEN = re.compile(
    r"""(?P<ws>\s+)|(?P<comment>(?:--|//)[^\n]*)|(?P<str>'(?:[^']|'')*')|(?P<num>\d+\.\d*|\.\d+|\d+)
    |(?P<id>(?:\$\$|\$|:)?[A-Za-z_][A-Za-z0-9_#]*(?:\.[A-Za-z_][A-Za-z0-9_#]*)?)
    |(?P<op>\|\||<=|>=|<>|!=|\^=|[-+*/%=<>(),])""",
    re.X,
)

AGGREGATES = {"SUM", "COUNT", "FIRST", "LAST", "MAX", "MIN", "AVG"}


def _tokens(text: str) -> list[tuple[str, str]]:
    out, pos = [], 0
    while pos < len(text):
        m = _TOKEN.match(text, pos)
        if not m:
            raise SyntaxError(f"bad character {text[pos]!r} in expression: {text!r}")
        pos = m.end()
        kind = m.lastgroup
        if kind in ("ws", "comment"):
            continue
        out.append((kind, m.group(kind)))
    return out


class _Parser:
    def __init__(self, text: str):
        self.toks = _tokens(text)
        self.i = 0
        self.text = text

    def peek(self, k: int = 0):
        j = self.i + k
        return self.toks[j] if j < len(self.toks) else (None, None)

    def take(self):
        t = self.peek()
        self.i += 1
        return t

    def expect(self, v):
        t = self.take()
        if t[1] != v:
            raise SyntaxError(f"expected {v!r} got {t[1]!r} in {self.text!r}")

    def kw(self, word):
        k, v = self.peek()
        return k == "id" and v.upper() == word

    def parse(self):
        e = self.or_()
        if self.peek()[0] is not None:
            raise SyntaxError(f"trailing tokens {self.toks[self.i:]} in {self.text!r}")
        return e

    def or_(self):
        e = self.and_()
        while self.kw("OR"):
            self.take()
            e = ("bin", "OR", e, self.and_())
        return e

    def and_(self):
        e = self.not_()
        while self.kw("AND"):
            self.take()
            e = ("bin", "AND", e, self.not_())
        return e

    def not_(self):
        if self.kw("NOT"):
            self.take()
            return ("not", self.not_())
        return self.eq()

    def eq(self):
        e = self.rel()
        while self.peek()[1] in ("=", "!=", "<>", "^="):
            op = self.take()[1]
            e = ("bin", "=" if op == "=" else "!=", e, self.rel())
        return e

    def rel(self):
        e = self.cat()
        while self.peek()[1] in ("<", "<=", ">", ">="):
            op = self.take()[1]
            e = ("bin", op, e, self.cat())
        return e

    def cat(self):
        e = self.add()
        while self.peek()[1] == "||":
            self.take()
            e = ("bin", "||", e, self.add())
        return e

    def add(self):
        e = self.mul()
        while self.peek()[1] in ("+", "-"):
            op = self.take()[1]
            e = ("bin", op, e, self.mul())
        return e

    def mul(self):
        e = self.unary()
        while self.peek()[1] in ("*", "/", "%"):
            op = self.take()[1]
            e = ("bin", op, e, self.unary())
        return e

    def unary(self):
        if self.peek()[1] in ("-", "+"):
            op = self.take()[1]
            x = self.unary()
            return ("neg", x) if op == "-" else x
        return self.primary()

    def primary(self):
        k, v = self.take()
        if v == "(":
            e = self.or_()
            self.expect(")")
            return e
        if k == "str":
            return ("lit", v[1:-1].replace("''", "'"), "s")
        if k == "num":
            return ("lit", Decimal(v), "n")
        if k == "id":
            u = v.upper()
            if self.peek()[1] == "(":
                self.take()
                args = []
                if self.peek()[1] != ")":
                    while True:
                        if self.peek()[1] == "*" and self.peek(1)[1] in (",", ")"):
                            self.take()
                            args.append(("star",))
                        else:
                            args.append(self.or_())
                        if self.peek()[1] == ",":
                            self.take()
                            continue
                        break
                self.expect(")")
                return ("call", u, args)
            if u == "TRUE":
                return ("lit", Decimal(1), "n")
            if u == "FALSE":
                return ("lit", Decimal(0), "n")
            if u == "NULL":
                return ("lit", None, "null")
            if u in ("SYSDATE", "SESSSTARTTIME"):
                return ("sysdate",)
            if v.startswith("$"):
                return ("param", v)
            return ("ref", v)
        raise SyntaxError(f"unexpected token {v!r} in {self.text!r}")


_CACHE: dict[str, tuple] = {}


def parse(text: str) -> tuple:
    if text not in _CACHE:
        _CACHE[text] = _Parser(text).parse()
    return _CACHE[text]


def refs(ast) -> set[str]:
    """Port names referenced by ``ast``."""
    out: set[str] = set()

    def walk(n):
        if n[0] == "ref":
            out.add(n[1])
        elif n[0] in ("bin",):
            walk(n[2]); walk(n[3])
        elif n[0] in ("not", "neg"):
            walk(n[1])
        elif n[0] == "call":
            for a in n[2]:
                walk(a)

    walk(ast)
    return out


def has_aggregate(ast) -> bool:
    if ast[0] == "call":
        return ast[1] in AGGREGATES or any(has_aggregate(a) for a in ast[2])
    if ast[0] == "bin":
        return has_aggregate(ast[2]) or has_aggregate(ast[3])
    if ast[0] in ("not", "neg"):
        return has_aggregate(ast[1])
    return False


# --------------------------------------------------------------------------- PowerCenter types
def pc_type(datatype: str | None) -> str:
    d = (datatype or "string").lower()
    if d in ("string", "nstring", "text", "ntext", "char", "varchar", "varchar2", "nchar", "nvarchar2"):
        return "s"
    if d.startswith("date") or d == "timestamp":
        return "d"
    return "n"


def spark_type(datatype: str | None, precision: Any = None, scale: Any = None) -> str:
    d = (datatype or "string").lower()
    p, s = int(precision or 0), int(scale or 0)
    if d in ("integer", "small integer", "smallint"):
        return "int"
    if d == "bigint":
        return "bigint"
    if d in ("double", "real", "float"):
        return "double"
    if d in ("decimal", "number", "numeric"):
        return f"decimal({min(max(p, 1), 38)},{min(s, 38)})" if p else "decimal(38,10)"
    if pc_type(d) == "d":
        return "timestamp"
    return "string"


# --------------------------------------------------------------------------- Spark SQL rendering
def sql_str(v: str) -> str:
    return "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'"


_NUM_RE = "^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)"


def _date_pattern(fmt: str):
    """PowerCenter date format -> (regex, [(part, group_index)])."""
    regex, parts, i, g = "", [], 0, 0
    tokens = [("YYYY", r"([0-9]{4})", "Y4"), ("HH24", r"([0-9]{1,2})", "H"), ("RR", r"([0-9]{2})", "RR"),
              ("YY", r"([0-9]{2})", "YY"), ("MM", r"([0-9]{1,2})", "M"), ("DD", r"([0-9]{1,2})", "D"),
              ("HH", r"([0-9]{1,2})", "H"), ("MI", r"([0-9]{1,2})", "MI"), ("SS", r"([0-9]{1,2})", "S")]
    while i < len(fmt):
        for tok, rx, part in tokens:
            if fmt[i:].upper().startswith(tok):
                g += 1
                regex += rx
                parts.append((part, g))
                i += len(tok)
                break
        else:
            regex += re.escape(fmt[i])
            i += 1
    return "^" + regex + "$", parts


class SqlGen:
    """Render an AST to Spark SQL.

    ``ports`` maps port name -> (sql, type) for the transformation's ports in scope; ``params`` maps
    ``$$VAR`` / ``$PM...`` names -> python values; ``inline`` maps variable-port names -> AST (aggregator
    variable ports are substituted). In ``agg`` mode, port references outside aggregate functions take the
    last row of the group (PowerCenter returns the last row for non-aggregated ports).
    """

    def __init__(self, ports, params=None, inline=None, agg=False, seq_col="_SEQ", setvars=None):
        self.ports = ports
        self.params = params or {}
        self.inline = inline or {}
        self.agg = agg
        self.seq = seq_col
        self.setvars = setvars if setvars is not None else {}
        self._in_agg = False

    # conversions -------------------------------------------------------
    @staticmethod
    def val(x):
        sql, t = x
        if t == "b":
            return (f"(CASE WHEN {sql} THEN 1 WHEN NOT ({sql}) THEN 0 END)", "n")
        return x

    @staticmethod
    def cond(x):
        sql, t = x
        if t == "b":
            return sql
        if t == "null":
            return "(NULL IS NOT NULL)"
        if t == "s":
            return f"(CAST({sql} AS DOUBLE) != 0)"
        return f"({sql} != 0)"

    @staticmethod
    def as_str(x):
        sql, t = SqlGen.val(x)
        if t == "n":
            s = f"CAST(CAST({sql} AS DECIMAL(38,10)) AS STRING)"
            return f"regexp_replace(regexp_replace({s}, '[.]0+$', ''), '([.][0-9]*[1-9])0+$', '$1')"
        if t == "d":
            return f"date_format({sql}, 'MM/dd/yyyy HH:mm:ss')"
        if t == "null":
            return "CAST(NULL AS STRING)"
        return sql

    # rendering ---------------------------------------------------------
    def render(self, ast, want: str | None = None) -> str:
        """SQL for a port value; ``want`` is the port's PowerCenter type class (for IIF defaults)."""
        self.want = want
        return self.val(self.e(ast))[0]

    def condition(self, ast) -> str:
        self.want = None
        return self.cond(self.e(ast))

    def e(self, n):
        k = n[0]
        if k == "lit":
            v, t = n[1], n[2]
            if t == "s":
                return (sql_str(v), "s")
            if t == "null":
                return ("NULL", "null")
            return (str(v) if v == v.to_integral_value() and abs(v) < 2**31 else f"CAST({v} AS DECIMAL(38,10))", "n")
        if k == "sysdate":
            return ("current_timestamp()", "d")
        if k == "param":
            name = n[1]
            v = self.params.get(name, self.params.get(name.lstrip("$")))
            if v is None:
                return ("NULL", "null")
            if isinstance(v, (int, float, Decimal)):
                return (str(v), "n")
            if isinstance(v, str) and re.fullmatch(r"-?\d+(\.\d+)?", v) and not name.upper().endswith(("_YEAR_NUM", "SUBJECT", "MESSAGE")):
                return (v, "n")
            return (sql_str(str(v)), "s")
        if k == "ref":
            name = n[1]
            if name in self.inline:
                return self.e(self.inline[name])
            if name not in self.ports:
                raise KeyError(f"unknown port {name}")
            sql, t = self.ports[name]
            if self.agg and not self._in_agg:
                return (f"max_by({sql}, {self.seq})", t)
            return (sql, t)
        if k == "neg":
            s, _ = self.val(self.e(n[1]))
            return (f"(-{s})", "n")
        if k == "not":
            return (f"(NOT {self.cond(self.e(n[1]))})", "b")
        if k == "bin":
            op = n[1]
            if op in ("AND", "OR"):
                return (f"({self.cond(self.e(n[2]))} {op} {self.cond(self.e(n[3]))})", "b")
            a, b = self.e(n[2]), self.e(n[3])
            if op == "||":
                return (f"concat_ws('', {self.as_str(a)}, {self.as_str(b)})", "s")
            a, b = self.val(a), self.val(b)
            if op in ("=", "!=", "<", "<=", ">", ">="):
                return (f"({a[0]} {op} {b[0]})", "b")
            t = "d" if "d" in (a[1], b[1]) and op in ("+", "-") else "n"
            return (f"({a[0]} {op} {b[0]})", t)
        if k == "call":
            return self.call(n[1], n[2])
        if k == "star":
            return ("1", "n")
        raise ValueError(n)

    def _default_for(self, t):
        return {"n": "0", "s": "''"}.get(t, "NULL")

    def call(self, f, args):
        e, val, cond, as_str = self.e, self.val, self.cond, self.as_str
        if f in AGGREGATES:
            if not self.agg:
                raise ValueError(f"aggregate {f} outside an Aggregator")
            self._in_agg = True
            try:
                x = val(e(args[0])) if args[0][0] != "star" else ("1", "n")
                c = cond(e(args[1])) if len(args) > 1 else None
            finally:
                self._in_agg = False
            xv = x[0] if c is None else f"CASE WHEN {c} THEN {x[0]} END"
            if f == "SUM":
                return (f"sum({xv})", "n")
            if f == "AVG":
                return (f"avg({xv})", "n")
            if f == "COUNT":
                return (f"count({xv})", "n")
            if f in ("MAX", "MIN"):
                return (f"{f.lower()}({xv})", x[1])
            order = f"CASE WHEN {c or 'TRUE'} AND {x[0]} IS NOT NULL THEN {self.seq} END"
            return (f"{'min_by' if f == 'FIRST' else 'max_by'}({x[0]}, {order})", x[1])
        if f == "ISNULL":
            return (f"({e(args[0])[0]} IS NULL)", "b")
        if f == "IIF":
            c = cond(e(args[0]))
            a = val(e(args[1]))
            if len(args) > 2:
                b = val(e(args[2]))
            else:
                b = (self._default_for(a[1] if a[1] != "null" else (self.want or "null")), a[1])
            t = a[1] if a[1] != "null" else b[1]
            return (f"(CASE WHEN {c} THEN {a[0]} ELSE {b[0]} END)", t)
        if f == "DECODE":
            subject = args[0]
            rest = args[1:]
            pairs, default = rest[: len(rest) // 2 * 2], (rest[-1] if len(rest) % 2 else None)
            parts, t = [], "null"
            is_true = subject[0] == "lit" and subject[2] == "n" and subject[1] == 1
            sv = None if is_true else val(e(subject))
            for i in range(0, len(pairs), 2):
                if is_true:
                    w = cond(e(pairs[i]))
                else:
                    w = f"{sv[0]} = {val(e(pairs[i]))[0]}"
                r = val(e(pairs[i + 1]))
                t = r[1] if t == "null" else t
                parts.append(f"WHEN {w} THEN {r[0]}")
            d = val(e(default)) if default is not None else ("NULL", "null")
            t = d[1] if t == "null" else t
            return (f"(CASE {' '.join(parts)} ELSE {d[0]} END)", t)
        if f == "IN":
            x = val(e(args[0]))
            items = [val(e(a))[0] for a in args[1:]]
            return (f"({x[0]} IN ({', '.join(items)}))", "b")
        if f == "SUBSTR":
            s = as_str(e(args[0]))
            st = f"CAST({val(e(args[1]))[0]} AS INT)"
            if len(args) > 2:
                ln = f"CAST({val(e(args[2]))[0]} AS INT)"
                return (f"substring({s}, {st}, greatest({ln}, 0))", "s")
            return (f"substring({s}, {st})", "s")
        if f == "INSTR":
            s, sub = as_str(e(args[0])), as_str(e(args[1]))
            if len(args) == 2:
                return (f"instr({s}, {sub})", "n")
            return (f"locate({sub}, {s}, CAST({val(e(args[2]))[0]} AS INT))", "n")
        if f == "LENGTH":
            return (f"length({as_str(e(args[0]))})", "n")
        if f in ("UPPER", "LOWER"):
            return (f"{f.lower()}({as_str(e(args[0]))})", "s")
        if f in ("LTRIM", "RTRIM"):
            s = as_str(e(args[0]))
            if len(args) > 1:
                side = "LEADING" if f == "LTRIM" else "TRAILING"
                return (f"TRIM({side} {as_str(e(args[1]))} FROM {s})", "s")
            return (f"{f.lower()}({s})", "s")
        if f in ("LPAD", "RPAD"):
            s = as_str(e(args[0]))
            ln = f"CAST({val(e(args[1]))[0]} AS INT)"
            pad = as_str(e(args[2])) if len(args) > 2 else "' '"
            return (f"{f.lower()}({s}, {ln}, {pad})", "s")
        if f == "CHR":
            return (f"char(CAST({val(e(args[0]))[0]} AS INT))", "s")
        if f == "ABS":
            return (f"abs({val(e(args[0]))[0]})", "n")
        if f == "SIGN":
            return (f"CAST(signum({val(e(args[0]))[0]}) AS INT)", "n")
        if f in ("ROUND", "TRUNC"):
            x = val(e(args[0]))
            if x[1] == "d":
                fmt = "DAY" if len(args) < 2 else {"YYYY": "YEAR", "YY": "YEAR", "MM": "MONTH", "DD": "DAY"}.get(args[1][1].upper(), "DAY")
                return (f"date_trunc('{fmt}', {x[0]})", "d")
            p = int(args[1][1]) if len(args) > 1 and args[1][0] == "lit" else 0
            if f == "ROUND":
                return (f"round({x[0]}, {p})", "n")
            m = 10**p
            sc = f"CAST({x[0]} AS DECIMAL(38,10))"
            if p == 0:
                return (f"(CASE WHEN {sc} >= 0 THEN floor({sc}) ELSE ceil({sc}) END)", "n")
            return (f"(CASE WHEN {sc} >= 0 THEN floor({sc} * {m}) ELSE ceil({sc} * {m}) END / {m})", "n")
        if f == "TO_DECIMAL" or f == "TO_INTEGER" or f == "TO_FLOAT" or f == "TO_BIGINT":
            x = val(e(args[0]))
            scale = int(args[1][1]) if f == "TO_DECIMAL" and len(args) > 1 and args[1][0] == "lit" else (0 if f in ("TO_INTEGER", "TO_BIGINT") else 10)
            typ = f"DECIMAL(38,{scale})"
            if x[1] == "n":
                if f in ("TO_INTEGER", "TO_BIGINT"):
                    return (f"CAST({x[0]} AS BIGINT)", "n")
                return (f"CAST(round({x[0]}, {scale}) AS {typ})", "n")
            s = as_str(x)
            num = f"nullif(regexp_extract(trim({s}), '{_NUM_RE}', 0), '')"
            if f in ("TO_INTEGER", "TO_BIGINT"):
                return (f"(CASE WHEN {s} IS NULL THEN NULL ELSE coalesce(CAST(CAST({num} AS DECIMAL(38,10)) AS BIGINT), 0) END)", "n")
            return (f"(CASE WHEN {s} IS NULL THEN NULL ELSE coalesce(CAST(round(CAST({num} AS DECIMAL(38,10)), {scale}) AS {typ}), 0) END)", "n")
        if f == "TO_CHAR":
            x = val(e(args[0]))
            if x[1] == "d":
                fmt = args[1][1] if len(args) > 1 and args[1][0] == "lit" else "MM/DD/YYYY HH24:MI:SS"
                return (f"date_format({x[0]}, {sql_str(_spark_datefmt(fmt))})", "s")
            return (as_str(x), "s")
        if f == "IS_NUMBER":
            x = val(e(args[0]))
            if x[1] == "n":
                return (f"({x[0]} IS NOT NULL)", "b")
            s = as_str(x)
            return (f"(CASE WHEN {s} IS NULL THEN NULL ELSE trim({s}) RLIKE '{_NUM_RE}([eE][+-]?[0-9]+)?$' END)", "b")
        if f == "IS_SPACES":
            s = as_str(e(args[0]))
            return (f"(CASE WHEN {s} IS NULL THEN NULL ELSE length({s}) > 0 AND trim({s}) = '' END)", "b")
        if f in ("IS_DATE", "TO_DATE"):
            s = as_str(e(args[0]))
            fmt = args[1][1] if len(args) > 1 else "MM/DD/YYYY HH24:MI:SS"
            ts = _sql_parse_date(s, fmt)
            if f == "TO_DATE":
                return (ts, "d")
            return (f"(CASE WHEN {s} IS NULL THEN NULL ELSE {ts} IS NOT NULL END)", "b")
        if f == "GET_DATE_PART":
            d = val(e(args[0]))[0]
            part = args[1][1].upper()
            fn = "year" if part.startswith("Y") else "month" if part.startswith("M") and part != "MI" else "day" if part.startswith("D") else "hour" if part.startswith("H") else "minute" if part == "MI" else "second"
            return (f"{fn}({d})", "n")
        if f == "ABORT":
            return (f"raise_error({as_str(e(args[0]))})", "null")
        if f == "ERROR":
            return ("NULL", "null")
        if f == "SETVARIABLE":
            v = val(e(args[1]))
            self.setvars[args[0][1]] = v[0]
            return v
        raise NotImplementedError(f"PowerCenter function {f}")


def _spark_datefmt(fmt: str) -> str:
    out = fmt
    for a, b in (("YYYY", "yyyy"), ("HH24", "HH"), ("MI", "mm"), ("SS", "ss"), ("DD", "dd"), ("RR", "yy"), ("YY", "yy")):
        out = out.replace(a, b)
    return out


def _sql_parse_date(s: str, fmt: str) -> str:
    regex, parts = _date_pattern(fmt)
    rx = sql_str(regex)
    comp = {"Y": "1900", "M": "1", "D": "1", "H": "0", "MI": "0", "S": "0"}
    for part, g in parts:
        grp = f"CAST(regexp_extract({s}, {rx}, {g}) AS INT)"
        if part == "Y4":
            comp["Y"] = grp
        elif part == "RR":
            comp["Y"] = f"(CASE WHEN {grp} < 50 THEN 2000 + {grp} ELSE 1900 + {grp} END)"
        elif part == "YY":
            comp["Y"] = f"(2000 + {grp})"
        else:
            comp[part] = grp
    mk = f"make_timestamp({comp['Y']}, {comp['M']}, {comp['D']}, {comp['H']}, {comp['MI']}, {comp['S']})"
    return f"(CASE WHEN {s} RLIKE {rx} THEN {mk} END)"


# --------------------------------------------------------------------------- Python row evaluation
class Abort(RuntimeError):
    """PowerCenter ABORT()."""


def _dec(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return Decimal(int(v))
    if isinstance(v, Decimal):
        return v
    if isinstance(v, (int,)):
        return Decimal(v)
    if isinstance(v, float):
        return Decimal(repr(v))
    if isinstance(v, str):
        try:
            return Decimal(v.strip())
        except InvalidOperation:
            return None
    return v


def py_str(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (Decimal, int, float)):
        d = _dec(v)
        s = format(d, "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s if s not in ("-0", "") else "0"
    if isinstance(v, _dt.datetime):
        return v.strftime("%m/%d/%Y %H:%M:%S")
    return str(v)


def _truth(v):
    if v is None:
        return False
    if isinstance(v, str):
        d = _dec(v)
        return bool(d)
    return bool(v)


def _py_to_decimal(v, scale=None):
    if v is None:
        return None
    if isinstance(v, (Decimal, int, float, bool)):
        d = _dec(v)
    else:
        m = re.match(_NUM_RE, str(v).strip())
        d = Decimal(m.group(0)) if m and m.group(0) not in ("+", "-", "") else Decimal(0)
    if scale is not None:
        d = d.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)
    return d


def _py_parse_date(s, fmt):
    if s is None:
        return None
    regex, parts = _date_pattern(fmt)
    m = re.match(regex, s)
    if not m:
        return None
    comp = {"Y": 1900, "M": 1, "D": 1, "H": 0, "MI": 0, "S": 0}
    for part, g in parts:
        v = int(m.group(g))
        if part == "Y4":
            comp["Y"] = v
        elif part == "RR":
            comp["Y"] = 2000 + v if v < 50 else 1900 + v
        elif part == "YY":
            comp["Y"] = 2000 + v
        else:
            comp[part] = v
    try:
        return _dt.datetime(comp["Y"], comp["M"], comp["D"], comp["H"], comp["MI"], comp["S"])
    except ValueError:
        return None


def compile_py(ast, params: dict, types: dict[str, str], want: str | None = None, setvars: dict | None = None) -> Callable[[dict], Any]:
    """Compile ``ast`` to ``fn(env) -> value`` with PowerCenter semantics (row-at-a-time)."""

    def typ(n):
        k = n[0]
        if k == "lit":
            return n[2]
        if k == "ref":
            return types.get(n[1], "s")
        if k == "sysdate":
            return "d"
        if k == "call":
            f = n[1]
            if f in ("IIF",):
                return typ(n[2][1])
            if f == "DECODE":
                return typ(n[2][2]) if len(n[2]) > 2 else "null"
            if f in ("SUBSTR", "LTRIM", "RTRIM", "LPAD", "RPAD", "CHR", "TO_CHAR", "UPPER", "LOWER"):
                return "s"
            if f in ("TO_DATE",):
                return "d"
            if f in ("TRUNC", "ROUND"):
                return typ(n[2][0])
            if f in ("MAX", "MIN", "FIRST", "LAST"):
                return typ(n[2][0])
            if f == "SETVARIABLE":
                return typ(n[2][1])
            return "n"
        if k == "bin" and n[1] == "||":
            return "s"
        return "n"

    def c(n, want_t=None):
        k = n[0]
        if k == "lit":
            v = n[1]
            return lambda env: v
        if k == "sysdate":
            return lambda env: params.get("__SESSSTARTTIME") or _dt.datetime.now()
        if k == "param":
            name = n[1]
            v = params.get(name, params.get(name.lstrip("$")))
            if isinstance(v, str) and re.fullmatch(r"-?\d+(\.\d+)?", v) and not name.upper().endswith(("_YEAR_NUM", "SUBJECT", "MESSAGE")):
                v = Decimal(v)
            return lambda env: v
        if k == "ref":
            name = n[1]
            return lambda env: env.get(name)
        if k == "neg":
            x = c(n[1])
            return lambda env: (None if (v := _dec(x(env))) is None else -v)
        if k == "not":
            x = c(n[1])
            return lambda env: (None if (v := x(env)) is None else (0 if _truth(v) else 1))
        if k == "bin":
            op, a, b = n[1], c(n[2]), c(n[3])
            if op == "AND":
                return lambda env: 1 if _truth(a(env)) and _truth(b(env)) else 0
            if op == "OR":
                return lambda env: 1 if _truth(a(env)) or _truth(b(env)) else 0
            if op == "||":
                def cat(env):
                    x, y = a(env), b(env)
                    return (py_str(x) or "") + (py_str(y) or "")
                return cat
            if op in ("=", "!=", "<", "<=", ">", ">="):
                import operator as o
                fn = {"=": o.eq, "!=": o.ne, "<": o.lt, "<=": o.le, ">": o.gt, ">=": o.ge}[op]

                def cmp(env):
                    x, y = a(env), b(env)
                    if x is None or y is None:
                        return None
                    if isinstance(x, str) and isinstance(y, str):
                        return 1 if fn(x, y) else 0
                    if isinstance(x, _dt.datetime) or isinstance(y, _dt.datetime):
                        return 1 if fn(x, y) else 0
                    x, y = _dec(x), _dec(y)
                    if x is None or y is None:
                        return None
                    return 1 if fn(x, y) else 0
                return cmp

            def arith(env):
                x, y = _dec(a(env)), _dec(b(env))
                if x is None or y is None:
                    return None
                if op == "+":
                    return x + y
                if op == "-":
                    return x - y
                if op == "*":
                    return x * y
                if y == 0:
                    return None
                return x / y if op == "/" else x % y
            return arith
        if k == "call":
            return fcall(n[1], n[2], want_t)
        if k == "star":
            return lambda env: 1
        raise ValueError(n)

    def fcall(f, args, want_t):
        A = [c(x) for x in args if x[0] != "star"]
        if f == "ISNULL":
            return lambda env: 1 if A[0](env) is None else 0
        if f == "IIF":
            t = typ(args[1])
            dflt = {"n": Decimal(0), "s": ""}.get(t if t != "null" else (want_t or want or "null"))
            fb = A[2] if len(A) > 2 else (lambda env: dflt)
            return lambda env: A[1](env) if _truth(A[0](env)) else fb(env)
        if f == "DECODE":
            subject = args[0]
            is_true = subject[0] == "lit" and subject[2] == "n" and subject[1] == 1
            rest = A[1:]
            pairs = [(rest[i], rest[i + 1]) for i in range(0, len(rest) - 1, 2)]
            default = rest[-1] if len(rest) % 2 else (lambda env: None)

            def dec(env):
                if is_true:
                    for w, r in pairs:
                        if _truth(w(env)):
                            return r(env)
                    return default(env)
                sv = A[0](env)
                for w, r in pairs:
                    wv = w(env)
                    if sv is not None and wv is not None:
                        if (isinstance(sv, str) and isinstance(wv, str) and sv == wv) or (
                            not isinstance(sv, str) or not isinstance(wv, str)) and _dec(sv) == _dec(wv):
                            return r(env)
                return default(env)
            return dec
        if f == "IN":
            def _in(env):
                x = A[0](env)
                if x is None:
                    return None
                for o in A[1:]:
                    y = o(env)
                    if y is not None and (x == y or (not isinstance(x, str) and _dec(x) == _dec(y))):
                        return 1
                return 0
            return _in
        if f == "SUBSTR":
            def sub(env):
                s = py_str(A[0](env))
                st = _dec(A[1](env))
                if s is None or st is None:
                    return None
                st = int(st)
                ln = int(_dec(A[2](env))) if len(A) > 2 and A[2](env) is not None else None
                if ln is not None and ln <= 0:
                    return ""
                i = st - 1 if st > 0 else (len(s) + st if st < 0 else 0)
                i = max(i, 0)
                return s[i:] if ln is None else s[i:i + ln]
            return sub
        if f == "INSTR":
            def ins(env):
                s, sub = py_str(A[0](env)), py_str(A[1](env))
                if s is None or sub is None:
                    return None
                start = int(_dec(A[2](env))) - 1 if len(A) > 2 else 0
                return Decimal(s.find(sub, start) + 1)
            return ins
        if f == "LENGTH":
            return lambda env: None if (s := py_str(A[0](env))) is None else Decimal(len(s))
        if f in ("UPPER", "LOWER"):
            return lambda env: None if (s := py_str(A[0](env))) is None else (s.upper() if f == "UPPER" else s.lower())
        if f in ("LTRIM", "RTRIM"):
            def tr(env):
                s = py_str(A[0](env))
                if s is None:
                    return None
                chars = py_str(A[1](env)) if len(A) > 1 else " "
                return s.lstrip(chars) if f == "LTRIM" else s.rstrip(chars)
            return tr
        if f in ("LPAD", "RPAD"):
            def pad(env):
                s = py_str(A[0](env))
                n = _dec(A[1](env))
                if s is None or n is None:
                    return None
                n = int(n)
                p = py_str(A[2](env)) if len(A) > 2 else " "
                if len(s) >= n:
                    return s[:n]
                fill = (p * n)[: n - len(s)]
                return fill + s if f == "LPAD" else s + fill
            return pad
        if f == "CHR":
            return lambda env: None if (v := _dec(A[0](env))) is None else chr(int(v))
        if f == "ABS":
            return lambda env: None if (v := _dec(A[0](env))) is None else abs(v)
        if f == "SIGN":
            return lambda env: None if (v := _dec(A[0](env))) is None else Decimal((v > 0) - (v < 0))
        if f in ("TRUNC", "ROUND"):
            p = int(args[1][1]) if len(args) > 1 and args[1][0] == "lit" and isinstance(args[1][1], Decimal) else 0

            def tr(env):
                v = A[0](env)
                if v is None:
                    return None
                if isinstance(v, _dt.datetime):
                    return v.replace(hour=0, minute=0, second=0, microsecond=0)
                d = _dec(v)
                q = Decimal(1).scaleb(-p)
                from decimal import ROUND_DOWN
                return d.quantize(q, rounding=ROUND_DOWN if f == "TRUNC" else ROUND_HALF_UP)
            return tr
        if f in ("TO_DECIMAL", "TO_INTEGER", "TO_BIGINT", "TO_FLOAT"):
            scale = int(args[1][1]) if f == "TO_DECIMAL" and len(args) > 1 else (0 if f in ("TO_INTEGER", "TO_BIGINT") else None)
            return lambda env: _py_to_decimal(A[0](env), scale)
        if f == "TO_CHAR":
            if len(args) > 1:
                fmt = args[1][1]

                def tc(env):
                    v = A[0](env)
                    if isinstance(v, _dt.datetime):
                        out = fmt
                        for a, b in (("YYYY", "%Y"), ("HH24", "%H"), ("MI", "%M"), ("SS", "%S"), ("MM", "%m"), ("DD", "%d"), ("RR", "%y"), ("YY", "%y")):
                            out = out.replace(a, b)
                        return v.strftime(out)
                    return py_str(v)
                return tc
            return lambda env: py_str(A[0](env))
        if f == "IS_NUMBER":
            def isn(env):
                v = A[0](env)
                if v is None:
                    return None
                if not isinstance(v, str):
                    return 1
                return 1 if re.fullmatch(_NUM_RE[1:] + r"([eE][+-]?[0-9]+)?", v.strip()) else 0
            return isn
        if f == "IS_SPACES":
            return lambda env: None if (s := A[0](env)) is None else (1 if len(s) > 0 and s.strip() == "" else 0)
        if f in ("IS_DATE", "TO_DATE"):
            fmt = args[1][1] if len(args) > 1 else "MM/DD/YYYY HH24:MI:SS"
            if f == "TO_DATE":
                return lambda env: _py_parse_date(py_str(A[0](env)), fmt)
            return lambda env: None if (s := A[0](env)) is None else (1 if _py_parse_date(py_str(s), fmt) else 0)
        if f == "GET_DATE_PART":
            part = args[1][1].upper()

            def gdp(env):
                d = A[0](env)
                if d is None:
                    return None
                if part.startswith("Y"):
                    return Decimal(d.year)
                if part == "MI":
                    return Decimal(d.minute)
                if part.startswith("M"):
                    return Decimal(d.month)
                if part.startswith("D"):
                    return Decimal(d.day)
                if part.startswith("H"):
                    return Decimal(d.hour)
                return Decimal(d.second)
            return gdp
        if f == "ABORT":
            def ab(env):
                raise Abort(py_str(A[0](env)))
            return ab
        if f == "ERROR":
            return lambda env: None
        if f == "SETVARIABLE":
            name = args[0][1]

            def sv(env):
                v = A[1](env)
                if setvars is not None and v is not None:
                    cur = setvars.get(name)
                    setvars[name] = v if cur is None or v > cur else cur
                return v
            return sv
        raise NotImplementedError(f"PowerCenter function {f}")

    return c(ast, want)


def py_cast(v, datatype: str | None, precision=None, scale=None):
    """Cast a Python value to a port's datatype (string ports truncate to their precision)."""
    if v is None:
        return None
    t = spark_type(datatype, precision, scale)
    if t == "string":
        s = py_str(v)
        p = int(precision or 0)
        return s[:p] if p else s
    if t == "timestamp":
        return v if isinstance(v, _dt.datetime) else None
    d = _dec(v)
    if d is None:
        return None
    if t in ("int", "bigint"):
        return int(d.to_integral_value(rounding=ROUND_HALF_UP))
    if t == "double":
        return float(d)
    s = int(scale or 0)
    q = d.quantize(Decimal(1).scaleb(-s), rounding=ROUND_HALF_UP)
    p = int(precision or 0)
    if p and len(q.as_tuple().digits) - (0 if q == 0 else 0) > p and abs(q) >= Decimal(10) ** (p - s):
        return None
    return q
