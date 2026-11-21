"""Translate PowerCenter transformation-language expressions to Spark SQL.

Covers the function set used by the CPM extract mappings (see PLAN.md §3.1). Port references are
resolved through a callback so callers can inline upstream ports (``_codegen``) or map them to
DataFrame columns. ``$$VAR`` mapping variables become ``__PARAM_VAR__`` placeholders that
``bind_params`` replaces with SQL literals at run time.
"""
from __future__ import annotations

import re
from typing import Callable

_TOKEN = re.compile(
    r"""
    (?P<ws>\s+)
  | (?P<comment>(--|//)[^\n]*)
  | (?P<str>'(?:[^']|'')*')
  | (?P<num>\d+\.\d*|\.\d+|\d+)
  | (?P<op>\|\||<=|>=|<>|!=|[=<>+\-*/%(),])
  | (?P<ident>(\$\$|\$|:LKP\.)?[A-Za-z_][A-Za-z0-9_#.]*)
    """,
    re.VERBOSE,
)

IS_NUMBER_RE = r"^\\s*[+-]?(\\d+\\.?\\d*|\\.\\d+)([eE][+-]?\\d+)?\\s*$"


def tokenize(src: str) -> list[tuple[str, str]]:
    out, pos = [], 0
    while pos < len(src):
        m = _TOKEN.match(src, pos)
        if not m:
            raise ValueError(f"cannot tokenize at {src[pos:pos + 30]!r}")
        pos = m.end()
        kind = m.lastgroup
        if kind in ("ws", "comment"):
            continue
        out.append((kind, m.group(kind)))
    return out


class _Parser:
    # PowerCenter precedence, lowest first.
    _BINARY = [("OR",), ("AND",), ("=", "<>", "!="), ("<", "<=", ">", ">="), ("||",), ("+", "-"), ("*", "/", "%")]

    def __init__(self, tokens):
        self.t, self.i = tokens, 0

    def peek(self):
        return self.t[self.i] if self.i < len(self.t) else (None, None)

    def take(self, value=None):
        tok = self.peek()
        if value is not None and (tok[1] or "").upper() != value:
            raise ValueError(f"expected {value}, got {tok}")
        self.i += 1
        return tok

    def parse(self):
        node = self.expr(0)
        if self.i != len(self.t):
            raise ValueError(f"trailing tokens: {self.t[self.i:]}")
        return node

    def expr(self, level):
        if level == len(self._BINARY):
            return self.unary()
        if self._BINARY[level] == ("AND",):
            left = self.not_expr(level)
        else:
            left = self.expr(level + 1)
        while (self.peek()[1] or "").upper() in self._BINARY[level]:
            op = self.take()[1].upper()
            right = self.not_expr(level) if self._BINARY[level] == ("AND",) else self.expr(level + 1)
            left = ("bin", op, left, right)
        return left

    def not_expr(self, level):
        if (self.peek()[1] or "").upper() == "NOT":
            self.take()
            return ("not", self.not_expr(level))
        return self.expr(level + 1)

    def unary(self):
        kind, val = self.peek()
        if val in ("-", "+"):
            self.take()
            return ("neg", self.unary()) if val == "-" else self.unary()
        if (val or "").upper() == "NOT":
            self.take()
            return ("not", self.unary())
        return self.primary()

    def primary(self):
        kind, val = self.take()
        if kind == "num":
            return ("num", val)
        if kind == "str":
            return ("str", val)
        if val == "(":
            node = self.expr(0)
            self.take(")")
            return node
        if kind == "ident":
            up = val.upper()
            if self.peek()[1] == "(":
                self.take("(")
                args = []
                if self.peek()[1] != ")":
                    args.append(self.expr(0))
                    while self.peek()[1] == ",":
                        self.take()
                        args.append(self.expr(0))
                self.take(")")
                return ("call", up, args)
            if up in ("TRUE", "FALSE"):
                return ("bool", up)
            if up == "NULL":
                return ("null",)
            return ("ref", val)
        raise ValueError(f"unexpected token {val!r}")


def java_date_format(fmt: str) -> str:
    """PowerCenter/Oracle date format -> Spark datetime pattern."""
    out = fmt
    for pc, java in (("HH24", "HH"), ("YYYY", "yyyy"), ("MON", "MMM"), ("MI", "mm"), ("SS", "ss"), ("RR", "yy"), ("YY", "yy"), ("DD", "dd")):
        out = out.replace(pc, java)
    return out


def parse(src: str):
    return _Parser(tokenize(src)).parse()


def _s(x: str) -> str:
    if re.fullmatch(r"'(?:[^']|'')*'", x) or x.startswith(("concat(", "lpad(", "rpad(", "substring(", "CAST(")) and x.endswith((" AS STRING)", ")")) and not x.startswith("CAST(") or x.endswith(" AS STRING)"):
        return x
    return f"CAST({x} AS STRING)"


class Translator:
    def __init__(self, ref: Callable[[str], str]):
        self.ref = ref

    def sql(self, node) -> str:
        kind = node[0]
        if kind == "num":
            return node[1]
        if kind == "str":
            return node[1]
        if kind == "bool":
            return node[1]
        if kind == "null":
            return "NULL"
        if kind == "ref":
            name = node[1]
            if name.upper() in ("SYSDATE", "SESSSTARTTIME"):
                return "current_timestamp()"
            if name.startswith("$$"):
                return f"__PARAM_{name[2:]}__"
            return self.ref(name)
        if kind == "neg":
            return f"(- {self.sql(node[1])})"
        if kind == "not":
            return f"(NOT {self.sql(node[1])})"
        if kind == "bin":
            op, a, b = node[1], self.sql(node[2]), self.sql(node[3])
            if op == "||":
                # PowerCenter concatenation treats NULL as an empty string.
                return f"concat(coalesce({_s(a)}, ''), coalesce({_s(b)}, ''))"
            if op == "!=":
                op = "<>"
            return f"({a} {op} {b})"
        if kind == "call":
            return self.call(node[1], node[2])
        raise ValueError(node)

    def call(self, fn: str, args: list) -> str:
        a = [self.sql(x) for x in args]
        if fn == "IIF":
            return f"(CASE WHEN {a[0]} THEN {a[1]} ELSE {a[2] if len(a) > 2 else 'NULL'} END)"
        if fn == "DECODE":
            subject, rest = args[0], a[1:]
            default = rest.pop() if len(rest) % 2 else "NULL"
            pairs = list(zip(rest[0::2], rest[1::2]))
            if subject[0] == "bool" and subject[1] == "TRUE":
                whens = " ".join(f"WHEN {c} THEN {r}" for c, r in pairs)
            else:
                whens = " ".join(f"WHEN {a[0]} = {c} THEN {r}" for c, r in pairs)
            return f"(CASE {whens} ELSE {default} END)"
        if fn == "ISNULL":
            return f"({a[0]} IS NULL)"
        if fn == "IS_SPACES":
            return f"(length({_s(a[0])}) > 0 AND trim({_s(a[0])}) = '')"
        if fn == "IS_NUMBER":
            return f"coalesce({_s(a[0])} RLIKE '{IS_NUMBER_RE}', FALSE)"
        if fn == "TO_CHAR":
            return _s(a[0])
        if fn == "TO_DECIMAL":
            scale = args[1][1] if len(args) > 1 else "0"
            return f"CAST({a[0]} AS DECIMAL(28,{scale}))"
        if fn == "TO_INTEGER":
            return f"CAST({a[0]} AS INT)"
        if fn in ("LPAD", "RPAD"):
            pad = a[2] if len(a) > 2 else "' '"
            return f"{fn.lower()}({_s(a[0])}, {a[1]}, {pad})"
        if fn in ("LTRIM", "RTRIM"):
            if len(a) == 1:
                return f"{fn.lower()}({_s(a[0])})"
            side = "LEADING" if fn == "LTRIM" else "TRAILING"
            return f"trim({side} {a[1]} FROM {_s(a[0])})"
        if fn == "SUBSTR":
            length = f", {a[2]}" if len(a) > 2 else ""
            return f"substring({_s(a[0])}, {a[1]}{length})"
        if fn == "LENGTH":
            return f"length({_s(a[0])})"
        if fn in ("UPPER", "LOWER", "ABS"):
            return f"{fn.lower()}({a[0]})"
        if fn == "SIGN":
            return f"CAST(signum({a[0]}) AS INT)"
        if fn == "TRUNC":
            p = f", {a[1]}" if len(a) > 1 else ""
            return f"(CASE WHEN {a[0]} < 0 THEN ceil({a[0]}{p}) ELSE floor({a[0]}{p}) END)"
        if fn == "REPLACECHR":
            chars = args[2]
            new = "''" if args[3][0] == "null" else f"substring({a[3]}, 1, 1)"
            if chars[0] != "str":
                raise ValueError("REPLACECHR with non-literal char set")
            cls = "".join("\\\\" + c if c in "]\\^-[" else c.replace("'", "''") for c in chars[1][1:-1].replace("''", "'"))
            return f"regexp_replace({_s(a[1])}, '[{cls}]', {new})"
        if fn == "REPLACESTR":
            out = _s(a[1])
            new = "''" if args[-1][0] == "null" else a[-1]
            for old in a[2:-1]:
                out = f"replace({out}, {old}, {new})"
            return out
        if fn == "INSTR":
            if len(a) > 3:
                raise ValueError("INSTR with occurrence argument unsupported")
            start = a[2] if len(a) > 2 else "1"
            return f"locate({_s(a[1])}, {_s(a[0])}, {start})"
        if fn == "ROUND":
            return f"round({a[0]}, {a[1] if len(a) > 1 else 0})"
        if fn == "GET_DATE_PART":
            part = args[1][1].strip("'").upper()
            sp = {"YYYY": "year", "YY": "year", "Y": "year", "MM": "month", "MON": "month", "DD": "day", "HH": "hour", "HH24": "hour", "MI": "minute", "SS": "second"}[part]
            return f"{sp}({a[0]})"
        if fn in ("TO_DATE", "IS_DATE"):
            fmt = f", '{java_date_format(args[1][1][1:-1])}'" if len(args) > 1 else ""
            expr = f"to_timestamp({_s(a[0])}{fmt})"
            return expr if fn == "TO_DATE" else f"({expr} IS NOT NULL)"
        if fn == "ADD_TO_DATE":
            part = args[1][1].strip("'").upper()
            if part in ("DD", "D", "DDD", "DY", "DAY"):
                return f"({a[0]} + make_interval(0, 0, 0, {a[2]}))"
            if part in ("MM", "MON", "MONTH"):
                return f"add_months({a[0]}, {a[2]})"
            if part in ("YYYY", "YY", "Y", "YEAR"):
                return f"add_months({a[0]}, 12 * ({a[2]}))"
            raise ValueError(f"ADD_TO_DATE part {part} unsupported")
        if fn == "LAST_DAY":
            return f"last_day({a[0]})"
        if fn == "CHR":
            return f"chr({a[0]})"
        if fn == "IN":
            return f"({a[0]} IN ({', '.join(a[1:])}))"
        raise ValueError(f"unsupported function {fn}")


def to_sql(src: str, ref: Callable[[str], str] = lambda n: f"`{n}`") -> str:
    return Translator(ref).sql(parse(src))


def bind_params(sql: str, params: dict[str, str]) -> str:
    def lit(m):
        v = params.get(m.group(1), "")
        return "'" + str(v).replace("'", "''") + "'"

    return re.sub(r"__PARAM_(\w+?)__", lit, sql)
