"""Emit PySpark/Delta workflow modules from the pc2dbx model (conventions: databricks/PLAN.md)."""
from __future__ import annotations

import re
from collections import OrderedDict
from dataclasses import dataclass, field

from .expr import Translator, param_key, spark_type
from .model import Folder, Instance, Mapping, Port, Session, Transformation, Workflow

# PLAN.md §2.1: workflow -> (package area, module name)
AREAS = {
    "wf_Pay_Calendar": "reference", "wf_Pseudossn": "reference", "wf_COMPTIME": "reference",
    "wf_CPM": "cpm", "wf_CPM_AFPS": "cpm_extracts", "wf_CPM_CDC": "cpm_extracts",
    "wf_CPM_NIH": "cpm_extracts", "wf_CPM_OIG": "cpm_extracts", "wf_FDA_Leave": "cpm_extracts",
    "wf_LES": "les", "wf_EHRP2BIIS_UPDATE": "ehrp",
}
FILE_TYPES = {"Flat File", "VSAM", "PWX_SEQ_NRDB2"}
ACTIVE = {"Source Qualifier", "Aggregator", "Joiner", "Normalizer", "Source Definition", "Mapplet"}

HELPERS = '''
def _sql(ctx: RunContext, text: str, literal: bool = False) -> str:
    """Resolve ``${KEY}`` placeholders (PowerCenter ``$$KEY`` / ``$Param_KEY``) from job parameters.

    SQL overrides get raw text substitution (as PowerCenter does); expressions (``literal=True``)
    get SQL literals: quoted strings unless the variable is declared numeric.
    """

    def sub(m: re.Match) -> str:
        key = m.group(1)
        value = ctx.param(key, PARAM_DEFAULTS.get(key))
        if literal and key not in NUMERIC_PARAMS:
            return "'" + value.replace("\\\\", "\\\\\\\\").replace("'", "\\\\'") + "'"
        return value if value != "" or not literal else "NULL"

    return re.sub(r"\\$\\{(\\w+)\\}", sub, text)


def _expr(ctx: RunContext, text: str):
    return F.expr(_sql(ctx, text, literal=True))


def _with_rid(df: DataFrame) -> DataFrame:
    """Row identity used to re-align passive branches (PowerCenter row pipeline)."""
    return df.withColumn("_rid", F.monotonically_increasing_id())


def _first_match(df: DataFrame, order: list[str]) -> DataFrame:
    """Keep one lookup match per input row (Lookup policy on multiple match)."""
    w = Window.partitionBy("_rid").orderBy(*[F.col(c).asc_nulls_last() for c in order])
    return df.withColumn("_lkp_rn", F.row_number().over(w)).filter("_lkp_rn = 1").drop("_lkp_rn")


def _views(ctx: RunContext, tables: dict[str, str | None]) -> None:
    """Register Unity Catalog tables under their PowerCenter names for ported SQL overrides."""
    for name, owner in tables.items():
        ctx.table(name, owner).createOrReplaceTempView(name.lower())


def _ensure(df: DataFrame, cols: list[str]) -> DataFrame:
    """Lookup SQL overrides return a subset of lookup ports; add the missing ones as NULL."""
    have = {c.lower() for c in df.columns}
    for c in cols:
        if c.lower() not in have:
            df = df.withColumn(c, F.lit(None))
    return df


def _assign(ctx: RunContext, pairs: dict[str, str]) -> RunContext:
    """Pre-session variable assignment ($$MAP_X = $$WF_Y)."""
    params = dict(ctx.params)
    for dst, src in pairs.items():
        value = params.get(src, PARAM_DEFAULTS.get(src))
        if value is not None:
            params[dst] = value
    return dataclasses.replace(ctx, params=params)


def _agg_var(df: DataFrame, col: str, how: str):
    fn = {"MAX": F.max, "MIN": F.min, "COUNT": F.count}.get(how, lambda c: F.last(c, True))
    row = df.agg(fn(col).alias("v")).first()
    return None if row is None or row["v"] is None else str(row["v"])
'''


def ident(name: str) -> str:
    s = re.sub(r"\W", "_", name)
    return s if s and not s[0].isdigit() else "_" + s


def comment(text: str, prefix: str = "# ") -> list[str]:
    return [prefix + line.rstrip() for line in (text or "").splitlines() or [""]]


def pc_text(text: str) -> str:
    """PowerCenter text with $$VAR / $Param_X / $PMx references -> ${KEY} placeholders."""
    return re.sub(r"\$\$(\w+)|\$Param_(\w+)|\$(PM\w+)", lambda m: "${" + next(g for g in m.groups() if g) + "}", text)


def port_oracle_sql(sql: str, owners: set[str]) -> tuple[str, list[str]]:
    notes = []
    out = pc_text(sql)
    for owner in sorted(owners, key=len, reverse=True):
        out = re.sub(rf"\b{re.escape(owner)}\.", "", out, flags=re.I)
    out = re.sub(r"\bTRUNC\s*\(\s*SYSDATE\s*\)", "current_date()", out, flags=re.I)
    out = re.sub(r"\bSYSDATE\b", "current_timestamp()", out, flags=re.I)
    for pat, why in [(r"\(\+\)", "Oracle (+) outer-join syntax"), (r"\bROWNUM\b", "ROWNUM"),
                     (r"\bCONNECT\s+BY\b", "CONNECT BY"), (r"\bTO_DATE\s*\(", "Oracle TO_DATE format masks"),
                     (r"\bTO_CHAR\s*\(", "Oracle TO_CHAR format masks"), (r"\bNVL2\s*\(", "NVL2"),
                     (r"\bMINUS\b", "MINUS (use EXCEPT)"), (r"\bDUAL\b", "DUAL")]:
        if re.search(pat, out, flags=re.I):
            notes.append(why)
    return out.strip().rstrip(";"), notes


@dataclass
class Out:
    var: str
    kind: str = "df"  # df | seq | router | none
    has_op: bool = False
    groups: dict = field(default_factory=dict)  # router: port -> df var
    columns: list = field(default_factory=list)


class MappingGen:
    def __init__(self, folder: Folder, mapping: Mapping, session: Session | None, module: "ModuleGen"):
        self.folder, self.m, self.session, self.mod = folder, mapping, session, module
        self.lines: list[str] = []
        self.out: dict[str, Out] = {}
        self.lookup_n = 0
        self.setvars: list[tuple[str, str, str]] = []

    # ------------------------------------------------------------------ utils
    def emit(self, line: str = "", indent: int = 1) -> None:
        self.lines.append("    " * indent + line if line else "")

    def todo(self, obj: str, reason: str, original: str = "") -> None:
        self.mod.issue(f"{self.m.name}/{obj}", reason)
        self.emit(f"# TODO(pc2dbx): {obj}: {reason}")
        if original:
            for line in comment(original, "#   PowerCenter: "):
                self.emit(line)

    def tr(self, inst: Instance) -> Transformation | None:
        return self.m.transformation_for(inst)

    def incoming(self, name: str):
        return [c for c in self.m.connectors if c.to_instance == name]

    def order(self) -> list[Instance]:
        names = list(self.m.instances)
        preds = {n: set() for n in names}
        for c in self.m.connectors:
            if c.to_instance in preds and c.from_instance in preds and c.from_instance != c.to_instance:
                preds[c.to_instance].add(c.from_instance)
        for inst in self.m.instances.values():
            for src in inst.associated_sources:
                if src in preds:
                    preds[inst.name].add(src)
        done: list[str] = []
        while len(done) < len(names):
            ready = [n for n in names if n not in done and preds[n] <= set(done)]
            if not ready:
                ready = [next(n for n in names if n not in done)]
            done.append(ready[0])
        return [self.m.instances[n] for n in done]

    def types(self, t: Transformation | None) -> dict[str, str]:
        out = {v.name.lstrip("$"): v.datatype for v in self.m.variables}
        if t:
            out.update({p.name: p.datatype for p in t.ports})
        return out

    def expr_code(self, sql: str) -> str:
        return f"_expr(ctx, {sql!r})"

    # ------------------------------------------------------------- input view
    def view(self, inst: Instance, only: set[str] | None = None, rid: bool = True, var: str = "df") -> Out:
        conns = [c for c in self.incoming(inst.name) if only is None or c.to_field in only]
        by_up: "OrderedDict[str, list]" = OrderedDict()
        seq = []
        for c in conns:
            up = self.out.get(c.from_instance)
            if up is None:
                continue
            if up.kind == "seq":
                seq.append(c)
                continue
            key = up.groups.get(c.from_field, up.var) if up.kind == "router" else up.var
            by_up.setdefault(key, []).append((c, up))
        has_op = False
        first = True
        if not by_up:
            self.emit(f"{var} = _with_rid(ctx.spark.range(1).drop('id'))  # no row-producing upstream")
        for upvar, items in by_up.items():
            cols = ["'_rid'"] if rid or len(by_up) > 1 else []
            cols += [f"F.col({c.from_field!r}).alias({c.to_field!r})" if c.from_field != c.to_field
                     else repr(c.to_field) for c, _ in items]
            if any(u.has_op for _, u in items):
                cols.append("'_row_op'")
                has_op = True
            sel = f"{upvar}.select({', '.join(cols)})"
            if first:
                self.emit(f"{var} = {sel}")
                first = False
            else:
                self.emit(f"{var} = {var}.join({sel}, '_rid', 'inner')  # re-align passive branch")
        for c in seq:
            s = self.out[c.from_instance]
            start, inc = s.columns
            nextval = f"(F.lit({start}) + (F.row_number().over(Window.orderBy('_rid')) - 1) * {inc})"
            if c.from_field.upper() == "CURRVAL":
                nextval = f"({nextval} + {inc})"
            self.emit(f"{var} = {var}.withColumn({c.to_field!r}, {nextval})  # {c.from_instance}.{c.from_field}")
        return Out(var=var, has_op=has_op)

    # ------------------------------------------------------------- generation
    def generate(self, fn_name: str, mapplet_input: bool = False) -> list[str]:
        for inst in self.order():
            t = self.tr(inst)
            kind = inst.transformation_type
            self.emit()
            label = f"{kind}: {inst.name}" + (f" ({inst.transformation_name})" if inst.transformation_name != inst.name else "")
            self.emit(f"# --- {label}")
            if t is not None and t.description:
                for line in comment(t.description, "#     "):
                    self.emit(line)
            handler = {
                "Source Definition": self.g_source, "Source Qualifier": self.g_sq, "Expression": self.g_expression,
                "Filter": self.g_filter, "Router": self.g_router, "Aggregator": self.g_aggregator,
                "Joiner": self.g_joiner, "Lookup Procedure": self.g_lookup, "Update Strategy": self.g_update,
                "Sorter": self.g_sorter, "Sequence": self.g_sequence, "Normalizer": self.g_normalizer,
                "Target Definition": self.g_target, "Mapplet": self.g_mapplet,
                "Input Transformation": self.g_input, "Output Transformation": self.g_output,
            }.get(kind)
            if handler is None:
                self.todo(inst.name, f"transformation type '{kind}' is not supported")
                self.out[inst.name] = self.view(inst, var=f"df_{ident(inst.name)}")
                continue
            handler(inst, t)
        return self.lines

    def g_input(self, inst, t):
        self.out[inst.name] = Out(var="df_in")

    def g_output(self, inst, t):
        o = self.view(inst, var=f"df_{ident(inst.name)}")
        self.emit(f"return {o.var}")

    def g_source(self, inst, t):
        src = self.folder.sources.get(inst.transformation_name)
        var = f"df_{ident(inst.name)}"
        if src is None:
            self.todo(inst.name, "source definition not found in export")
            self.out[inst.name] = Out(var=var, kind="none")
            return
        if src.dbtype not in FILE_TYPES:
            self.emit(f"{var} = _with_rid(ctx.table({src.name!r}, {src.owner or None!r}))  # {src.dbtype} {src.dbdname}")
            self.out[inst.name] = Out(var=var)
            return
        ext = None
        if self.session:
            for e in self.session.extensions:
                if e.type == "READER" and "Source filename" in e.attrs and e.instance in (
                    [inst.name] + [i.name for i in self.m.instances.values() if inst.name in i.associated_sources]
                ):
                    ext = e
        path = self.mod.file_path(ext.attrs.get("Source file directory", "") if ext else "",
                                  ext.attrs.get("Source filename", "") if ext else src.name, "in")
        if src.flatfile.get("DELIMITED") == "YES":
            delim = (src.flatfile.get("DELIMITERS") or ",")[0]
            skip = int(src.flatfile.get("SKIPROWS") or 0)
            names = [f.name for f in src.fields]
            self.emit(f"{var} = ctx.spark.read.csv({path}, sep={delim!r}, header={skip > 0}).toDF(*{names!r})")
            if skip > 1:
                self.todo(inst.name, f"flat file skips {skip} header rows; only one is skipped")
        else:
            fields = self.mod.layout(src, self)
            self.emit(f"{var} = read_fixed_width(ctx.spark, {path}, {fields})  # {src.dbtype}")
            if src.dbtype != "Flat File":
                self.mod.issue(f"{self.m.name}/{inst.name}",
                               f"{src.dbtype} source read as fixed-width text; EBCDIC/packed-decimal fields need a "
                               "decoding step before landing")
        self.emit(f"{var} = _with_rid({var})")
        self.out[inst.name] = Out(var=var)

    def g_sq(self, inst, t):
        var = f"df_{ident(inst.name)}"
        a = t.attrs if t else {}
        if self.session and inst.name in self.session.transformation_overrides:
            a = {**a, **{k: v for k, v in self.session.transformation_overrides[inst.name].items() if v}}
        srcs = [self.folder.sources.get(self.m.instances[s].transformation_name) for s in inst.associated_sources
                if s in self.m.instances]
        srcs = [s for s in srcs if s is not None]
        relational = srcs and all(s.dbtype not in FILE_TYPES for s in srcs)
        connected = [p.name for p in t.ports if any(c.from_instance == inst.name and c.from_field == p.name
                                                    for c in self.m.connectors)] if t else []
        for key in ("Pre SQL", "Post SQL"):
            if a.get(key, "").strip():
                self.todo(inst.name, f"SQ {key} must run as a Databricks SQL step", a[key])
        query = a.get("Sql Query", "").strip()
        owners = {s.owner for s in self.folder.sources.values() if s.owner}
        if relational and (query or len(srcs) > 1):
            if not query:
                cols = []
                for c in self.incoming(inst.name):
                    src_inst = self.m.instances.get(c.from_instance)
                    if src_inst:
                        cols.append(f"{src_inst.transformation_name.lower()}.{c.from_field} AS {c.to_field}")
                where = [x for x in (a.get("User Defined Join", ""), a.get("Source Filter", "")) if x.strip()]
                query = ("SELECT " + ("DISTINCT " if a.get("Select Distinct") == "YES" else "") + ", ".join(cols)
                         + " FROM " + ", ".join(s.name.lower() for s in srcs)
                         + (" WHERE " + " AND ".join(f"({w})" for w in where) if where else ""))
                if not where:
                    self.todo(inst.name, "multi-source Source Qualifier without join condition (cross join)")
                connected = []
            sql, notes = port_oracle_sql(query, owners)
            tables = {s.name: s.owner or None for s in srcs}
            for name in re.findall(r"\b(?:FROM|JOIN)\s+([A-Za-z_][\w$#]*)", sql, flags=re.I):
                if name.lower() not in {k.lower() for k in tables}:
                    tables[name.upper()] = self.folder.source_owner(name) or None
            self.mod.issue(f"{self.m.name}/{inst.name}", "Oracle SQL override ported verbatim; review dialect"
                           + (f" ({', '.join(notes)})" if notes else ""))
            self.emit(f"_views(ctx, {tables!r})")
            for line in comment(query, "# PowerCenter SQL: "):
                self.emit(line)
            self.emit(f"{var} = ctx.spark.sql(_sql(ctx, {sql!r}))")
            if connected:
                self.emit(f"{var} = {var}.toDF(*{connected!r})  # SQL columns map positionally to connected ports")
            self.emit(f"{var} = _with_rid({var})")
        else:
            self.view(inst, var=var)
            if a.get("Source Filter", "").strip() and relational:
                sql, _ = port_oracle_sql(a["Source Filter"], owners)
                sql = re.sub(r"\b\w+\.(\w+)\b", r"\1", sql)
                ren = {c.from_field: c.to_field for c in self.incoming(inst.name)}
                sql = re.sub(r"\b(\w+)\b", lambda m: ren.get(m.group(1), m.group(1)), sql)
                for line in comment(a["Source Filter"], "# Source Filter: "):
                    self.emit(line)
                self.emit(f"{var} = {var}.filter(F.expr(_sql(ctx, {sql!r})))")
            if a.get("Select Distinct") == "YES":
                self.emit(f"{var} = _with_rid({var}.drop('_rid').distinct())")
        n_sorted = int(a.get("Number Of Sorted Ports") or 0)
        if n_sorted and t:
            keys = [p.name for p in t.ports][:n_sorted]
            self.emit(f"{var} = {var}.orderBy(*{keys!r})")
        self.out[inst.name] = Out(var=var)

    def g_expression(self, inst, t):
        var = f"df_{ident(inst.name)}"
        o = self.view(inst, var=var)
        tr = Translator(self.types(t))
        connected_in = {c.to_field for c in self.incoming(inst.name)}
        for p in t.ports:
            if p.is_input and p.name not in connected_in:
                self.emit(f"{var} = {var}.withColumn({p.name!r}, F.lit(None).cast({self.mod.stype(p)!r}))")
        variables = [p for p in t.ports if p.is_variable]
        outputs = [p for p in t.ports if p.is_output and not p.is_input]
        var_names = [p.name.upper() for p in variables]
        for p in variables + outputs:
            self.port_column(inst, var, p, tr, var_names)
        keep = ["_rid"] + (["_row_op"] if o.has_op else []) + [p.name for p in t.ports if p.is_output]
        if self.setvars and any(sv[0] == var for sv in self.setvars):
            self.emit(f"{var}_vars = {var}")
            self.setvars = [(f"{var}_vars" if v == var else v, c, a) for v, c, a in self.setvars]
        self.emit(f"{var} = {var}.select(*{keep!r})")
        self.out[inst.name] = Out(var=var, has_op=o.has_op)

    def port_column(self, inst, var, p: Port, tr: Translator, var_names: list[str]) -> None:
        stype = self.mod.stype(p)
        if not p.expression.strip() or p.expression.strip() == p.name:
            if p.is_variable or not p.is_input:
                self.emit(f"{var} = {var}.withColumn({p.name!r}, F.lit(None).cast({stype!r}))")
            return
        tr.lookup_offset = self.lookup_n
        res = tr.translate(p.expression)
        for line in comment(p.expression, f"# {p.name} = "):
            self.emit(line)
        if not res.ok:
            self.todo(f"{inst.name}.{p.name}", f"untranslatable expression ({res.reason})", p.expression)
            self.emit(f"{var} = {var}.withColumn({p.name!r}, F.lit(None).cast({stype!r}))")
            return
        for note in res.notes:
            self.mod.issue(f"{self.m.name}/{inst.name}.{p.name}", note)
        if p.is_variable:
            me = var_names.index(p.name.upper())
            refs = {r.upper() for r in res.refs}
            if p.name.upper() in refs or refs & set(var_names[me + 1:]):
                self.todo(f"{inst.name}.{p.name}", "variable port reads its previous-row value; needs a Window/"
                          "stateful rewrite (initialised per row here)")
                init = "''" if stype == "string" else "0"
                self.emit(f"{var} = {var}.withColumn({p.name!r}, F.expr({init!r}).cast({stype!r}))")
                for v in var_names[me + 1:]:
                    if v in refs:
                        self.emit(f"{var} = {var}.withColumn({v!r}, F.expr({init!r}))")
        for lk in res.lookups:
            self.unconnected_lookup(var, lk)
        self.lookup_n += len(res.lookups)
        self.emit(f"{var} = {var}.withColumn({p.name!r}, {self.expr_code(res.sql)}.cast({stype!r}))")
        for i, sv in enumerate(res.setvars):
            col = f"_set_{sv['variable']}_{len(self.setvars)}"
            self.emit(f"{var} = {var}.withColumn({col!r}, {self.expr_code(sv['sql'])})  # SETVARIABLE")
            agg = sv["aggregate"] or next((v.aggfunction for v in self.m.variables
                                           if param_key(v.name) == sv["variable"]), "")
            self.setvars.append((var, col, agg))
            self.mod.setvar_names.setdefault(self.m.name, {})[col] = sv["variable"]

    def unconnected_lookup(self, var: str, lk: dict) -> None:
        name, col = lk["lookup"], lk["column"]
        t = self.m.transformations.get(name) or next(
            (x for x in self.m.transformations.values() if x.name.upper() == name.upper()), None)
        if t is None:
            self.todo(col, f"unconnected lookup {name} not found")
            self.emit(f"{var} = {var}.withColumn({col!r}, F.lit(None))")
            return
        inputs = [p.name for p in t.ports if p.porttype == "INPUT"]
        lports = [p.name for p in t.ports if p.has("LOOKUP")]
        ret = next((p.name for p in t.ports if p.has("RETURN")), lports[0] if lports else None)
        if len(inputs) != len(lk["args"]) or ret is None:
            self.todo(col, f"unconnected lookup {name}: argument/port mismatch")
            self.emit(f"{var} = {var}.withColumn({col!r}, F.lit(None))")
            return
        self.emit(f"# :LKP.{name}({', '.join(lk['args'])}) -> {col}")
        rename = {p: f"{col}__{p}" for p in inputs}
        rename.update({p: f"{col}_l__{p}" for p in lports})
        lkp = self.lookup_df(t, f"{col}_df")
        self.emit(f"{col}_df = {col}_df.select(*[F.col(c).alias({col!r} + '_l__' + c) for c in {lports!r}])")
        for p, arg in zip(inputs, lk["args"]):
            self.emit(f"{var} = {var}.withColumn({rename[p]!r}, {self.expr_code(arg)})")
        cond = Translator({p.name: p.datatype for p in t.ports}).condition(t.attrs.get("Lookup condition", ""))
        if not cond.ok:
            self.todo(col, f"lookup condition untranslatable ({cond.reason})", t.attrs.get("Lookup condition", ""))
            self.emit(f"{var} = {var}.withColumn({col!r}, F.lit(None))")
            return
        sql = re.sub(r"\b(\w+)\b", lambda m: rename.get(m.group(1), m.group(1)), cond.sql)
        self.emit(f"{var} = {var}.join({lkp}, F.expr({sql!r}), 'left')")
        order = [rename[p] for p in lports if p in cond.refs] or [rename[ret]]
        self.emit(f"{var} = _first_match({var}, {order!r})  # {t.attrs.get('Lookup policy on multiple match', '')}")
        drop = [rename[p] for p in inputs + lports if rename[p] != rename[ret]]
        self.emit(f"{var} = {var}.withColumnRenamed({rename[ret]!r}, {col!r}).drop(*{drop!r})")

    def lookup_df(self, t: Transformation, var: str) -> str:
        a = t.attrs
        table = a.get("Lookup table name", "") or t.name
        owners = {s.owner for s in self.folder.sources.values() if s.owner}
        lports = [p.name for p in t.ports if p.has("LOOKUP")]
        override = a.get("Lookup Sql Override", "").strip()
        if a.get("Source Type", "Database") != "Database":
            self.todo(t.name, f"lookup source type {a.get('Source Type')} not supported")
        if override:
            sql, notes = port_oracle_sql(override, owners)
            sql = re.sub(r"--.*$", "", sql, flags=re.M).strip()
            tables = {}
            for name in re.findall(r"\b(?:FROM|JOIN)\s+([A-Za-z_][\w$#]*)", sql, flags=re.I):
                tables[name.upper()] = self.folder.source_owner(name) or None
            self.mod.issue(f"{self.m.name}/{t.name}", "Lookup SQL override ported verbatim; review dialect"
                           + (f" ({', '.join(notes)})" if notes else ""))
            for line in comment(override, "# Lookup SQL override: "):
                self.emit(line)
            self.emit(f"_views(ctx, {tables!r})")
            self.emit(f"{var} = _ensure(ctx.spark.sql(_sql(ctx, {sql!r})), {lports!r})")
        else:
            owner = self.folder.source_owner(table) or None
            self.emit(f"{var} = ctx.table({table!r}, {owner!r}).select(*{lports!r})")
        if a.get("Lookup Source Filter", "").strip():
            sql, _ = port_oracle_sql(a["Lookup Source Filter"], owners)
            self.emit(f"{var} = {var}.filter(F.expr(_sql(ctx, {sql!r})))  # Lookup Source Filter")
        return var

    def g_lookup(self, inst, t):
        var = f"df_{ident(inst.name)}"
        if not self.incoming(inst.name):
            self.emit(f"# unconnected lookup; joined where :LKP.{inst.transformation_name}(...) is called")
            self.out[inst.name] = Out(var=var, kind="none")
            return
        o = self.view(inst, var=var)
        lkp = self.lookup_df(t, f"{var}_src")
        cond_text = t.attrs.get("Lookup condition", "")
        cond = Translator({p.name: p.datatype for p in t.ports}).condition(cond_text)
        for line in comment(cond_text, "# Lookup condition: "):
            self.emit(line)
        if not cond.ok:
            self.todo(inst.name, f"lookup condition untranslatable ({cond.reason})", cond_text)
        else:
            policy = t.attrs.get("Lookup policy on multiple match", "Use Any Value")
            lports = [p.name for p in t.ports if p.has("LOOKUP")]
            order = [p for p in lports if p in cond.refs] or lports[:1]
            self.emit(f"{var} = {var}.join({lkp}, F.expr({cond.sql!r}), 'left')")
            if policy == "Use Last Value":
                self.emit(f"{var} = _first_match({var}.select('*', *[F.col(c).alias('_o_' + c) for c in {order!r}]),"
                          f" {[f'_o_{c}' for c in order]!r})  # TODO(pc2dbx): Use Last Value ordering")
            elif policy == "Report Error":
                self.todo(inst.name, "lookup policy 'Report Error' is approximated by the first match")
                self.emit(f"{var} = _first_match({var}, {order!r})")
            else:
                self.emit(f"{var} = _first_match({var}, {order!r})  # {policy}")
        keep = ["_rid"] + (["_row_op"] if o.has_op else []) + [p.name for p in t.ports if p.is_output]
        self.emit(f"{var} = {var}.select(*{keep!r})")
        self.out[inst.name] = Out(var=var, has_op=o.has_op)

    def g_filter(self, inst, t):
        var = f"df_{ident(inst.name)}"
        o = self.view(inst, var=var)
        text = t.attrs.get("Filter Condition", "TRUE")
        res = Translator(self.types(t)).condition(text)
        for line in comment(text, "# Filter Condition: "):
            self.emit(line)
        if res.ok:
            self.emit(f"{var} = {var}.filter({self.expr_code(res.sql)})")
        else:
            self.todo(inst.name, f"filter condition untranslatable ({res.reason})", text)
        self.out[inst.name] = Out(var=var, has_op=o.has_op)

    def g_router(self, inst, t):
        var = f"df_{ident(inst.name)}"
        o = self.view(inst, var=var)
        tr = Translator(self.types(t))
        groups = sorted([g for g in t.groups if g.type.startswith("OUTPUT")], key=lambda g: g.order)
        conds = []
        out = Out(var=var, kind="router", has_op=o.has_op)
        for g in groups:
            gvar = f"{var}__{ident(g.name)}"
            ports = [p for p in t.ports if p.group == g.name]
            sel = ["'_rid'"] + (["'_row_op'"] if o.has_op else []) + [
                f"F.col({(p.ref_field or p.name)!r}).alias({p.name!r})" for p in ports]
            if g.type == "OUTPUT/DEFAULT":
                cond = " AND ".join(f"NOT coalesce({c}, false)" for c in conds) or "true"
                self.emit(f"# group {g.name} (DEFAULT): rows matching no other group")
            else:
                res = tr.condition(g.expression)
                for line in comment(g.expression, f"# group {g.name}: "):
                    self.emit(line)
                if not res.ok:
                    self.todo(f"{inst.name}.{g.name}", f"router condition untranslatable ({res.reason})", g.expression)
                    cond = "false"
                else:
                    cond = res.sql
                conds.append(cond)
            self.emit(f"{gvar} = {var}.filter({self.expr_code(cond)}).select({', '.join(sel)})")
            for p in ports:
                out.groups[p.name] = gvar
        self.out[inst.name] = out

    def g_aggregator(self, inst, t):
        var = f"df_{ident(inst.name)}"
        self.view(inst, var=var)
        tr = Translator(self.types(t))
        keys = [p.name for p in t.ports if p.expression_type == "GROUPBY"]
        for p in t.ports:
            if p.is_variable:
                self.port_column(inst, var, p, tr, [p.name.upper() for p in t.ports if p.is_variable])
        aggs = []
        for p in t.ports:
            if not p.is_output or p.name in keys:
                continue
            stype = self.mod.stype(p)
            if p.is_input and (not p.expression or p.expression == p.name):
                aggs.append(f"F.last({p.name!r}, True).alias({p.name!r})")
                continue
            res = tr.translate(p.expression)
            for line in comment(p.expression, f"# {p.name} = "):
                self.emit(line)
            if not res.ok:
                self.todo(f"{inst.name}.{p.name}", f"untranslatable aggregate ({res.reason})", p.expression)
                aggs.append(f"F.lit(None).cast({stype!r}).alias({p.name!r})")
            elif res.aggregate:
                aggs.append(f"{self.expr_code(res.sql)}.cast({stype!r}).alias({p.name!r})")
            else:
                aggs.append(f"F.last({self.expr_code(res.sql)}, True).cast({stype!r}).alias({p.name!r})")
        group = f".groupBy(*{keys!r})" if keys else ""
        self.emit(f"{var} = {var}{group}.agg(")
        for a in aggs or ["F.count(F.lit(1)).alias('_count')"]:
            self.emit(f"    {a},")
        self.emit(")")
        self.emit(f"{var} = _with_rid({var})")
        self.out[inst.name] = Out(var=var)

    def g_joiner(self, inst, t):
        var = f"df_{ident(inst.name)}"
        master = {p.name for p in t.ports if "MASTER" in p.porttype}
        detail = {p.name for p in t.ports if p.is_input and p.name not in master}
        m = self.view(inst, only=master, var=f"{var}_master")
        d = self.view(inst, only=detail, var=f"{var}_detail")
        cond_text = t.attrs.get("Join Condition", "")
        jt = t.attrs.get("Join Type", "Normal Join")
        how = {"Normal Join": "inner", "Master Outer Join": "left", "Detail Outer Join": "right",
               "Full Outer Join": "full"}.get(jt, "inner")
        res = Translator(self.types(t)).condition(cond_text)
        self.emit(f"# {jt}: detail pipeline on the left, master on the right")
        for line in comment(cond_text, "# Join Condition: "):
            self.emit(line)
        if not res.ok:
            self.todo(inst.name, f"join condition untranslatable ({res.reason})", cond_text)
            self.emit(f"{var} = {d.var}.drop('_rid').crossJoin({m.var}.drop('_rid'))")
        else:
            self.emit(f"{var} = {d.var}.drop('_rid').join({m.var}.drop('_rid'), {self.expr_code(res.sql)}, {how!r})")
        if t.attrs.get("Sorted Input") == "YES":
            self.emit("# Sorted Input = YES (no effect in Spark)")
        self.emit(f"{var} = _with_rid({var})")
        self.out[inst.name] = Out(var=var)

    def g_update(self, inst, t):
        var = f"df_{ident(inst.name)}"
        self.view(inst, var=var)
        text = t.attrs.get("Update Strategy Expression", "DD_INSERT")
        res = Translator(self.types(t), dd_tokens=True).translate(text)
        for line in comment(text, "# Update Strategy Expression: "):
            self.emit(line)
        if not res.ok:
            self.todo(inst.name, f"update strategy untranslatable ({res.reason}); defaulting to DD_INSERT", text)
            res.sql = "DD_INSERT"
        self.emit(f"{var} = delta.update_strategy({var}, _sql(ctx, {res.sql!r}, literal=True))")
        self.out[inst.name] = Out(var=var, has_op=True)

    def g_sorter(self, inst, t):
        var = f"df_{ident(inst.name)}"
        o = self.view(inst, var=var)
        keys = [f"F.col({p.name!r}).{'desc' if p.sort_direction == 'DESCENDING' else 'asc'}()"
                for p in t.ports if p.is_sortkey]
        if t.attrs.get("Distinct") == "YES":
            cols = [p.name for p in t.ports if p.is_output]
            self.emit(f"{var} = {var}.dropDuplicates({cols!r})  # Distinct = YES")
        if keys:
            self.emit(f"{var} = {var}.orderBy({', '.join(keys)})  # order only guaranteed for this step")
        self.out[inst.name] = Out(var=var, has_op=o.has_op)

    def g_sequence(self, inst, t):
        a = t.attrs
        inc = int(a.get("Increment By") or 1)
        start = int(a.get("Current Value") or a.get("Start Value") or 1)
        key = f"SEQ_{ident(inst.name).upper()}_CURRENT"
        self.mod.defaults[key] = str(start)
        self.emit(f"# Sequence: start/current={start}, increment={inc}, reset={a.get('Reset', 'NO')}, "
                  f"cycle={a.get('Cycle', 'NO')}")
        self.emit(f"seq_{ident(inst.name)} = int(ctx.param({key!r}, PARAM_DEFAULTS[{key!r}]))")
        self.mod.issue(f"{self.m.name}/{inst.name}", "Sequence current value must be persisted between runs "
                       f"(job parameter {key}); numbering uses row_number() over _rid")
        self.out[inst.name] = Out(var="", kind="seq", columns=[f"seq_{ident(inst.name)}", inc])

    def g_normalizer(self, inst, t):
        var = f"df_{ident(inst.name)}"
        self.view(inst, var=var)
        occ: "OrderedDict[str, list[str]]" = OrderedDict()
        for p in t.ports:
            if p.is_input:
                occ.setdefault(p.ref_source_field or p.name, []).append(p.name)
        outs = {p.ref_source_field or p.name: p for p in t.ports if p.is_output and not p.porttype.startswith("GENERATED")}
        n = max((len(v) for v in occ.values()), default=1)
        if n > 1:
            structs = []
            for i in range(n):
                fields = [f"F.col({cols[min(i, len(cols) - 1)]!r}).alias({outs[f].name!r})"
                          for f, cols in occ.items() if f in outs]
                structs.append(f"F.struct({', '.join(fields)})")
            self.emit(f"{var} = {var}.select('_rid', F.posexplode(F.array(")
            for s in structs:
                self.emit(f"    {s},")
            self.emit(")).alias('_occ', '_r')).select('_rid', '_occ', '_r.*')")
        else:
            sel = ["'_rid'", "F.lit(0).alias('_occ')"] + [
                f"F.col({cols[0]!r}).alias({outs[f].name!r})" if cols[0] != outs[f].name else repr(cols[0])
                for f, cols in occ.items() if f in outs]
            self.emit(f"{var} = {var}.select({', '.join(sel)})")
        for p in t.ports:
            if p.porttype.startswith("GENERATED COLUMN ID"):
                self.emit(f"{var} = {var}.withColumn({p.name!r}, F.col('_occ') + 1)")
            elif p.porttype.startswith("GENERATED KEY"):
                self.emit(f"{var} = {var}.withColumn({p.name!r}, F.dense_rank().over(Window.orderBy('_rid')))")
        self.emit(f"{var} = _with_rid({var}.drop('_rid', '_occ'))")
        self.out[inst.name] = Out(var=var)

    def g_mapplet(self, inst, t):
        var = f"df_{ident(inst.name)}"
        mp = self.folder.mapplets.get(inst.transformation_name)
        self.view(inst, var=var)
        if mp is None:
            self.todo(inst.name, f"mapplet {inst.transformation_name} not found")
        else:
            fn = self.mod.mapplet_fn(self.folder, mp)
            self.emit(f"{var} = {fn}(ctx, {var})")
        self.out[inst.name] = Out(var=var)

    def g_target(self, inst, t):
        var = f"df_{ident(inst.name)}"
        tgt = self.folder.targets.get(inst.transformation_name)
        o = self.view(inst, rid=False, var=var)
        if tgt is None:
            self.todo(inst.name, "target definition not found")
            return
        ext = self.session.extension(inst.name, "WRITER") if self.session else None
        wa = ext.attrs if ext else {}
        for key in ("Pre SQL", "Post SQL"):
            if wa.get(key, "").strip():
                self.todo(inst.name, f"target {key} must run as a Databricks SQL step", wa[key])
        if tgt.dbtype in FILE_TYPES:
            path = self.mod.file_path(wa.get("Output file directory", ""), wa.get("Output filename", "") or tgt.name, "out")
            if tgt.flatfile.get("DELIMITED") == "YES":
                delim = (tgt.flatfile.get("DELIMITERS") or ",")[0]
                header = "Output Field Names" in wa.get("Header Options", "")
                self.emit(f"{var}.coalesce(1).write.mode('overwrite').csv({path}, sep={delim!r}, header={header})")
                self.mod.issue(f"{self.m.name}/{inst.name}", "delimited flat-file target is written as a Spark CSV "
                               "directory (single part file); rename/copy to the final file name in ops")
            else:
                fields = self.mod.layout(tgt, self)
                self.emit(f"write_fixed_width({var}, {path}, {fields})  # {tgt.dbtype}")
                if tgt.dbtype != "Flat File":
                    self.mod.issue(f"{self.m.name}/{inst.name}", f"{tgt.dbtype} target written as fixed-width text "
                                   "(no EBCDIC/packed-decimal encoding)")
            return
        owner = wa.get("Table Name Prefix", "").rstrip(".") or self.folder.source_owner(tgt.name) or None
        table = f"ctx.table_name({tgt.name!r}, {owner!r})"
        treat = self.session.attrs.get("Treat source rows as", "Insert") if self.session else "Insert"
        keys = tgt.keys
        if o.has_op or treat in ("Update", "Delete"):
            if not keys:
                self.todo(inst.name, "Update Strategy target has no primary key; MERGE keys must be supplied")
                keys = ["TODO_KEY"]
            op = "'_row_op'" if o.has_op else "None"
            self.emit(f"delta.merge(ctx, {var}, {table}, {keys!r}, op_col={op})  # Treat source rows as: {treat}")
        elif wa.get("Truncate target table option") == "YES":
            self.emit(f"delta.overwrite(ctx, {var}, {table})  # Truncate target table option = YES")
        else:
            self.emit(f"delta.append(ctx, {var}, {table})")


class ModuleGen:
    def __init__(self, folder: Folder, workflow: Workflow, source_path: str = ""):
        self.folder, self.wf, self.source_path = folder, workflow, source_path
        self.issues: list[dict] = []
        self.defaults: dict[str, str] = {}
        self.numeric_params: set[str] = set()
        self.layouts: "OrderedDict[str, list[str]]" = OrderedDict()
        self.mapplets: "OrderedDict[str, list[str]]" = OrderedDict()
        self.setvar_names: dict[str, dict[str, str]] = {}

    def issue(self, obj: str, reason: str) -> None:
        item = {"workflow": self.wf.name, "object": obj, "reason": reason}
        if item not in self.issues:
            self.issues.append(item)

    @staticmethod
    def stype(p: Port) -> str:
        return spark_type(p.datatype, p.precision, p.scale)

    def file_path(self, directory: str, filename: str, default_dir: str) -> str:
        d = pc_text(directory or "").replace("\\", "/")
        d = re.sub(r"^\$\{Root_Directory\}/?", "", d)
        d = re.sub(r"^(data/)?int/", "", d).strip("/")
        if d.startswith("${") or not d:
            d = default_dir
        parts = [p for p in d.split("/") if p]
        args = ", ".join(f"_sql(ctx, {p!r})" if "${" in p else repr(p) for p in parts)
        return f"ctx.path({args}, _sql(ctx, {pc_text(filename)!r}))"

    def layout(self, rec, mg: MappingGen) -> str:
        const = f"FIELDS_{ident(rec.name).upper()}"
        if const in self.layouts:
            return const
        lines = []
        is_target = not hasattr(rec, "dbdname")
        fields = rec.fields
        if any(f.occurs > 1 or f.attrs.get("REDEFINES") for f in fields):
            self.issue(f"{rec.name}", "COBOL OCCURS/REDEFINES layout flattened; verify field offsets")
        cursor = 0
        for f in fields:
            if f.fieldtype == "GROUPITEM":
                continue
            length = (f.precision if is_target else (f.physical_length or f.length or f.precision)) or 1
            if not is_target and f.physical_offset > cursor:
                lines.append(f"Field('_filler_{cursor}', {f.physical_offset - cursor}),")
            numeric = spark_type(f.datatype, f.precision, f.scale) not in ("string", "timestamp")
            reps = max(f.occurs, 1)
            for i in range(reps):
                name = f.name if reps == 1 else f"{f.name}_{i + 1}"
                lines.append(f"Field({name!r}, {length}{', numeric=True' if numeric else ''}),")
            if not is_target:
                cursor = max(cursor, f.physical_offset) + length * reps
            if "COMP" in (f.usage or "").upper() or "COMP" in (f.picture or "").upper():
                self.issue(f"{rec.name}.{f.name}", "COMP/COMP-3 binary field cannot be read as text")
        self.layouts[const] = lines
        return const

    def mapplet_fn(self, folder: Folder, mp: Mapping) -> str:
        fn = f"_mplt_{ident(mp.name)}"
        if fn not in self.mapplets:
            self.mapplets[fn] = []
            g = MappingGen(folder, mp, None, self)
            body = g.generate(fn, mapplet_input=True)
            self.mapplets[fn] = body
        return fn

    def mapping_fn(self, sess: Session) -> list[str]:
        m = self.folder.mappings.get(sess.mapping_name)
        fn = ident(sess.name)
        out = [f"def {fn}(ctx: RunContext) -> dict[str, str]:"]
        doc = f"Session {sess.name} -> mapping {sess.mapping_name}."
        out.append(f"    {doc!r}")
        if sess.description:
            out += ["    " + line for line in comment(sess.description)]
        builtins = {"PMSessionName": sess.name, "PMMappingName": sess.mapping_name}
        out.append(f"    ctx = dataclasses.replace(ctx, params={{**{builtins!r}, **ctx.params}})  # $PM built-ins")
        if sess.pre_assignments:
            pairs = {param_key(a): param_key(b) for a, b in sess.pre_assignments}
            out.append(f"    ctx = _assign(ctx, {pairs!r})  # pre-session variable assignment")
        for cmd in sess.pre_commands:
            out += ["    " + line for line in comment(cmd, "# TODO(pc2dbx): pre-session command (ops task): ")]
            self.issue(sess.name, "pre-session shell command must become an ops task")
        if m is None:
            self.issue(sess.name, f"mapping {sess.mapping_name} not found")
            out.append("    return {}")
            return out
        for v in m.variables:
            key = param_key(v.name)
            if v.default:
                self.defaults.setdefault(key, v.default)
            if spark_type(v.datatype, v.precision, v.scale) not in ("string", "timestamp"):
                self.numeric_params.add(key)
        g = MappingGen(self.folder, m, sess, self)
        out += g.generate(fn)
        for cmd in sess.post_commands:
            out += ["    " + line for line in comment(cmd, "# TODO(pc2dbx): post-session command (ops task): ")]
            self.issue(sess.name, "post-session shell command must become an ops task")
        out.append("")
        out.append("    mapping_vars: dict[str, str] = {}")
        names = self.setvar_names.get(m.name, {})
        for var, col, agg in g.setvars:
            out.append(f"    mapping_vars[{names[col]!r}] = _agg_var({var}, {col!r}, {agg!r})")
        if sess.post_assignments:
            out.append("    out = {}")
            for dst, src in sess.post_assignments:
                out.append(f"    if {param_key(src)!r} in mapping_vars:")
                out.append(f"        out[{param_key(dst)!r}] = mapping_vars[{param_key(src)!r}]")
            out.append("    return out  # post-session variable assignment -> task values")
        else:
            out.append("    return mapping_vars")
        return out

    def render(self) -> str:
        wf = self.wf
        self.defaults.update({"PMWorkflowName": wf.name, "PMFolderName": self.folder.name,
                              "PMRepositoryServiceName": "databricks", "PMIntegrationServiceName": "databricks",
                              "PMWorkflowRunId": "0", "PMSessionRunId": "0"})
        self.numeric_params.update({"PMWorkflowRunId", "PMSessionRunId"})
        for v in wf.variables:
            key = param_key(v.name)
            if v.default:
                self.defaults.setdefault(key, v.default)
            if spark_type(v.datatype, v.precision, v.scale) not in ("string", "timestamp"):
                self.numeric_params.add(key)
        sessions = wf.ordered_sessions()
        bodies = [self.mapping_fn(s) for s in sessions]
        deps: dict[str, list[str]] = {}
        inst_to_task = {t.name: t.task_name for t in wf.task_instances}
        sess_names = {s.name for s in sessions}

        def upstream_sessions(task: str, seen=None) -> list[str]:
            seen = seen or set()
            out = []
            for frm, _ in wf.predecessors(task):
                if frm in seen:
                    continue
                seen.add(frm)
                if inst_to_task.get(frm, frm) in sess_names:
                    out.append(inst_to_task.get(frm, frm))
                else:
                    out += upstream_sessions(frm, seen)
            return out

        for ti in wf.task_instances:
            if ti.task_name in sess_names:
                deps[ti.task_name] = sorted(set(upstream_sessions(ti.name)))
        other = [f"{t.name} ({t.task_type})" for t in wf.task_instances
                 if t.task_name not in sess_names and t.task_type != "Start"]
        conds = sorted({l.condition for l in wf.links if l.condition and not l.condition.endswith("= Succeeded")})
        for c in conds:
            self.issue(wf.name, f"non-trivial workflow link condition: {c}")

        L = ['"""' + f"{wf.name} - generated by pc2dbx from {self.source_path or 'PowerCenter XML'}."]
        L.append("")
        L.append(f"Folder {self.folder.name}; schedule {wf.schedule.get('type') or 'see SCHEDULE'}.")
        if wf.description:
            L.append(wf.description.replace('"""', "'''").replace("\\", "/"))
        if other:
            L.append("Non-session tasks (handled by orchestration): " + ", ".join(other))
        L.append('"""')
        L += ["from __future__ import annotations", "", "import dataclasses", "import re", "from typing import Iterable", "",
              "from pyspark.sql import DataFrame, Window", "from pyspark.sql import functions as F", "",
              "from biis_lakehouse.common import delta",
              "from biis_lakehouse.common.fixed_width import Field, read_fixed_width, write_fixed_width",
              "from biis_lakehouse.context import RunContext", ""]
        L.append(f"WORKFLOW = {wf.name!r}")
        L.append(f"SCHEDULE = {wf.schedule!r}")
        L.append(f"DEPENDENCIES = {deps!r}")
        L.append(f"PARAM_DEFAULTS: dict[str, str] = {dict(sorted(self.defaults.items()))!r}")
        L.append(f"NUMERIC_PARAMS = {sorted(self.numeric_params)!r}")
        L.append("")
        for const, lines in self.layouts.items():
            L.append(f"{const} = [")
            L += ["    " + x for x in lines]
            L.append("]")
        L.append(HELPERS)
        for fn, body in self.mapplets.items():
            L += ["", f"def {fn}(ctx: RunContext, df_in: DataFrame) -> DataFrame:"] + body
        for body in bodies:
            L += ["", ""] + body
        L += ["", "", "TASKS = {"]
        L += [f"    {s.name!r}: {ident(s.name)}," for s in sessions]
        L += ["}", ""]
        return "\n".join(L)


def module_name(workflow: str) -> str:
    return workflow.lower()


def area_for(workflow: str, folder: Folder) -> str:
    return AREAS.get(workflow, ident(folder.name).lower())


def generate_workflow(folder: Folder, wf: Workflow, source_path: str = "") -> tuple[str, list[dict]]:
    gen = ModuleGen(folder, wf, source_path)
    code = gen.render()
    return code, gen.issues
