"""Executes a PowerCenter mapping (as exported in ``XML/CPM``, captured in ``_spec.json``) on Spark.

The mapping graph is walked in dependency order.  Every *active* transformation (Source Qualifier,
Normalizer, Filter, Router group, Aggregator, Joiner) starts a new DataFrame ("frame"); *passive*
transformations (Expression, Lookup, mapplet Input/Output) add their ports as columns to the frame of their
input, which keeps PowerCenter's row alignment between ports of the same pipeline.  Ports are stored as
``<instance>__<port>`` columns; ``_SEQ`` carries PowerCenter's row order (Source Qualifier ORDER BY / file
order) for order-sensitive logic (FIRST/LAST, non-aggregated Aggregator ports, stateful variable ports).

Session code supplies the Source Qualifier / VSAM Normalizer output DataFrames (``inputs``) and writes the
target DataFrames returned by :func:`run_mapping`.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Callable, Optional

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql import types as T

from biis_lakehouse.context import RunContext

from . import _pcexpr as P

SEQ = "_SEQ"


@lru_cache(maxsize=1)
def spec() -> dict:
    return json.loads((Path(__file__).with_name("_spec.json")).read_text())


def col(inst: str, port: str) -> str:
    return f"{inst.replace('::', '_m_')}__{port}"


def q(name: str) -> str:
    return f"`{name}`"


@dataclass
class MappingResult:
    targets: dict[str, DataFrame]
    setvars: dict[str, object] = field(default_factory=dict)


# --------------------------------------------------------------------------- graph
@dataclass
class _Graph:
    instances: dict  # name -> {"type", "tx": transformation dict}
    connectors: list  # (from_inst, from_port, to_inst, to_port)


def _graph(mapping_name: str) -> _Graph:
    s = spec()
    m = s["mappings"][mapping_name]
    insts: dict = {}
    conns: list = []
    io_ports: dict = {}
    for name, i in m["instances"].items():
        if i["type"] == "Mapplet":
            mp = s["mapplets"][i["tx"]]
            for iname, ii in mp["instances"].items():
                tx = mp["transformations"].get(ii["tx"])
                insts[f"{name}::{iname}"] = {"type": ii["type"], "tx": tx}
                if ii["type"] in ("Input Transformation", "Output Transformation"):
                    for f in tx["fields"]:
                        io_ports[(name, f["name"], ii["type"])] = f"{name}::{iname}"
            for a, ap, b, bp in mp["connectors"]:
                conns.append((f"{name}::{a}", ap, f"{name}::{b}", bp))
        else:
            insts[name] = {"type": i["type"], "tx": m["transformations"].get(i["tx"]), "kind": i["kind"]}
    for a, ap, b, bp in m["connectors"]:
        a = io_ports.get((a, ap, "Output Transformation"), a)
        b = io_ports.get((b, bp, "Input Transformation"), b)
        conns.append((a, ap, b, bp))
    return _Graph(insts, conns)


def _order(g: _Graph, given: set[str]) -> list[str]:
    deps = {n: set() for n in g.instances}
    for a, _, b, _ in g.connectors:
        deps[b].add(a)
    done, out = set(), []
    pending = [n for n in g.instances]
    while pending:
        progressed = False
        for n in list(pending):
            if n in given or g.instances[n]["type"] == "Source Definition" or deps[n] <= done:
                done.add(n)
                out.append(n)
                pending.remove(n)
                progressed = True
        if not progressed:
            raise RuntimeError(f"cycle in mapping graph: {pending}")
    return out


# --------------------------------------------------------------------------- helpers
def _ptype(f) -> str:
    return P.pc_type(f.get("datatype"))


def _cast_sql(sql: str, f) -> str:
    st = P.spark_type(f.get("datatype"), f.get("precision"), f.get("scale"))
    if st == "string" and f.get("precision"):
        return f"substring(CAST({sql} AS STRING), 1, {int(f['precision'])})"
    return f"CAST({sql} AS {st.upper()})"


def _is_stateful(fields) -> bool:
    vars_ = [f for f in fields if "VARIABLE" in f["porttype"]]
    names = [f["name"] for f in vars_]
    for i, f in enumerate(vars_):
        r = P.refs(P.parse(f.get("expression") or "NULL")) if f.get("expression") else set()
        if r & set(names[i:]):
            return True
    return False


class _Run:
    def __init__(self, ctx: RunContext, mapping: str, inputs: dict[str, DataFrame], params: dict,
                 lookups: dict[str, DataFrame], lookup_owner: Callable[[str], Optional[str]]):
        self.ctx, self.mapping = ctx, mapping
        self.g = _graph(mapping)
        self.inputs, self.params, self.lookups, self.lookup_owner = inputs, params, lookups, lookup_owner
        self.frames: dict[str, DataFrame] = {}
        self.where: dict[tuple[str, str], str] = {}  # (inst, port) -> frame id
        self.setvars: dict[str, object] = {}
        self.targets: dict[str, DataFrame] = {}
        self.incoming: dict[str, list] = {}
        for c in self.g.connectors:
            self.incoming.setdefault(c[2], []).append(c)

    # ports / frames ------------------------------------------------------
    def _frame_of_inputs(self, inst: str) -> Optional[str]:
        fids = {self.where[(a, ap)] for a, ap, _, _ in self.incoming.get(inst, []) if (a, ap) in self.where}
        if len(fids) > 1:
            raise RuntimeError(f"{inst}: inputs come from several pipelines {fids}")
        return next(iter(fids), None)

    def _bind_inputs(self, inst: str, fields, fid: str, df: DataFrame) -> DataFrame:
        """Add the instance's input ports as columns (connected -> upstream column, else NULL)."""
        src = {bp: (a, ap) for a, ap, _, bp in self.incoming.get(inst, [])}
        sel = []
        for f in fields:
            pt = f["porttype"]
            if "INPUT" not in pt and pt not in ("MASTER",):
                continue
            name = col(inst, f["name"])
            if f["name"] in src:
                a, ap = src[f["name"]]
                sel.append(F.col(q(col(a, ap))).alias(name))
            else:
                sel.append(F.lit(None).cast(P.spark_type(f.get("datatype"), f.get("precision"), f.get("scale"))).alias(name))
            self.where[(inst, f["name"])] = fid
        return df.select("*", *sel) if sel else df

    def _port_env(self, inst: str, fields) -> dict:
        return {f["name"]: (q(col(inst, f["name"])), _ptype(f)) for f in fields}

    def _prune(self, done: set[str]):
        need: dict[str, set[str]] = {}
        for a, ap, b, _ in self.g.connectors:
            if b not in done and (a, ap) in self.where:
                need.setdefault(self.where[(a, ap)], set()).add(col(a, ap))
        for fid, df in list(self.frames.items()):
            keep = [c for c in df.columns if c in need.get(fid, set()) or c == SEQ]
            if len(keep) < len(df.columns):
                self.frames[fid] = df.select(*[q(c) for c in keep])

    # transformations -----------------------------------------------------
    def given(self, inst: str):
        df = self.inputs[inst]
        if SEQ not in df.columns:
            df = df.withColumn(SEQ, F.monotonically_increasing_id())
        tx = self.g.instances[inst]["tx"]
        ports = [f["name"] for f in tx["fields"]] if tx else [c for c in df.columns if c != SEQ]
        lower = {c.lower(): c for c in df.columns}
        sel = []
        for p in ports:
            if p.lower() in lower:
                sel.append(F.col(q(lower[p.lower()])).alias(col(inst, p)))
            else:
                sel.append(F.lit(None).alias(col(inst, p)))
            self.where[(inst, p)] = inst
        self.frames[inst] = df.select(*sel, F.col(SEQ))

    def expression(self, inst: str, fields):
        fid = self._frame_of_inputs(inst)
        if fid is None:  # no connected inputs: hangs off nothing (constant-only expression)
            raise RuntimeError(f"{inst}: expression without inputs")
        df = self._bind_inputs(inst, fields, fid, self.frames[fid])
        comp = [f for f in fields if "VARIABLE" in f["porttype"]] + [
            f for f in fields if f["porttype"] == "OUTPUT"]
        if _is_stateful(fields):
            df = self._expression_py(inst, fields, comp, df)
        else:
            env = self._port_env(inst, fields)
            level: dict[str, int] = {}
            stages: list[list] = []
            setvars: dict[str, str] = {}
            for f in comp:
                ast = P.parse(f.get("expression") or "NULL")
                lv = 1 + max([level.get(r, 0) for r in P.refs(ast)], default=0)
                level[f["name"]] = lv
                gen = P.SqlGen(env, self.params, setvars=setvars)
                sql = _cast_sql(gen.render(ast, _ptype(f)), f)
                while len(stages) < lv:
                    stages.append([])
                stages[lv - 1].append(F.expr(sql).alias(col(inst, f["name"])))
            for st in stages:
                df = df.select("*", *st)
            # PowerCenter evaluates every port, connected or not: force ports that can ABORT.
            for f in comp:
                if "ABORT(" in (f.get("expression") or "").upper():
                    df.agg(F.count(F.col(q(col(inst, f["name"]))))).collect()
            for var, sql in setvars.items():
                agg = (spec()["mappings"][self.mapping]["variables"].get(var, {}).get("agg") or "MAX").lower()
                agg = agg if agg in ("max", "min", "count") else "max"
                self.setvars[var] = df.agg(F.expr(f"{agg}({sql})")).first()[0]
        for f in fields:
            self.where[(inst, f["name"])] = fid
        self.frames[fid] = df

    def _expression_py(self, inst, fields, comp, df: DataFrame) -> DataFrame:
        """Stateful variable ports: evaluate row by row in PowerCenter port order on one ordered partition."""
        params = dict(self.params)
        types = {f["name"]: _ptype(f) for f in fields}
        in_ports = [f["name"] for f in fields if "INPUT" in f["porttype"]]
        specs = [(f["name"], f.get("expression") or "NULL", f.get("datatype"), f.get("precision"), f.get("scale"),
                  _ptype(f), "VARIABLE" in f["porttype"]) for f in comp]
        cols_in = [col(inst, p) for p in in_ports]
        base_fields = list(df.schema.fields)
        new_fields = [T.StructField(col(inst, n), T._parse_datatype_string(P.spark_type(dt, p, s)), True)
                      for n, _, dt, p, s, _, _ in specs]
        schema = T.StructType(base_fields + new_fields)
        init = {n: ({"n": P.Decimal(0), "s": ""}.get(t)) for n, _, _, _, _, t, isvar in specs if isvar}

        def run(rows):
            from biis_lakehouse.pipelines.cpm import _pcexpr as PP
            fns = [(n, PP.compile_py(PP.parse(e), params, types, t), dt, p, s, isvar) for n, e, dt, p, s, t, isvar in specs]
            state = dict(init)
            for r in rows:
                d = r.asDict()
                env = dict(state)
                for p_, c_ in zip(in_ports, cols_in):
                    env[p_] = d[c_]
                out = []
                for n, fn, dt, p, s, isvar in fns:
                    v = PP.py_cast(fn(env), dt, p, s)
                    env[n] = v
                    if isvar:
                        state[n] = v
                    out.append(v)
                yield tuple(r) + tuple(out)

        ordered = df.repartition(1).sortWithinPartitions(SEQ)
        return self.ctx.spark.createDataFrame(ordered.rdd.mapPartitions(run), schema)

    def lookup(self, inst: str, tx):
        fields = tx["fields"]
        fid = self._frame_of_inputs(inst)
        df = self._bind_inputs(inst, fields, fid, self.frames[fid])
        lk_ports = [f for f in fields if "LOOKUP" in f["porttype"]]
        if inst in self.lookups:
            src = self.lookups[inst]
        else:
            table = tx["attrs"]["Lookup table name"]
            src = self.ctx.table(table, self.lookup_owner(table))
        lower = {c.lower(): c for c in src.columns}
        lk = src.select(*[(F.col(q(lower[f["name"].lower()])) if f["name"].lower() in lower else F.lit(None))
                          .cast(P.spark_type(f.get("datatype"), f.get("precision"), f.get("scale")))
                          .alias(f"__L_{inst}_{f['name']}") for f in lk_ports])
        env = self._port_env(inst, [f for f in fields if "INPUT" in f["porttype"]])
        env.update({f["name"]: (q(f"__L_{inst}_{f['name']}"), _ptype(f)) for f in lk_ports})
        cond_txt = tx["attrs"]["Lookup condition"]
        cond_ast = P.parse(cond_txt)
        equi = re.fullmatch(r"\s*\w+\s*=\s*\w+(\s+AND\s+\w+\s*=\s*\w+)*\s*", cond_txt, re.I) is not None
        lk_cols = [f"__L_{inst}_{f['name']}" for f in lk_ports]
        policy = (tx["attrs"].get("Lookup policy on multiple match") or "Use Any Value").lower()
        order = [F.col(q(c)).desc_nulls_last() if "last" in policy else F.col(q(c)).asc_nulls_last() for c in lk_cols]
        cond = F.expr(P.SqlGen(env, self.params).condition(cond_ast))
        if equi:
            keys = [k.split("=")[0].strip() for k in re.split(r"\s+AND\s+", cond_txt.strip(), flags=re.I)]
            kcols = [f"__L_{inst}_{k}" for k in keys]
            w = Window.partitionBy(*[q(c) for c in kcols]).orderBy(*order)
            lk = lk.withColumn("__rn", F.row_number().over(w)).filter("__rn = 1").drop("__rn")
            out = df.join(F.broadcast(lk), cond, "left")
        else:
            rid = f"__RID_{inst}"
            df = df.withColumn(rid, F.monotonically_increasing_id())
            out = df.join(F.broadcast(lk), cond, "left")
            w = Window.partitionBy(q(rid)).orderBy(*order)
            out = out.withColumn("__rn", F.row_number().over(w)).filter("__rn = 1").drop("__rn", rid)
        for f in fields:
            if "OUTPUT" in f["porttype"] and "LOOKUP" in f["porttype"]:
                out = out.withColumn(col(inst, f["name"]), F.col(q(f"__L_{inst}_{f['name']}")))
            self.where[(inst, f["name"])] = fid
        self.frames[fid] = out.drop(*lk_cols)

    def passthrough(self, inst: str, fields):
        fid = self._frame_of_inputs(inst)
        df = self._bind_inputs(inst, fields, fid, self.frames[fid])
        for f in fields:
            self.where[(inst, f["name"])] = fid
        self.frames[fid] = df

    def filter(self, inst: str, tx):
        fields = tx["fields"]
        fid = self._frame_of_inputs(inst)
        df = self._bind_inputs(inst, fields, fid, self.frames[fid])
        cond = P.SqlGen(self._port_env(inst, fields), self.params).condition(P.parse(tx["attrs"].get("Filter Condition") or "TRUE"))
        self.frames[inst] = df.filter(F.coalesce(F.expr(cond), F.lit(False)))
        for f in fields:
            self.where[(inst, f["name"])] = inst

    def router(self, inst: str, tx):
        fields = tx["fields"]
        in_fields = [f for f in fields if f.get("group") == "INPUT" or f["porttype"] == "INPUT"]
        fid = self._frame_of_inputs(inst)
        df = self._bind_inputs(inst, in_fields, fid, self.frames[fid])
        env = self._port_env(inst, in_fields)
        conds = {}
        for g in tx["groups"]:
            if g["type"] == "OUTPUT":
                conds[g["name"]] = F.coalesce(F.expr(P.SqlGen(env, self.params).condition(P.parse(g["expression"]))), F.lit(False))
        for g in tx["groups"]:
            if g["type"] == "INPUT":
                continue
            if g["type"] == "OUTPUT":
                c = conds[g["name"]]
            else:
                c = F.lit(True)
                for v in conds.values():
                    c = c & ~v
            gid = f"{inst}#{g['name']}"
            outs = [f for f in fields if f.get("group") == g["name"]]
            sel = [F.col(q(col(inst, f["ref_field"]))).alias(col(inst, f["name"])) for f in outs]
            self.frames[gid] = df.filter(c).select(*sel, F.col(SEQ))
            for f in outs:
                self.where[(inst, f["name"])] = gid

    def aggregator(self, inst: str, tx):
        fields = tx["fields"]
        fid = self._frame_of_inputs(inst)
        df = self._bind_inputs(inst, fields, fid, self.frames[fid])
        in_env = self._port_env(inst, [f for f in fields if "INPUT" in f["porttype"]])
        inline = {f["name"]: P.parse(f["expression"]) for f in fields
                  if "INPUT" not in f["porttype"] and f.get("expression")}
        keys = [f for f in fields if f.get("expressiontype") == "GROUPBY"]
        aggs = []
        for f in fields:
            if "OUTPUT" not in f["porttype"]:
                continue
            if "INPUT" in f["porttype"]:
                sql = f"max_by({q(col(inst, f['name']))}, {SEQ})"
            else:
                gen = P.SqlGen(in_env, self.params, inline={k: v for k, v in inline.items() if k != f["name"]}, agg=True)
                sql = _cast_sql(gen.render(inline[f["name"]], _ptype(f)), f)
            aggs.append(F.expr(sql).alias(col(inst, f["name"])))
        keyed = df.select("*", *[F.col(q(col(inst, f["name"]))).alias(f"__k{i}") for i, f in enumerate(keys)])
        grouped = keyed.groupBy(*[f"__k{i}" for i in range(len(keys))])
        out = grouped.agg(*aggs, F.min(SEQ).alias(SEQ)).drop(*[f"__k{i}" for i in range(len(keys))])
        self.frames[inst] = out
        for f in fields:
            self.where[(inst, f["name"])] = inst

    def joiner(self, inst: str, tx):
        fields = tx["fields"]
        master = [f for f in fields if "MASTER" in f["porttype"]]
        detail = [f for f in fields if "MASTER" not in f["porttype"]]
        src = {bp: (a, ap) for a, ap, _, bp in self.incoming.get(inst, [])}

        def side(fs):
            fids = {self.where[src[f["name"]]] for f in fs if f["name"] in src}
            fid = next(iter(fids))
            sel = [F.col(q(col(*src[f["name"]]))).alias(col(inst, f["name"])) if f["name"] in src else
                   F.lit(None).alias(col(inst, f["name"])) for f in fs]
            return self.frames[fid].select(*sel, F.col(SEQ))

        m, d = side(master).withColumnRenamed(SEQ, "__mseq"), side(detail)
        env = self._port_env(inst, fields)
        cond = F.expr(P.SqlGen(env, self.params).condition(P.parse(tx["attrs"]["Join Condition"])))
        how = {"normal join": "inner", "master outer join": "left", "detail outer join": "right",
               "full outer join": "full"}[(tx["attrs"].get("Join Type") or "Normal Join").lower()]
        out = d.join(m, cond, how)
        out = out.withColumn(SEQ, F.coalesce(F.col(SEQ), F.col("__mseq"))).drop("__mseq")
        self.frames[inst] = out
        for f in fields:
            self.where[(inst, f["name"])] = inst

    def normalizer(self, inst: str, tx):
        fields = tx["fields"]
        fid = self._frame_of_inputs(inst)
        df = self._bind_inputs(inst, fields, fid, self.frames[fid])
        occurs = {f["name"]: f["occurs"] for f in tx.get("nrm_fields", []) if f["occurs"]}
        ins: dict[str, list[str]] = {}
        for f in fields:
            if f["porttype"] == "INPUT":
                ins.setdefault(f.get("ref_source_field"), []).append(col(inst, f["name"]))
        occ_field = next(iter(occurs), None)
        if occ_field:
            arr = F.array(*[F.col(q(c)) for c in ins[occ_field]])
            df = df.select("*", F.posexplode(arr).alias("__pos", "__val"))
        start = int(next((f.get("sequence_generator_value") or 1 for f in fields if "GENERATED KEY" in f["porttype"]), 1))
        w = Window.orderBy(SEQ)
        sel = []
        for f in fields:
            pt, ref = f["porttype"], f.get("ref_source_field")
            if pt == "OUTPUT":
                sel.append((F.col("__val") if ref == occ_field and occ_field else F.col(q(ins[ref][0]))).alias(col(inst, f["name"])))
            elif "GENERATED COLUMN ID" in pt:
                sel.append((F.col("__pos") + 1).cast("int").alias(col(inst, f["name"])))
            elif "GENERATED KEY" in pt:
                sel.append((F.dense_rank().over(w) + start - 1).cast("bigint").alias(col(inst, f["name"])))
        seq = (F.col(SEQ) * 1000 + F.col("__pos")) if occ_field else F.col(SEQ)
        self.frames[inst] = df.select(*sel, seq.alias(SEQ))
        for f in fields:
            self.where[(inst, f["name"])] = inst

    def target(self, inst: str):
        src = self.incoming.get(inst, [])
        fid = self._frame_of_inputs(inst)
        df = self.frames[fid]
        self.targets[inst] = df.select(*[F.col(q(col(a, ap))).alias(bp) for a, ap, _, bp in src], F.col(SEQ)) \
            .orderBy(SEQ).drop(SEQ)

    # driver --------------------------------------------------------------
    def run(self) -> MappingResult:
        given = set(self.inputs)
        done: set[str] = set()
        for inst in _order(self.g, given):
            i = self.g.instances[inst]
            ty, tx = i["type"], i["tx"]
            if inst in given:
                self.given(inst)
            elif ty == "Source Definition":
                pass
            elif ty in ("Source Qualifier",) or (ty == "Normalizer" and inst.startswith("Norm_")):
                raise KeyError(f"session must supply {inst}")
            elif ty == "Expression":
                self.expression(inst, tx["fields"])
            elif ty == "Lookup Procedure":
                self.lookup(inst, tx)
            elif ty in ("Input Transformation", "Output Transformation"):
                self.passthrough(inst, tx["fields"])
            elif ty == "Filter":
                self.filter(inst, tx)
            elif ty == "Router":
                self.router(inst, tx)
            elif ty == "Aggregator":
                self.aggregator(inst, tx)
            elif ty == "Joiner":
                self.joiner(inst, tx)
            elif ty == "Normalizer":
                self.normalizer(inst, tx)
            elif ty == "Target Definition":
                self.target(inst)
            else:
                raise NotImplementedError(f"{inst}: {ty}")
            done.add(inst)
            self._prune(done)
        return MappingResult(self.targets, self.setvars)


def run_mapping(ctx: RunContext, mapping: str, inputs: dict[str, DataFrame], params: dict,
                lookups: dict[str, DataFrame] | None = None,
                lookup_owner: Callable[[str], Optional[str]] = lambda table: None) -> MappingResult:
    return _Run(ctx, mapping, inputs, params, lookups or {}, lookup_owner).run()


# --------------------------------------------------------------------------- Source Qualifiers
def _sq_connected_ports(mapping: str, sq: str) -> list[str]:
    m = spec()["mappings"][mapping]
    out_ports = {ap for a, ap, _, _ in m["connectors"] if a == sq}
    return [f["name"] for f in m["transformations"][sq]["fields"] if f["name"] in out_ports]


def sub_params(text: str, params: dict) -> str:
    """Replace ``$$VAR`` references in SQL text with their values (PowerCenter parameter expansion)."""
    return re.sub(r"\$\$\w+", lambda m: str(params.get(m.group(0), params.get(m.group(0)[2:], m.group(0)))), text)


def sq_query(ctx: RunContext, mapping: str, sq: str, sql: str) -> DataFrame:
    """Source Qualifier with a SQL override: the select list maps positionally onto the connected SQ ports."""
    df = ctx.spark.sql(sql)
    ports = _sq_connected_ports(mapping, sq)
    if len(df.columns) != len(ports):
        raise ValueError(f"{sq}: query returns {len(df.columns)} columns, {len(ports)} ports connected")
    return df.toDF(*ports).withColumn(SEQ, F.monotonically_increasing_id())


def sq_default(ctx: RunContext, mapping: str, sq: str, from_sql: str | None = None, where: str | None = None,
               owners: dict[str, str] | None = None) -> DataFrame:
    """Source Qualifier default query: connected ports selected from their source columns (aliases are the
    source instance names), ``from_sql`` replaces the implicit comma join, ``where`` = user join / filter."""
    m = spec()["mappings"][mapping]
    owners = owners or {}
    ports = _sq_connected_ports(mapping, sq)
    src = {bp: (a, ap) for a, ap, b, bp in m["connectors"] if b == sq}
    sel = ", ".join(f"{src[p][0]}.{src[p][1]} AS {p}" for p in ports)
    if from_sql is None:
        insts = sorted({a for a, _ in src.values()})
        from_sql = ", ".join(f"{ctx.table_name(m['instances'][i]['tx'], owners.get(i))} {i}" for i in insts)
    sql = f"SELECT {sel} FROM {from_sql}" + (f" WHERE {where}" if where else "")
    return ctx.spark.sql(sql).withColumn(SEQ, F.monotonically_increasing_id())


def vsam_input(ctx: RunContext, mapping: str, normalizer: str, frame: DataFrame) -> DataFrame:
    """Map a parsed VSAM record frame (copybook field names) onto the VSAM Normalizer's ports."""
    m = spec()["mappings"][mapping]
    src = {bp: ap for a, ap, b, bp in m["connectors"] if b == normalizer}
    cols = set(frame.columns)
    sel = [(F.col(q(src[f["name"]])) if src.get(f["name"]) in cols else F.lit(None).cast("string")).alias(f["name"])
           for f in m["transformations"][normalizer]["fields"]]
    return frame.select(*sel, F.col(SEQ))
