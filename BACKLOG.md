# Backlog

Deferred work, tracked here instead of an issue tracker for now.

## Dex-compliant plugin/extension template

**Status**: blocked on input from repo owner.

Goal: a template (Jinja2-based, per owner's confirmation) that the
owner's `dex` CLI (https://yarrib.github.io/dex, a scaffolding tool) can
consume to generate new tars plugins/components -- so extending tars
(a new `source.*`/`classifier.*`/`parser.*`/`sink.*`) is `dex new ...`
rather than hand-copying an existing plugin file.

Blocked because this session's egress policy doesn't allow browsing
`yarrib.github.io` or arbitrary web pages generally (confirmed via the
agent proxy: 403, "not allowed by your organization's egress policy"),
and this session's GitHub tool access is scoped to `yarrib/tars` only, so
the Dex repo/docs can't be read from here either. Revisit when:

- the GitHub tool scope for a session can include the Dex repo, or
- someone pastes the Dex template manifest format (filename, schema:
  variables/prompts/target paths) and CLI invocation shape (e.g.
  `dex new <template> <target>`) directly into a session.

What's already decided, so this doesn't need re-litigating:
- Templating engine: Jinja2 (matches what tars already uses for DLT
  codegen in `src/tars/databricks/templates/`).
- What it should generate: a new plugin module under
  `src/tars/plugins/{sources,classifiers,parsers,sinks}/`, implementing
  one of the base classes in `src/tars/plugins/base.py`, registered via
  the `[project.entry-points."tars.plugins"]` table in `pyproject.toml`
  (see any existing plugin, e.g. `src/tars/plugins/classifiers/rule_based.py`,
  as the shape to mirror) -- plus a matching test file under `tests/`.
- Where it should live once built: likely alongside the existing docs in
  `docs/plugin-development.md`, or a new `templates/dex/` directory at
  repo root if Dex expects templates outside the Python package.
