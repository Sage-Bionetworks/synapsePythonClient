<!-- Last reviewed: 2026-03 -->

## Project

User-facing documentation for the Synapse Python Client. Built with MkDocs + Material theme, deployed via GitHub Pages. Follows the Diataxis documentation framework with four content types: tutorials, guides, reference, and explanations.

## Stack

MkDocs with Material theme, mkdocstrings (Google-style docstrings), termynal (CLI animations), markdown-include (file embedding).

### Python style
- Use built-in generics (`list`, `dict`, `tuple`, `set`) instead of `typing.List`, `typing.Dict`, etc. (Python 3.9+)

## Conventions

### Content types (Diataxis framework)
- **tutorials/** — Step-by-step learning (competence-building). Themed around a biomedical researcher working with Alzheimer's Disease data. Progressive build-up: Project → Folder → File → Annotations → etc.
- **guides/** — How-to guides for specific use cases (problem-solution oriented). Includes extension-specific guides (curator).
- **reference/** — API reference auto-generated from docstrings via mkdocstrings. Split into `experimental/sync/` and `experimental/async/` for new OOP API.
- **explanations/** — Deep conceptual content ("why" not just "how"). Design decisions, internal machinery.

### File inclusion pattern (markdown-include)
Tutorial code lives in `tutorials/python/tutorial_scripts/*.py` and is embedded in markdown via line-range includes:
```markdown
{!docs/tutorials/python/tutorial_scripts/annotation.py!lines=5-23}
```
Single source of truth — edit the `.py` file, not the markdown. Changing line numbers in scripts requires updating the line ranges in the corresponding `.md` files.

### mkdocstrings reference generation
Reference markdown files use `::: synapseclient.ClassName` syntax to trigger auto-generation from docstrings. Key configuration:
- `docstring_style: google` — parse Google-style docstrings
- `members_order: source` — preserve source code order
- `filters: ["!^_", "!to_synapse_request", "!fill_from_dict"]` — private members, `to_synapse_request()`, and `fill_from_dict()` are excluded from docs
- `inherited_members: true` — shows mixin methods on inheriting classes
- Member lists are explicit — each reference page specifies which methods to document
- When adding a new public method to a model class, add it to the `members:` list in the corresponding reference pages (`docs/reference/experimental/sync/` and `docs/reference/experimental/async/`). Without this, mkdocstrings won't generate an anchor and cross-references like `[synapseclient.models.ClassName.method]` will break.

### Anchor links for cross-referencing
Pattern: `[](){ #reference-anchor }` in reference pages. Tutorials link to reference via `[API Reference][project-reference-sync]`. Explicit type hints use: `[syn.login][synapseclient.Synapse.login]`.

### termynal CLI animations
Terminal animation blocks marked with `<!-- termynal -->` HTML comment. Prompts configured as `$` or `>`. Used in authentication.md and installation docs.

### Custom CSS (`css/custom.css`)
- API reference indentation: `doc-contents` has 25px left padding with border
- Smaller table font (0.7rem) for API docs
- Wide layout: `max-width: 1700px` for complex content

### Navigation structure
Defined in `mkdocs.yml` nav section. 5 main sections: Home, Tutorials, How-To Guides, API Reference, Further Reading, News. API Reference has ~85 markdown files (~40 legacy, ~45 experimental).

## Constraints

- Do not edit tutorial code inline in markdown — edit the `.py` script file in `tutorial_scripts/` and update line ranges if needed.
- Reference docs auto-generate from source docstrings — to change method documentation, edit the docstring in the Python source, not the markdown.
- `mkdocs.yml` is at the repo root, not in `docs/` — it configures the entire doc build.
- Docs deploy to Read the Docs (configured via `.readthedocs.yaml` at repo root).
- Local build output goes to `docs_site/` (via `site_dir` in `mkdocs.yml`) — gitignored.
- Cross-referencing uses the `autorefs` plugin: `[display text][synapseclient.ClassName.method]` auto-resolves to mkdocstrings anchors.

### news.md
Release notes live in `docs/news.md`. Each release gets a heading with the version number and date, followed by bullet points describing changes. Group entries by category (Features, Bug Fixes, etc.). Reference Jira ticket numbers (SYNPY-XXXX) in each entry.
