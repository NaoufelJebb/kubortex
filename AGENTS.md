# AGENTS.md — AI Coding Agent Guidelines
_Last updated: 2026-03-25_

This file defines the operating rules for AI coding agents (Codex, Claude, etc.).

Agents must follow these guidelines before modifying code.

---

# Development Philosophy (SPARC)

Follow SPARC principles for structured, high-quality development:

**S — Simplicity**
Prefer clear, maintainable implementations. Avoid unnecessary complexity.

**P — Pattern**
Follow established repository patterns and architectural conventions.
Propose alternatives only when there is a clear technical justification.

**A — Architecture**
Design modular components with well-defined interfaces and integration points.

**R — Refinement**
Improve implementations iteratively using testing, review, and feedback.

**C — Completion**
Ensure changes include:
- working implementation
- tests when applicable
- updated documentation when needed

---

# Non-Negotiable Rules

| ID | Agents MUST do | Agents MUST NOT do |
|---|---|---|
| G0 | Read `README.md` before writing code. Also check for `AGENTS.md` files in relevant subdirectories. | Write code without sufficient project context. |
| G1 | Ask the developer for clarification when requirements or architecture are unclear. | Guess project-specific behavior or requirements. |
| G2 | Add **`AIDEV-NOTE:` anchor comments** near complex or non-obvious changes. | Remove or modify existing `AIDEV-` comments. |
| G3 | Stay within the scope of the current task. | Continue work from a previous prompt after a "new task". |
| G4 | Use structured reasoning to identify root causes and validate design decisions. | Make architectural decisions without analysis. |
| G5 | Modify existing code directly when refactoring. | Create duplicate implementations of the same functionality. |

---

# Code Modification Guidelines

When implementing changes:

1. **Understand the context**
   - Read relevant files completely.
   - Identify patterns used in similar parts of the repository.

2. **Minimize surface area**
   - Prefer small, focused changes.
   - Avoid unrelated refactoring unless required.

3. **Follow existing conventions**
   - Naming
   - Module structure
   - Error handling
   - Logging

---

# Task Execution Model

Agents should follow this workflow:

1. Read `README.md`
2. Locate relevant source files
3. Understand existing architecture
4. Implement minimal required change
5. Run linting and tests
6. Ensure no regressions
7. Document non-obvious decisions

Do not declare a task complete unless:
- the implementation works.
- the code follows repository conventions.
- tests pass.
- linting passes.


# Dev Workflow — Environments

All development for **Kubortex** happens inside its project directory. The project uses **uv** for Python
environment and dependency management via `pyproject.toml`. Before working on the codebase, activate the uv-managed virtual environment, and install the project dependencies in editable mode.

```bash
cd projects/kizuki
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"


## Testing

- **Framework**: Use `pytest`.
- **Location**: Tests are stored in `tests/`, mirroring the structure of `src/`.

Run tests from the **project root** using `uv`:

```bash
uv run pytest --cov=src
```

Disable Python bytecode caching by setting `PYTHONDONTWRITEBYTECODE=1` when running Python commands (e.g., `PYTHONDONTWRITEBYTECODE=1 uv run pytest`).

Guidelines:
- Maintain the same module structure between `src/` and `tests/`.
- Add tests for all new features and bug fixes.
- Tests must be deterministic and should not rely on external services or uncontrolled filesystem state.
- Use fixtures for setup and teardown where appropriate.

---

# Dev Workflow – Code Quality

Code should follow consistent standards emphasizing clarity, modularity, and maintainability.

General principles:
- **Keep files concise** (preferably under ~300 lines). Refactor when files grow too large.
- **Write modular code** with small, focused functions and classes.
- **Apply DRY principles** by extracting shared logic into reusable utilities.
- **Prioritize readability and maintainability over clever implementations.**

---

## Linting & Formatting

Use **`ruff`** for both linting and formatting.

Run commands from the **project root**.

### Format code

```bash
uv run ruff format src/
```

### Check for linting issues

```bash
uv run ruff check src/
```

---

## Recommended Agent Workflow

When modifying code:

1. Implement the change.
2. Format the code:
   ```bash
   uv run ruff format src/
   ```
3. Fix linting issues:
   ```bash
   uv run ruff check src/
   ```
4. Run tests:
   ```bash
   uv run pytest --cov=src
   ```

A task is considered complete only when:
- Code formatting has been applied.
- `ruff check` reports no issues.
- All tests pass.
