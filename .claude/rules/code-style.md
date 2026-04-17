# Code Style

Guidelines for writing Python in this codebase. When in doubt, follow the Zen of Python — *simple is better than complex*.

## Core principles

1. **Clean, production-ready code.** No spaghetti. Modularity first, pure functions whenever feasible. Side effects live at the edges of the system, not in the middle of business logic.
2. **Testable by construction.** If a function is hard to unit-test, that's a design smell — refactor before writing the test. Dependencies should be injectable, not hardcoded.
3. **Composable over monolithic.** Small pieces that snap together. Prefer many small functions that can be combined over one large function with flags.
4. **Design patterns with intent.** Reach for a pattern only when it solves a concrete problem that already exists in the code. No speculative abstractions. When you use one, leave a one-line comment naming it (`# Strategy pattern: ...`).
5. **Composition over inheritance.** Deep class hierarchies are almost always the wrong answer. Prefer protocols, dataclasses, and plain functions.

## Typing

6. **Everything is typed.** All function signatures — arguments and return types — are annotated. Module-level constants too when non-obvious.
7. **Native types over `typing` imports.** `dict[str, int]` not `Dict[str, int]`. `list`, `tuple`, `X | None` instead of `Optional[X]`, `X | Y` instead of `Union[X, Y]`.
8. **Structured data uses `dataclass` or `pydantic.BaseModel`.** Don't pass dicts around between layers — if it has a shape, give it a type.

## Docstrings & comments

9. **Docstrings are succinct but complete.** One-line summary for simple functions. Multi-line with `Args`/`Returns`/`Raises` sections when behavior is non-trivial. Skip documenting parameters whose names already make them obvious — don't pad.
10. **File header comment.** Each `.py` file starts with a single comment (2–3 lines max) describing what lives in it.
11. **Inline comments explain *why*, not *what*.** The code shows what it does; comments exist for intent, tradeoffs, and non-obvious decisions.

## Idioms & modern Python

12. **Pathlib over `os.path`.** Always.
13. **f-strings for formatting.** Not `%` or `.format()`.
14. **Context managers for resources.** Files, locks, connections, temp dirs — always `with`.
15. **No mutable default arguments.** `def f(x: list | None = None)` then `x = x or []` inside.
16. **No magic numbers or strings.** Hoist to module-level `UPPER_CASE` constants with a name that explains the value.

## Error handling

17. **Fail loud, fail specific.** Raise the narrowest exception that fits. Never `except:` or `except Exception:` without re-raising or logging with full context.
18. **Validate at boundaries.** Check inputs at the entry point of a module; trust them internally.
19. **No silent fallbacks.** If something unexpected happens, the user/caller needs to know.

## Control flow

20. **Early returns and guard clauses.** Flatten nesting. If you're past 3 levels of indentation, something wants to become its own function.
21. **No dead code.** Delete it — version control remembers.

## Imports

22. **Three groups, separated by a blank line:** stdlib, third-party, local. Alphabetized within each group.
23. **No wildcard imports.** No unused imports. `ruff` enforces this.

## Naming

24. `snake_case` for functions, methods, variables, modules.
25. `PascalCase` for classes.
26. `SCREAMING_SNAKE_CASE` for module-level constants.
27. `_leading_underscore` for internal/private — it's a contract with the reader.
28. Names describe purpose, not type. `users` not `user_list`. `is_active` for booleans.

## Testing

29. **Framework: `pytest`.** Use fixtures for setup, `@pytest.mark.parametrize` instead of loops inside tests.
30. **Tests live in `tests/`** mirroring the package structure. `src/foo/bar.py` → `tests/foo/test_bar.py`.
31. **One behavior per test.** Test names read like sentences: `test_user_creation_rejects_empty_email`.
32. **No logic in tests** — no loops or conditionals. If a test needs branching, it's really multiple tests.

## Logging

33. **`logging` module, never `print`** in non-throwaway code. Use module-level loggers: `logger = logging.getLogger(__name__)`.
34. **Structured when possible.** Include relevant context in the log record, not smushed into the message string.

## Size smell-tests

These are signals, not hard rules. Crossing them means *look closer*, not *reject*.

- Functions over ~50 lines → probably doing more than one thing.
- Files over ~300 lines → probably more than one concern.
- Functions with more than ~5 parameters → group them into a dataclass.
- Classes with more than ~7 public methods → probably more than one responsibility.

## Tooling

- **Formatter:** `ruff format` (or `black`)
- **Linter:** `ruff check`
- **Type checker:** `mypy` (strict mode) or `pyright`
- **Test runner:** `pytest`

Run all four before considering work "done."

## Avoid

Common anti-patterns. Don't do these.

- **Defensive `try/except` around code that shouldn't fail.** If the happy path is expected, let it run. Exceptions are for *exceptional* cases, not control flow.
- **Wrapping exceptions just to re-raise them.** If you have nothing to add (context, cleanup, translation to a domain exception), don't catch.
- **Over-commenting.** `# increment i` above `i += 1` is noise. Trust the reader.
- **Abstract base classes with a single implementation.** YAGNI. Use the concrete class; introduce the abstraction when the second implementation actually exists.
- **Config objects passed five layers deep.** If only the leaf needs it, pass only what the leaf needs.
- **Stringly-typed APIs.** `status: Literal["active", "inactive"]` or an `Enum`, not `status: str` with a docstring listing allowed values.
- **`Any` as an escape hatch.** If you're typing `Any`, you're opting out of the type system. Usually means the shape needs a `TypedDict`, `Protocol`, or `dataclass`.
- **Getters and setters for plain attributes.** This isn't Java. Use `@property` only when there's actual logic.
- **Catching `Exception` to "log and continue."** You're hiding bugs. Either handle a specific exception or let it propagate.
- **`os.system` / `subprocess.call` with shell strings.** Use `subprocess.run([...], check=True)` with a list of args.
- **Mutating arguments.** Functions take inputs and return outputs. If you need to modify, return a new value.
- **`global` / module-level mutable state.** Thread-unsafe, test-hostile, and almost always avoidable with explicit passing or a class.
- **Comments that duplicate the docstring,** or docstrings that duplicate the type signature. Pick one source of truth.
- **Tests that test the mock.** If your test only verifies that a mocked function was called with the args you just told it to pass, it's testing nothing.
- **Premature generalization.** Don't build a plugin system for two cases. Wait for the third.
