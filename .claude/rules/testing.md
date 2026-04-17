# Testing

How we write tests. Tests are production code — the rules in `code-style.md` apply here too.

## Core principles

1. **Test behavior, not implementation.** If a refactor that preserves behavior breaks your test, the test was wrong. Assert on outputs and observable side effects, not on which internal methods got called.
2. **Fast, isolated, deterministic.** A slow or flaky suite gets ignored. Each test runs in milliseconds, in any order, with no shared state, producing the same result every time.
3. **One behavior per test.** The name tells you what behavior. A test may have multiple `assert` lines, but they're all verifying the same thing.
4. **Tests are documentation.** A new reader should learn how the system behaves by reading the test names.
5. **No logic in tests.** No `if`, no `for`, no `try`. If a test needs branching, it's really multiple tests — use `parametrize`.

## Structure & organization

6. **`tests/` mirrors `src/`.** `src/foo/bar.py` → `tests/foo/test_bar.py`. One test file per source file is the default.
7. **Naming: `test_*.py` files, `test_*` functions, `Test*` classes.** pytest's default discovery — don't fight it.
8. **Use classes only to group fixtures,** not for shared state and not for "organization." A flat list of functions is usually clearer.
9. **Shared fixtures live in `conftest.py`** at the narrowest scope that makes sense — per-directory first, package-level only when genuinely cross-cutting.

## Anatomy & naming

10. **Arrange / Act / Assert.** Three visual blocks separated by a blank line. If you can't tell which part is which, restructure.
11. **Test names read as sentences.** `test_user_creation_rejects_empty_email`, `test_cache_evicts_oldest_entry_when_full`. Not `test_user_1`, not `test_it_works`.
12. **No docstrings on tests.** The name is the doc. If you feel you need a docstring, the name is wrong.
13. **One logical AAA per test.** Don't arrange → act → assert → act → assert. That's two tests pretending to be one.

## Fixtures

14. **Fixtures over setup/teardown.** Never `setUp`/`tearDown` (that's unittest). Use `@pytest.fixture` with `yield` for cleanup.
15. **Scope on purpose.** Default to `function`. Widen to `module` or `session` only for genuinely expensive setup (DB containers, compiled assets).
16. **Compose fixtures; don't inherit test classes.** A fixture can depend on other fixtures — that's how you share.
17. **Factory fixtures for variable data.** When tests need *similar but different* objects, yield a factory function from the fixture, not a fixed instance.

## Parametrization

18. **`@pytest.mark.parametrize` over loops.** Each case is a separate test with its own pass/fail, traceback, and name.
19. **Give cases readable IDs.** Use `ids=` or `pytest.param(..., id="...")` so failures name the case, not `test_foo[0-None-True-]`.
20. **Table-driven for input/output pairs.** When testing a pure function across many inputs, parametrize with `(input, expected)` tuples.

## Assertions

21. **Plain `assert`.** Not `assertEqual`, not `assertTrue`. pytest rewrites `assert` to give rich failure output.
22. **Helpful messages when non-obvious.** `assert x == y, f"expected {y}, got {x}"` — only when the default output wouldn't tell you what went wrong.
23. **`pytest.raises` for expected exceptions,** and assert on the exception type or message when it matters.
24. **`pytest.approx` for floats.** Never `==` on floating-point.

## Mocking & isolation

25. **Mock at boundaries, not internals.** Patch the HTTP client, the database, the clock — not a helper function three layers into your own code.
26. **Dependency injection beats `patch()`.** If you reach for `@patch("mymodule.something")`, your code probably wants that dependency passed in instead.
27. **Prefer fakes over mocks.** A fake repository backed by a dict is easier to work with and more realistic than a `Mock` with configured return values.
28. **Don't mock what you own unless you have to.** Mocking your own classes couples tests to their current structure.
29. **Verify behavior, not calls.** `assert repo.get(id) == user` tells you the system works. `mock_repo.get.assert_called_once_with(id)` only tells you the test was written.

## Determinism

30. **No `time.sleep` in tests. Ever.** If you think you need it, you need a fake clock or an event.
31. **Freeze time explicitly.** Use `freezegun` or `pytest-freezer` when logic depends on `now()`. Don't compare against "roughly now."
32. **Seed randomness.** If the code under test uses `random`, inject a seeded `Random` instance or patch it.
33. **No network in unit tests.** If it hits a real URL, it's an integration test and belongs in a separate directory/marker.
34. **No filesystem outside `tmp_path`.** Use pytest's `tmp_path` fixture; never write to the repo or `/tmp` directly.

## What to test

35. **Happy path, edge cases, error cases** — at minimum. Edge cases = empty, one, many, boundary values. Error cases = invalid inputs, upstream failures.
36. **Don't test the language or the library.** No tests that verify `json.loads` parses JSON or that `dict` stores keys.
37. **Coverage is a smell-test, not a goal.** 80% with good tests beats 100% with `assert result is not None` everywhere.
38. **Test at the right layer.** A behavior owned by a pure function should be tested there, not indirectly through an API handler.

## The pyramid

39. **Most tests are unit tests.** Fast, isolated, no I/O. These are the bulk of the suite.
40. **Fewer integration tests.** Real dependencies (DB, queue, filesystem), one subsystem at a time. Mark them with `@pytest.mark.integration`.
41. **Fewest end-to-end tests.** Full stack, real user paths. Slow and flaky-prone — reserve for critical flows only.

## Async & property-based

42. **Async tests use `pytest-asyncio`.** Mark with `@pytest.mark.asyncio`, or configure `asyncio_mode = "auto"`.
43. **Property-based testing with `hypothesis`** for invariants — "for any valid input, the output satisfies X." Especially useful for parsers, serializers, and pure algorithms.

## Performance & selection

- Aim for <100ms per unit test, <10s for the full unit suite.
- Mark slow tests (`@pytest.mark.slow`) so they can be skipped in tight dev loops.
- Use `pytest-xdist` (`-n auto`) for parallel runs once the suite is non-trivial.
- `pytest -k "pattern"` and `pytest --lf` during development — don't run everything every time.

## Tooling

- **Runner:** `pytest`
- **Coverage:** `pytest-cov`
- **Async:** `pytest-asyncio`
- **Time:** `freezegun` or `pytest-freezer`
- **Parallelism:** `pytest-xdist`
- **Property-based:** `hypothesis`

## Avoid

Common anti-patterns. Don't do these.

- **Testing private methods directly.** If `_helper` needs tests, it wants to be public — or its behavior should be covered through the public API.
- **Asserting on log messages as the only assertion.** Logs aren't contracts. Assert on the behavior they describe.
- **Snapshot/golden-file tests for everything.** Fine for rendered output (HTML, CLI), terrible for business logic — they lock in implementation and get blindly updated on failure.
- **Over-mocking.** A test with six `@patch` decorators is testing the patches, not the code.
- **Shared mutable state between tests.** Module-level lists, class attributes tests append to, singletons that leak. Guaranteed flakiness.
- **Tests that depend on order.** If `test_b` breaks when run before `test_a`, both tests are broken.
- **Comments explaining what the test does.** The name should. If the name can't, split the test.
- **`try/except` in a test to "make it not fail."** The test should fail — that's its job. If you're catching an exception, use `pytest.raises`.
- **Huge setup for tiny assertions.** 40 lines of fixture plumbing for one `assert x == 2` means the unit under test is doing too much, or you're testing at the wrong layer.
- **Tests that mirror the implementation line-by-line.** If the test reads like a copy of the function, it's locked to the current structure and will break on any refactor.
- **`assert True`, `assert result`, `assert not None`.** These pass for almost any bug. Assert on the actual expected value.
- **Conditional skips without a reason.** `@pytest.mark.skipif(...)` needs a comment explaining *why* and when it can be removed.
- **Flaky tests left in the suite.** A test that fails 1 time in 20 will be ignored when it's right. Fix it or delete it.
- **Tests written for coverage only.** A test that calls a function and asserts nothing rarely catches bugs. Write it with a real expectation or don't write it.
- **Integration tests masquerading as unit tests.** If it hits the network, the disk, or a real database, it's not a unit test — move it and mark it.
