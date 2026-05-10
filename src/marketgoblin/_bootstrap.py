# Side effects that must run before the rest of the package imports.
# Currently: load a local .env so credentials (e.g. TIINGO_API_KEY) become
# available without requiring callers to export env vars per shell session.
# Imported for its side effect from marketgoblin/__init__.py.

from dotenv import load_dotenv

# Walks from cwd upward; no-op if no .env is found. Existing env vars win
# over .env values by default — explicit shell exports stay authoritative.
load_dotenv()
