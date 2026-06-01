"""marketgoblin — market data platform for downloading and storing financial data."""

from importlib.metadata import version

from marketgoblin import _bootstrap as _bootstrap  # noqa: F401 — runs load_dotenv()
from marketgoblin.classification import Classification, IndustryProfile, SectorProfile
from marketgoblin.datasets import Dataset
from marketgoblin.goblin import MarketGoblin
from marketgoblin.sector_indices import (
    Industry,
    IndustryGroup,
    SectorIndex,
    SectorIndexMapping,
    SubIndustry,
    load_sector_indices,
    refresh_sector_indices,
)
from marketgoblin.ticker_metadata import TickerMetadata

# Single source of truth: read from installed package metadata (pyproject
# version) so the string can't drift from the released artifact.
__version__ = version("marketgoblin")
__all__ = [
    "Classification",
    "Dataset",
    "Industry",
    "IndustryGroup",
    "IndustryProfile",
    "MarketGoblin",
    "SectorIndex",
    "SectorIndexMapping",
    "SectorProfile",
    "SubIndustry",
    "TickerMetadata",
    "load_sector_indices",
    "refresh_sector_indices",
]
