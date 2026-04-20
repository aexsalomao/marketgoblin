"""marketgoblin — market data platform for downloading and storing financial data."""

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

__version__ = "0.4.0"
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
