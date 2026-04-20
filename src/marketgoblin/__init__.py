"""marketgoblin — market data platform for downloading and storing financial data."""

from marketgoblin.classification import Classification, IndustryProfile, SectorProfile
from marketgoblin.datasets import Dataset
from marketgoblin.goblin import MarketGoblin
from marketgoblin.ticker_metadata import TickerMetadata

__version__ = "0.3.0"
__all__ = [
    "Classification",
    "Dataset",
    "IndustryProfile",
    "MarketGoblin",
    "SectorProfile",
    "TickerMetadata",
]
