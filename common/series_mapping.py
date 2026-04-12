"""Hardcoded CPI series mappings for the demo pipeline."""

from typing import Final

SHARED_CPI_SERIES: Final[dict[str, dict[str, str]]] = {
    "all_items": {
        "label": "All items",
        "normalized_series": "us_all_items_cpi",
        "bls_series_id": "CUSR0000SA0",
        "imf_series_id": "USA.CPI._T.IX.M",
    },
    "education": {
        "label": "Education",
        "normalized_series": "us_education_cpi",
        "bls_series_id": "CUSR0000SAE1",
        "imf_series_id": "USA.CPI.CP10.IX.M",
    },
    "communication": {
        "label": "Communication",
        "normalized_series": "us_communication_cpi",
        "bls_series_id": "CUSR0000SAE2",
        "imf_series_id": "USA.CPI.CP08.IX.M",
    },
    "medical_care": {
        "label": "Medical care",
        "normalized_series": "us_medical_care_cpi",
        "bls_series_id": "CUSR0000SAM",
        "imf_series_id": "USA.CPI.CP06.IX.M",
    },
    "recreation": {
        "label": "Recreation",
        "normalized_series": "us_recreation_cpi",
        "bls_series_id": "CUSR0000SAR",
        "imf_series_id": "USA.CPI.CP09.IX.M",
    },
    "transportation": {
        "label": "Transportation",
        "normalized_series": "us_transportation_cpi",
        "bls_series_id": "CUSR0000SAT",
        "imf_series_id": "USA.CPI.CP07.IX.M",
    },
}

SUPPORTED_CATEGORY_KEYS: Final[tuple[str, ...]] = tuple(SHARED_CPI_SERIES)
DEFAULT_CATEGORY_KEY: Final[str] = "all_items"
SUPPORTED_NORMALIZED_SERIES: Final[tuple[str, ...]] = tuple(
    series["normalized_series"] for series in SHARED_CPI_SERIES.values()
)
CHART_FILENAMES_BY_CATEGORY: Final[dict[str, tuple[str, ...]]] = {
    category: (
        f"bls_{series['normalized_series']}.png",
        f"imf_{series['normalized_series']}.png",
        f"{category}_yoy_inflation_by_source.png",
        f"{category}_top_12m_inflation_periods.png",
    )
    for category, series in SHARED_CPI_SERIES.items()
}
DEMO_CHART_FILENAMES: Final[tuple[str, ...]] = tuple(
    filename
    for filenames in CHART_FILENAMES_BY_CATEGORY.values()
    for filename in filenames
)

BLS_SERIES_TO_METADATA: Final[dict[str, dict[str, str]]] = {
    series["bls_series_id"]: {
        "category": category,
        "label": series["label"],
        "normalized_series": series["normalized_series"],
    }
    for category, series in SHARED_CPI_SERIES.items()
}

IMF_SERIES_TO_METADATA: Final[dict[str, dict[str, str]]] = {
    series["imf_series_id"]: {
        "category": category,
        "label": series["label"],
        "normalized_series": series["normalized_series"],
    }
    for category, series in SHARED_CPI_SERIES.items()
}
