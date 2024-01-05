from enum import Enum
from typing import Any, Dict, List, Optional


class ExtractionStatus(Enum):
    DISCOVERY = "discovery"
    DISCOVERY_ERROR = "discovery_error"
    TAP = "tap"
    TAP_ERROR = "tap_error"
    TARGET = "target"
    TARGET_ERROR = "target_error"
    COMPLETE = "complete"

    def discovery_status(self) -> Optional[int]:
        if self in {
            ExtractionStatus.TAP,
            ExtractionStatus.TAP_ERROR,
            ExtractionStatus.TARGET,
            ExtractionStatus.TARGET_ERROR,
            ExtractionStatus.COMPLETE,
        }:
            return 0
        elif self == ExtractionStatus.DISCOVERY_ERROR:
            return 1
        else:
            return None

    def tap_status(self) -> Optional[int]:
        if self in {
            ExtractionStatus.TARGET,
            ExtractionStatus.TARGET_ERROR,
            ExtractionStatus.COMPLETE,
        }:
            return 0
        elif self == ExtractionStatus.TAP_ERROR:
            return 1
        else:
            return None

    def target_status(self) -> Optional[int]:
        if self in {
            ExtractionStatus.COMPLETE,
        }:
            return 0
        elif self == ExtractionStatus.TARGET_ERROR:
            return 1
        else:
            return None


def sample_streams_data(stream_data: Dict[str, int]) -> List[Dict[str, Any]]:
    return [
        {
            "stream_name": stream_name,
            "metadata": {
                "row-count": row_count,
            },
        }
        for stream_name, row_count in stream_data.items()
    ]


def sample_extractions_data(jobs: Dict[str, ExtractionStatus]) -> Dict[str, Any]:
    return {
        "data": [
            {
                "job_name": job_name,
                "discovery_exit_status": status.discovery_status(),
                "discovery_error_message": (
                    "discovery error" if status == ExtractionStatus.DISCOVERY_ERROR else None
                ),
                "tap_exit_status": status.tap_status(),
                "tap_error_message": "tap error" if status == ExtractionStatus.TAP_ERROR else None,
                "target_exit_status": status.target_status(),
                "target_error_message": (
                    "target error" if status == ExtractionStatus.TARGET_ERROR else None
                ),
            }
            for job_name, status in jobs.items()
        ]
    }
