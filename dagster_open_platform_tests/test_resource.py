import pytest
import responses
from dagster_open_platform.resources.stitch_resource import StitchResource

from .utils import ExtractionStatus, sample_extractions_data


@responses.activate
@pytest.mark.parametrize(
    "status",
    [
        ExtractionStatus.COMPLETE,
        ExtractionStatus.DISCOVERY_ERROR,
        ExtractionStatus.TAP_ERROR,
        ExtractionStatus.TARGET_ERROR,
    ],
)
def test_poll_sync(status: ExtractionStatus) -> None:
    client_id = "my_client_id"
    stitch_resource = StitchResource(stitch_client_id=client_id, access_token="bar")

    responses.add(
        method=responses.GET,
        url=f"{stitch_resource.api_base_url}v4/{client_id}/extractions",
        json=sample_extractions_data(
            {
                "my_job": status,
                "unrelated_job": ExtractionStatus.COMPLETE,
            }
        ),
        status=200,
    )

    if status == ExtractionStatus.COMPLETE:
        stitch_resource.poll_sync(source_id="foo", job_name="my_job", poll_interval=0.1)
    else:
        stage = status.value.split("_")[0]
        with pytest.raises(Exception, match=f"Stitch {stage} phase failed: {stage} error"):
            stitch_resource.poll_sync(source_id="foo", job_name="my_job", poll_interval=0.1)
