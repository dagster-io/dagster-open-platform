import logging

import responses
from dagster import IntMetadataValue
from dagster._core.definitions import build_assets_job
from dagster_open_platform.resources.stitch_resource import StitchResource, build_stitch_assets

from .utils import ExtractionStatus, sample_extractions_data, sample_streams_data


@responses.activate
def test_build_assets(capsys, caplog) -> None:
    with caplog.at_level(logging.INFO):
        client_id = "my_client_id"
        source_id = "foo"

        stitch_resource = StitchResource(stitch_client_id=client_id, access_token="bar")
        responses.add(
            method=responses.GET,
            url=f"{stitch_resource.api_base_url}v4/sources/{source_id}/streams",
            json=sample_streams_data(
                {
                    "lorem": 100,
                    "ipsum": 200,
                    "dolor": 300,
                }
            ),
            status=200,
        )

        responses.add(
            method=responses.POST,
            url=f"{stitch_resource.api_base_url}v4/sources/{source_id}/sync",
            json={"job_name": "my_new_job"},
            status=200,
        )

        responses.add(
            method=responses.GET,
            url=f"{stitch_resource.api_base_url}v4/{client_id}/extractions",
            json=sample_extractions_data(
                {
                    "unrelated_job": ExtractionStatus.COMPLETE,
                    "my_new_job": ExtractionStatus.COMPLETE,
                }
            ),
            status=200,
        )

        responses.add(
            method=responses.GET,
            url=f"{stitch_resource.api_base_url}v4/{client_id}/extractions/my_new_job",
            body="this is my\nlog\noutput",
            status=200,
        )

        stitch_assets = build_stitch_assets(
            source_id=source_id,
            destination_tables=["lorem", "ipsum", "dolor"],
            asset_key_prefix=["stitch"],
        )

        stitch_job = build_assets_job(
            "stitch_job",
            stitch_assets,
            resource_defs={"stitch": stitch_resource},
        )

        res = stitch_job.execute_in_process()

    assert res.success
    assert f"Sync initialized for source_id={source_id} with job name my_new_job" in caplog.text
    assert "Polling for job my_new_job, status: complete" in caplog.text

    assert "this is my\nlog\noutput" in capsys.readouterr().out

    materializations = res.asset_materializations_for_node(f"stitch_sync_{source_id}")
    assert len(materializations) == 3
    assert {".".join(materialization.asset_key.path) for materialization in materializations} == {
        "stitch.lorem",
        "stitch.ipsum",
        "stitch.dolor",
    }
    assert next(
        mat for mat in materializations if mat.asset_key.path == ["stitch", "lorem"]
    ).metadata["row_count"] == IntMetadataValue(100)
