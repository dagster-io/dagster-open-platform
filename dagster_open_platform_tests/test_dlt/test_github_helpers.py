import pytest
from dagster_open_platform.defs.dlt.sources.github import helpers

run_graphql_query = getattr(helpers, "_run_graphql_query")


class FakeResponse:
    def __init__(self, payload: object, status_code: int):
        self._payload = payload
        self.status_code = status_code

    def json(self) -> object:
        return self._payload


def test_run_graphql_query_raises_actionable_error_when_response_has_no_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_post(*_args: object, **_kwargs: object) -> FakeResponse:
        return FakeResponse({"message": "Bad credentials"}, 401)

    monkeypatch.setattr(helpers.session, "post", fake_post)

    with pytest.raises(ValueError, match=r"missing 'data'.*401.*Bad credentials"):
        run_graphql_query("token", "query", {})


def test_run_graphql_query_returns_data_and_rate_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(*_args: object, **_kwargs: object) -> FakeResponse:
        return FakeResponse(
            {
                "data": {
                    "repository": {"issues": {"nodes": []}},
                    "rateLimit": {"cost": 1, "remaining": 4999},
                }
            },
            200,
        )

    monkeypatch.setattr(helpers.session, "post", fake_post)

    data, rate_limit = run_graphql_query("token", "query", {})

    assert data == {"repository": {"issues": {"nodes": []}}}
    assert rate_limit == {"cost": 1, "remaining": 4999}
