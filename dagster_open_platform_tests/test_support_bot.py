from dagster_open_platform.assets.support_bot import parse_discussion, parse_issue


def test_parse_discussion():
    discussion = {
        "id": "1234",
        "title": "Sample Discussion",
        "bodyText": "This is a sample discussion body.",
        "answer": {"bodyText": "This is a sample answer."},
        "category": {"name": "General"},
        "createdAt": "2023-06-01T00:00:00Z",
        "closed": "true",
        "url": "https://example.com/discussion/1234",
        "labels": {"nodes": [{"name": "label1"}, {"name": "label2"}]},
        "reactions": {"totalCount": 5},
        "stateReason": "SOME_REASON",
    }

    parsed_discussion = parse_discussion(discussion)

    assert parsed_discussion == {
        "id": "1234",
        "type": "text",
        "text": "DISCUSSION TITLE: Sample Discussion\nQUESTION: This is a sample discussion body.\nANSWER: This is a sample answer.",
        "document_type": "discussion",
        "title": "Sample Discussion",
        "category": "General",
        "created_at": "2023-06-01T00:00:00Z",
        "closed": "true",
        "url": "https://example.com/discussion/1234",
        "labels": "label1,label2",
        "state_reason": "SOME_REASON",
        "votes": 5,
    }


def test_parse_issue():
    issue = {
        "id": "5678",
        "title": "Sample Issue",
        "bodyText": "This is a sample issue body.",
        "comments": {"nodes": [{"bodyText": "Comment 1"}, {"bodyText": "Comment 2"}]},
        "createdAt": "2023-06-01T00:00:00Z",
        "closedAt": "2023-06-02T00:00:00Z",
        "state": "CLOSED",
        "url": "https://example.com/issue/5678",
        "labels": {"nodes": [{"name": "bug"}, {"name": "enhancement"}]},
        "reactions": {"totalCount": 3},
        "stateReason": "SOME_REASON",
    }

    parsed_issue = parse_issue(issue)

    assert parsed_issue == {
        "id": "5678",
        "type": "text",
        "text": "ISSUE TITLE: Sample Issue\nBODY: This is a sample issue body.\n---\nCOMMENTS: Comment 1\nComment 2",
        "document_type": "issue",
        "title": "Sample Issue",
        "created_at": "2023-06-01T00:00:00Z",
        "closed_at": "2023-06-02T00:00:00Z",
        "state": "CLOSED",
        "state_reason": "SOME_REASON",
        "url": "https://example.com/issue/5678",
        "labels": "bug,enhancement",
        "votes": 3,
    }
