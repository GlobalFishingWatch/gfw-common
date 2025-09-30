from gfw.common.bigquery.table_description import TO_BE_COMPLETED, TableDescription


def test_render_with_relevant_params():
    desc = TableDescription(
        repo_name="my-project",
        version="1.0.0",
        title="Test Table",
        subtitle="A subtitle",
        summary="This is a test summary.",
        caveats="No caveats.",
        relevant_params={"source": "AIS", "country": "AR"},
    )

    rendered = desc.render()
    assert "Test Table" in rendered
    assert "A subtitle" in rendered
    assert "my-project" in rendered
    assert "v1.0.0" in rendered
    assert "This is a test summary." in rendered
    assert "No caveats." in rendered
    # Check that relevant params are formatted properly
    assert "- source: AIS" in rendered
    assert "- country: AR" in rendered


def test_render_without_relevant_params():
    desc = TableDescription(repo_name="my-project", version="1.0.0")

    rendered = desc.render()
    # Since relevant_params is empty, should include TO_BE_COMPLETED placeholder
    assert TO_BE_COMPLETED in rendered
    # Other fields default as expected
    assert "To be completed." in rendered
    assert "my-project" in rendered
    assert "v1.0.0" in rendered
