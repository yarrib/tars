import json

from tars.explode import explode_ai_query, explode_extraction

PARSED_TEXT = "Form 1040 for tax year 2023. Total tax: $4,321. Filed single."

RESULT = {
    "response": {
        "total_tax": {"value": 4321, "confidence": 0.91, "citation_ids": [0]},
        "filing_status": {"value": "single", "confidence": 0.8, "citation_ids": [1]},
        "employer": {"name": {"value": "Acme", "confidence": 0.5, "citation_ids": []}},
        "missing": {"value": None, "confidence": 0.1, "citation_ids": []},
    },
    "error_message": None,
    "metadata": {"citations": [
        {"id": 0, "start": PARSED_TEXT.index("$4,321"), "stop": PARSED_TEXT.index("$4,321") + 6},
        {"id": 1, "start": PARSED_TEXT.index("single"), "stop": PARSED_TEXT.index("single") + 6},
    ]},
}


def rows_by_name(rows):
    return {r["field_name"]: r for r in rows}


def test_scalar_fields_flattened():
    rows = rows_by_name(explode_extraction(json.dumps(RESULT), PARSED_TEXT))
    assert rows["total_tax"]["value"] == "4321"
    assert rows["total_tax"]["confidence"] == 0.91
    assert rows["filing_status"]["value"] == "single"


def test_citation_offsets_materialized_to_text():
    rows = rows_by_name(explode_extraction(RESULT, PARSED_TEXT))
    cit = rows["total_tax"]["citations"][0]
    assert PARSED_TEXT[cit["start"]:cit["stop"]] == cit["text"]
    assert cit["text"] == "$4,321"
    assert rows["filing_status"]["citations"][0]["text"] == "single"


def test_nested_fields_get_dotted_names():
    rows = rows_by_name(explode_extraction(RESULT, PARSED_TEXT))
    assert rows["employer.name"]["value"] == "Acme"


def test_null_value_kept_as_row():
    rows = rows_by_name(explode_extraction(RESULT, PARSED_TEXT))
    assert rows["missing"]["value"] is None


def test_document_error_yields_single_error_row():
    rows = explode_extraction({"error_message": "context too long", "response": {}}, "")
    assert len(rows) == 1
    assert rows[0]["field_name"] == "_document"
    assert "context too long" in rows[0]["error_message"]


def test_ai_query_evidence_grounded_when_found():
    response = json.dumps({"fields": [
        {"name": "total_tax", "value": "4321", "confidence": 0.9, "evidence": "$4,321"},
        {"name": "hallucinated", "value": "x", "confidence": 0.9, "evidence": "not in doc"},
    ]})
    rows = rows_by_name(explode_ai_query(response, PARSED_TEXT))
    grounded = rows["total_tax"]["citations"][0]
    assert grounded["start"] is not None
    assert PARSED_TEXT[grounded["start"]:grounded["stop"]] == "$4,321"
    ungrounded = rows["hallucinated"]["citations"][0]
    assert ungrounded["start"] is None
    assert ungrounded["text"] == "not in doc"


def test_ai_query_unparseable_response():
    rows = explode_ai_query("not json {", PARSED_TEXT)
    assert rows[0]["field_name"] == "_document"
    assert rows[0]["error_message"]
