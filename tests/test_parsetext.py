from tars.parsetext import extract_text


def test_elements_with_page_markers():
    parse = {"document": {
        "pages": [{"id": 0}, {"id": 1}],
        "elements": [
            {"content": "Form 1040", "page_id": 0},
            {"content": "Line 9: 50,000", "page_id": 0},
            {"content": "Schedule B", "page_id": 1},
        ],
    }}
    text, pages = extract_text(parse)
    assert pages == 2
    assert "[page 1]" in text and "[page 2]" in text
    assert text.index("Form 1040") < text.index("Schedule B")


def test_fallback_collects_content_recursively():
    parse = {"weird": {"nested": [{"content": "alpha"}, {"deeper": {"text": "beta"}}]}}
    text, pages = extract_text(parse)
    assert "alpha" in text and "beta" in text
    assert pages is None


def test_empty_parse():
    text, pages = extract_text({})
    assert text == ""
    assert pages is None
