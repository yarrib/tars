from tars.pairing import match_files


def test_exact_stem_match():
    pairs, unpaired = match_files(["return.pdf", "return.xml"])
    assert pairs == [("return.pdf", "return.xml")]
    assert unpaired == []


def test_substring_containment_both_directions():
    pairs, _ = match_files(["smith_1040.pdf", "smith_1040_groundtruth.xml"])
    assert pairs == [("smith_1040.pdf", "smith_1040_groundtruth.xml")]
    pairs, _ = match_files(["client_a_w2_2023_scan.pdf", "client_a_w2_2023.xml"])
    assert pairs == [("client_a_w2_2023_scan.pdf", "client_a_w2_2023.xml")]


def test_case_insensitive():
    pairs, _ = match_files(["Return.PDF", "RETURN.xml"])
    assert pairs == [("Return.PDF", "RETURN.xml")]


def test_exact_match_preferred_over_longer_candidate():
    pairs, _ = match_files(["doc.pdf", "doc.xml", "doc_extra.xml"])
    assert ("doc.pdf", "doc.xml") in pairs


def test_unpaired_surfaced():
    pairs, unpaired = match_files(["a.pdf", "b.xml", "notes.txt"])
    assert pairs == []
    assert set(unpaired) == {"a.pdf", "b.xml"}


def test_xml_used_at_most_once():
    pairs, unpaired = match_files(["doc.pdf", "doc_copy.pdf", "doc.xml"])
    assert len(pairs) == 1
    assert len(unpaired) == 1


def test_deterministic():
    names = ["z.pdf", "a.pdf", "a.xml", "z.xml"]
    assert match_files(names) == match_files(list(reversed(names)))
