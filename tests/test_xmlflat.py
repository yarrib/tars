from tars.xmlflat import flatten_xml


def test_leaf_paths_exclude_root():
    rows = flatten_xml("<Return><Filer><Name>Jane</Name></Filer></Return>")
    assert rows == [("Filer.Name", "Jane")]


def test_repeated_tags_keep_order():
    xml = "<R><Dep><Name>A</Name></Dep><Dep><Name>B</Name></Dep></R>"
    assert flatten_xml(xml) == [("Dep.Name", "A"), ("Dep.Name", "B")]


def test_namespace_stripped():
    xml = '<ns:Return xmlns:ns="http://x"><ns:Total>5</ns:Total></ns:Return>'
    assert flatten_xml(xml) == [("Total", "5")]


def test_empty_and_whitespace_text():
    rows = flatten_xml("<R><A>  </A><B>x</B></R>")
    assert rows == [("A", ""), ("B", "x")]


def test_root_leaf():
    assert flatten_xml("<Only>v</Only>") == [("Only", "v")]
