from tars.normalize import normalize_value, values_match


def test_money_formats_converge():
    assert normalize_value("$1,234.50") == normalize_value("1234.5")
    assert normalize_value("$1,234.50") == "1234.5"


def test_parens_negative():
    assert normalize_value("(1,200)") == "-1200"
    assert normalize_value("($45.00)") == "-45"


def test_trailing_zeros_trimmed():
    assert normalize_value("100.00") == "100"
    assert normalize_value("0.50") == "0.5"


def test_dates_converge():
    assert normalize_value("01/15/2024") == "2024-01-15"
    assert normalize_value("January 15, 2024") == "2024-01-15"
    assert normalize_value("2024-01-15") == "2024-01-15"


def test_text_lowercased_whitespace_collapsed():
    assert normalize_value("  Married   Filing Jointly ") == "married filing jointly"


def test_none_passthrough():
    assert normalize_value(None) is None


def test_values_match_any_ground_truth_occurrence():
    exact, norm = values_match("1234.5", ["$1,234.50", "999"])
    assert not exact and norm
    exact, norm = values_match("999", ["$1,234.50", "999"])
    assert exact and norm


def test_values_match_none_extraction():
    exact, norm = values_match(None, ["x"])
    assert not exact and not norm
