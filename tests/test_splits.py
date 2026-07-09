from collections import Counter

from tars.splits import assign_split, split_bucket


def test_deterministic():
    assert split_bucket("acme llc") == split_bucket("acme llc")
    assert assign_split("acme llc") == assign_split("acme llc")


def test_bucket_range():
    for client in ("a", "b", "acme", "Smith & Co", "日本商事"):
        assert 0 <= split_bucket(client) < 100


def test_ratios_roughly_hold():
    counts = Counter(assign_split(f"client-{i}", dev_pct=10, val_pct=20)
                     for i in range(5000))
    assert 0.07 < counts["dev"] / 5000 < 0.13
    assert 0.17 < counts["val"] / 5000 < 0.23
    assert 0.64 < counts["test"] / 5000 < 0.76


def test_boundaries_respect_percentages():
    # dev_pct=0 means nothing lands in dev
    assert all(assign_split(f"c{i}", dev_pct=0, val_pct=0) == "test"
               for i in range(200))
    assert all(assign_split(f"c{i}", dev_pct=100, val_pct=0) == "dev"
               for i in range(200))
