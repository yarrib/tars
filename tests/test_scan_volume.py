import os

from tars.discovery import scan_volume, sha256_file


def make(path, content=b"x"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as handle:
        handle.write(content)


def test_scan_pairs_and_unpaired(tmp_path):
    root = str(tmp_path)
    make(f"{root}/1040/acme/return.pdf")
    make(f"{root}/1040/acme/return.xml")
    make(f"{root}/1040/acme/loose_scan.pdf")
    make(f"{root}/w2/beta/w2_2023.pdf")
    make(f"{root}/w2/beta/w2_2023_gt.xml")
    make(f"{root}/stray.pdf")

    pairs, unpaired = scan_volume(root)
    assert {(p["doc_type"], p["client"]) for p in pairs} == {("1040", "acme"), ("w2", "beta")}
    unpaired_paths = {u["path"] for u in unpaired}
    assert any("loose_scan.pdf" in p for p in unpaired_paths)
    assert any("stray.pdf" in p for p in unpaired_paths)
    stray = next(u for u in unpaired if "stray" in u["path"])
    assert stray["doc_type"] == "" and stray["client"] == ""


def test_sha256_streams(tmp_path):
    f = tmp_path / "a.bin"
    f.write_bytes(b"hello world")
    assert sha256_file(str(f)) == (
        "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9")
