from pathlib import Path

import pytest

from biis_lakehouse.ops import files


def test_remove_file(tmp_path):
    (tmp_path / "a.txt").write_text("x")
    assert files.remove_file(str(tmp_path), "a.txt") is True
    assert not (tmp_path / "a.txt").exists()
    assert files.remove_file(str(tmp_path), "a.txt") is False  # absent: no-op like remove_file.sh


def test_archive_files_appends_pay_period(tmp_path):
    src, dst = tmp_path / "in" / "LES", tmp_path / "archive" / "LES"
    src.mkdir(parents=True)
    (src / "p1234d01.txt").write_text("1")
    (src / "LESFILE").write_text("2")
    moved = files.archive_files(str(src), str(dst), "201905")
    assert sorted(Path(m).name for m in moved) == ["LESFILE_P201905.txt", "p1234d01_P201905.txt"]
    assert list(src.iterdir()) == []
    assert (dst / "p1234d01_P201905.txt").read_text() == "1"


def test_move_glob_and_concat(tmp_path):
    (tmp_path / "in").mkdir()
    for n in ("tatran1", "tatran2", "other"):
        (tmp_path / "in" / n).write_text(n)
    files.move_glob(str(tmp_path / "in"), "tatran*", str(tmp_path / "arch"))
    assert sorted(p.name for p in (tmp_path / "arch").iterdir()) == ["tatran1", "tatran2"]
    with pytest.raises(FileNotFoundError):
        files.move_glob(str(tmp_path / "in"), "tatran*", str(tmp_path / "arch"))

    (tmp_path / "h").write_text("HDR\n")
    (tmp_path / "d").write_text("DAT\n")
    files.concat([str(tmp_path / "h"), str(tmp_path / "d")], str(tmp_path / "out"))
    assert (tmp_path / "out").read_text() == "HDR\nDAT\n"
