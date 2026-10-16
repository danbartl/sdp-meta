import io
import zipfile

import pytest
from openpyxl import Workbook
from polars.testing import assert_frame_equal

from sdp_meta_poc.cases import SCHEMA, batch
from sdp_meta_poc.delivery import make_delivery, read_excel, read_zip


@pytest.mark.parametrize("name", ["initial", "incremental", "late"])
def test_delivery_round_trip_preserves_nulls_integers_and_duplicates(name, tmp_path):
    actual, manifest = make_delivery(name, name, tmp_path)
    assert_frame_equal(actual, batch(name))
    assert len(manifest["original_sha256"]) == 64
    with pytest.raises(FileExistsError):
        make_delivery(name, name, tmp_path)


def test_archive_path_traversal_rejected():
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w") as archive:
        archive.writestr("../events.csv", "x")
    with pytest.raises(ValueError, match="exactly"):
        read_zip(data.getvalue())


def test_excel_formula_not_silently_read_as_null():
    workbook = Workbook()
    workbook.active.title = "events"
    workbook.active.append(list(SCHEMA))
    workbook.active.append(["a", "A", "=1+1", 1, "UPSERT"])
    data = io.BytesIO()
    workbook.save(data)
    with pytest.raises(ValueError, match="formulas"):
        read_excel(data.getvalue())
