
import pytest
from loadutil.libs.storageutils import StorageUtils

@pytest.mark.dummy
def test_dummy():
    assert True

@pytest.mark.the_best_test
def test_flat_columns_list():
    storage_utils = StorageUtils()
    flat_columns_list = storage_utils.create_flat_columns_list([('a',''), ('b', 'c')])
    if flat_columns_list[0] == 'a':
        assert True
    else:
        assert False
