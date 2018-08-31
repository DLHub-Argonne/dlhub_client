import dlhub_client.client as client
import pandas
import pytest


def test_dlhub_init():
    dl = client.DLHub()
    assert dl.service == "https://dlhub.org/api/v1"
    assert isinstance(dl, client.DLHub)


def test_get_servables():
    dl = client.DLHub()
    r = dl.get_servables()
    assert isinstance(r, pandas.DataFrame)
    assert r.shape[-1] > 0
    assert r.shape[0] != 0


def test_get_id_by_name():
    dl = client.DLHub()
    name = "oqmd_model"
    r = dl.get_id_by_name(name)
    r2 = dl.get_servables()
    true_val = r2["uuid"][r2["name"].tolist().index(name)]
    assert r == true_val

    # Invalid name
    with pytest.raises(IndexError):
        dl.get_id_by_name("foo")


def test_run():
    dl = client.DLHub()
    name = "zhuozhao_test_noop"
    data = {"data": ["V", "Co", "Zr"]}
    serv = dl.get_id_by_name(name)
    res = dl.run(serv, data)

    # Check the results
    assert isinstance(res, pandas.DataFrame)
