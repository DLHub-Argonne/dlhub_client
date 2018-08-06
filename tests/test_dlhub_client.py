import dlhub_client.client as client
import pandas
import pytest
import mdf_toolbox
import globus_sdk
import re
from mdf_forge.forge import Query

# NOTE: Change to dlhub once dlhub is populated
default_ix = "dlhub-test"

def test_dlhub_init():
    dl = client.DLHub()
    assert dl.service == "http://dlhub.org:5000/api/v1"
    assert isinstance(dl.search_client, globus_sdk.SearchClient)
    # Check default index
    assert dl.index == default_ix
    # NOTE: Change to dlhub-test once dlhub is populated
    dl2 = client.DLHub(index="dlhub")
    assert dl2.index == "dlhub"
    dl3 = client.DLHub(anonymous=True)
    assert isinstance(dl.search_client, globus_sdk.SearchClient)

def test_get_servables():
    dl = client.DLHub()
    r = dl.get_servables()
    assert isinstance(r, pandas.DataFrame)
    # Number of categories returned = 12
    assert r.shape[-1] == 12
    # At least one servable running
    assert r.shape[0] != 0

def test_get_id_by_name():
    dl = client.DLHub()
    name = "noop"
    r = dl.get_id_by_name(name)
    r2 = dl.get_servables()
    true_val = r2["uuid"][r2["name"].tolist().index(name)]
    assert r == true_val
    #Invalid name
    with pytest.raises(IndexError):
        dl.get_id_by_name("foo")

def test_run():
    dl = client.DLHub()
    #Test against noop
    name = "noop"
    data = {"data":""}
    serv = dl.get_id_by_name(name)
    res = dl.run(serv, data)
    assert isinstance(res, pandas.DataFrame)
    assert res.empty
    # Test against real model
    name = "metallic_glass"
    data = {"data":["V","Co","Zr"]}
    serv = dl.get_id_by_name(name)
    res = dl.run(serv, data)
    assert isinstance(res, pandas.DataFrame)
    assert len(res["pred"]) < 400
    assert len(res["pred"]) > 0


def test_search(capsys):
    # Error on no query
    dl = client.DLHub()
    assert dl.search() == []
    out, err = capsys.readouterr()
    assert "Error: No query" in out

    # Return info if requested
    res2 = dl.search(q="image recognition", info=False)
    assert isinstance(res2, list)
    assert isinstance(res2[0], dict)

    res3 = dl.search(q="image recognition", info=True)
    assert isinstance(res3, tuple)
    assert isinstance(res3[0], list)
    assert isinstance(res3[0][0], dict)
    assert isinstance(res3[1], dict)

    # Check limits
    res4 = dl.search("model", limit=2)
    assert len(res4) == 2

    # Check reset_query
    dl.match_field("dlhub.domain", "image recognition")
    res5 = dl.search(reset_query=False)
    res6 = dl.search()
    assert all([r in res6 for r in res5]) and all([r in res5 for r in res6])

    # Check default index
    dl2 = client.DLHub()
    assert dl2.search("model", limit=1, info=True)[1]["index"] == default_ix


###### Helper From Forge Tests: ######
# Return codes:
#  -1: No match, the value was never found
#   0: Exclusive match, no values other than argument found
#   1: Inclusive match, some values other than argument found
#   2: Partial match, value is found in some but not all results
def check_field(res, field, regex):
    dict_path = ""
    for key in field.split("."):
        if key == "[]":
            dict_path += "[0]"
        else:
            dict_path += "['{}']".format(key)
    # If no results, set matches to false
    all_match = (len(res) > 0)
    only_match = (len(res) > 0)
    some_match = False
    for r in res:
        vals = eval("r"+dict_path)
        if type(vals) is not list:
            vals = [vals]
        # If a result does not contain the value, no match
        if regex not in vals and not any([re.search(str(regex), str(value)) for value in vals]):
            all_match = False
            only_match = False
        # If a result contains other values, inclusive match
        elif len(vals) != 1:
            only_match = False
            some_match = True
        else:
            some_match = True

    if only_match:
        # Exclusive match
        return 0
    elif all_match:
        # Inclusive match
        return 1
    elif some_match:
        # Partial match
        return 2
    else:
        # No match
        return -1


def test_match_field():
    dl = client.DLHub()
    # Basic usage
    dl.match_field("servable.name", "cifar10")
    res1 = dl.search()
    assert check_field(res1, "servable.name", "cifar10") == 0
    # Check that query clears
    assert dl.search() == []

    # Also checking check_field
    dl.match_field("dlhub.domain", "physics")
    res2 = dl.search()
    assert check_field(res2, "dlhub.domain", "physics") == 0

    # Empty field and value shouldn't match anything
    res3 = dl.match_field("", "").search()
    assert res3 == []

    # Invalid field should't return any results
    res4 = dl.match_field("foo", "bar").search()
    assert res4 == []

def test_search_by_domain():
    dl = client.DLHub()
    # Empty domain shouldn't change return anything
    assert dl.search_by_domain("") == []

    # Basic usage
    res1 = dl.search_by_domain("image recognition")
    assert check_field(res1, "dlhub.domain", "image recognition") == 0

    # Multiple domains
    res2 = dl.search_by_domain(["image recognition", "physics"])
    assert check_field(res2, "dlhub.domain", "image recognition") == 2
    assert check_field(res2, "dlhub.domain", "physics") == 2

    # Error on invalid input
    with pytest.raises(ValueError) as e:
        dl.search_by_domain(1234)
        assert "input domain must be a list or str" in e.args[0]

def test_search_by_titles():
    dl = client.DLHub()
    # One title
    res1 = dl.search_by_titles("Cifar10")
    assert res1 != []
    assert check_field(res1, "datacite.title", "Cifar10") == 0

    # Many titles
    res2 = dl.search_by_titles(["Cifar10",
    "Deep-Learning Super-resolution Image Reconstruction (DSIR)"
    ])
    assert res2 != []
    assert check_field(res2, "datacite.title", "Cifar10") == 2

    # No titles
    res3 = dl.search_by_titles("")
    assert res3 == []

def test_reset_query():
    dl = client.DLHub()
    # Term will return results
    dl.match_field("datacite.publicationYear", 2018)
    dl.reset_query()
    # Specifying no query will return no results
    assert dl.search() == []

def test_get_input_info():
    dl = client.DLHub()
    # Invalid model request
    with pytest.raises(ValueError) as e:
        dl.get_input_info("foo")
        assert "No such model found: 'foo'" in e.args[0]

    # Basic usage
    res1 = dl.get_input_info("cifar10")
    compare_res = dl.search_by_titles(["Cifar10"])[0]
    assert res1 == compare_res["servable"]["run"]["input"]["shape"]

    # With description
    res2, res3 = dl.get_input_info("cifar10", info=True)
    assert res2 == compare_res["servable"]["run"]["input"]["shape"]
    assert res3 == compare_res["servable"]["run"]["input"]["description"]

    with pytest.raises(ValueError) as e:
        dl.get_input_info(["foo"])
        assert "Model name must be a str. Instead got type: 'list'" in e.args[0]

def test_get_output_info():
    dl = client.DLHub()
    # Invalid model request
    with pytest.raises(ValueError) as e:
        dl.get_output_info("foo")
        assert "No such model found: 'foo'" in e.args[0]

    # Basic usage
    res1 = dl.get_output_info("cifar10")
    compare_res = dl.search_by_titles(["Cifar10"])[0]
    assert res1 == compare_res["servable"]["run"]["output"]["shape"]

    # With description
    res2, res3 = dl.get_output_info("cifar10", info=True)
    assert res2 == compare_res["servable"]["run"]["output"]["shape"]
    assert res3 == compare_res["servable"]["run"]["output"]["description"]

    with pytest.raises(ValueError) as e:
        dl.get_output_info(["foo"])
        assert "Model name must be a str. Instead got type: 'list'" in e.args[0]

def test_match_years(capsys):
    dl = client.DLHub()
    # No input
    assert dl == dl.match_years()

    # Invalid input
    dl.match_years(["foo"]).search()
    out, err = capsys.readouterr()
    assert "Invalid year: 'foo'" in out

    dl.match_years(start="foo").search()
    out, err = capsys.readouterr()
    assert "Invalid start year: 'foo'" in out

    dl.match_years(stop="foo").search()
    out, err = capsys.readouterr()
    assert "Invalid stop year: 'foo'" in out

    # One Year
    res1 = dl.match_years(2018).search()
    assert res1 != []
    assert check_field(res1, "datacite.publicationYear", 2018) == 0

    # Multiple years
    res2 = dl.match_years(["2017", 2018]).search()
    assert res2 != []
    # NOTE: Uncomment this test when multiple model years
    # NOTE: have been ingested into search
    # assert check_field(res2, "datacite.publicationYear", 2017) == 2

    # Range of years
    res3 = dl.match_years(start=2018, stop=2018, inclusive=True).search()
    assert check_field(res3, "datacite.publicationYear", 2018) == 0

    res4 = dl.match_years(start=2014, stop=2018, inclusive=True).search()
    assert check_field(res4, "datacite.publicationYear", 2015) == -1
    # NOTE: Change to == 1 once other years have been ingested into dlhub
    assert check_field(res4, "datacite.publicationYear", 2018) == 0

    res5 = dl.match_years(start=2018, stop=2018, inclusive=False).search()
    assert res5 == []

def test_match_range():
    dl = client.DLHub()
    # Range of years
    res1= dl.match_range(field="datacite.publicationYear", start=2018, stop=2018, inclusive=True).search()
    assert check_field(res1, "datacite.publicationYear", 2018) == 0

    res2 = dl.match_range(field="datacite.publicationYear", start=2014, stop=2018, inclusive=True).search()
    assert check_field(res2, "datacite.publicationYear", 2015) == -1
    # NOTE: Change to == 2 once other years have been ingested into dlhub
    assert check_field(res2, "datacite.publicationYear", 2018) == 0

    res3 = dl.match_range(field="datacite.publicationYear", start=2018, stop=2018, inclusive=False).search()
    assert res3 == []

    #No lower bound
    res4 = dl.match_range(field="datacite.publicationYear", start=None, stop=2018, inclusive=True).search()
    assert res4 != []
    assert check_field(res4, "datacite.publicationYear", 100) == -1
    # NOTE: Change to == 2 once other years have been ingested into dlhub
    assert check_field(res4, "datacite.publicationYear", 2018) == 0

    # No upper bound
    res5 = dl.match_range(field="datacite.publicationYear", start=2010, stop=None, inclusive=True).search()
    assert res5 != []
    assert check_field(res5, "datacite.publicationYear", 2000) == -1
    # NOTE: Change to == 2 once other years have been ingested into dlhub
    assert check_field(res4, "datacite.publicationYear", 2018) == 0

    res6 = dl.match_range(field="datacite.publicationYear", start=2100, stop=None, inclusive=True).search()
    assert res6 == []
    assert check_field(res6, "datacite.publicationYear", 2018) == -1
