from dataplane_mcp.paging import page


def test_first_middle_and_last_page():
    items = list(range(5))
    assert page(items, 0, 2) == {"total": 5, "offset": 0, "limit": 2, "next_offset": 2, "items": [0, 1]}
    assert page(items, 2, 2) == {"total": 5, "offset": 2, "limit": 2, "next_offset": 4, "items": [2, 3]}
    assert page(items, 4, 2) == {"total": 5, "offset": 4, "limit": 2, "next_offset": None, "items": [4]}
    assert page(items, 3, 2)["next_offset"] is None


def test_out_of_range_and_bad_arguments():
    assert page([1, 2], 10, 5) == {"total": 2, "offset": 10, "limit": 5, "next_offset": None, "items": []}
    assert page([1, 2], -3, 0) == {"total": 2, "offset": 0, "limit": 1, "next_offset": 1, "items": [1]}
