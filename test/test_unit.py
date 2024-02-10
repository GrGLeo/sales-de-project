from utils import flat_json

def test_flat_json_simple():
    input_json = {
        'sale1': {'item': 'apple', 'quantity': 5},
        'sale2': {'item': 'banana', 'quantity': 10}
    }
    expected_output = {
        'uid4': ['sale1', 'sale2'],
        'item': ['apple', 'banana'],
        'quantity': [5, 10]
    }
    assert flat_json(input_json) == expected_output

# Test case for nested JSON
def test_flat_json_nested():
    input_json = {
        'sale1': {'item': 'apple', 'details': {'quantity': 5, 'price': 2}},
        'sale2': {'item': 'banana', 'details': {'quantity': 10, 'price': 1}}
    }
    expected_output = {
        'uid4': ['sale1', 'sale2'],
        'item': ['apple', 'banana'],
        'quantity': [5, 10],
        'price': [2, 1]
    }
    assert flat_json(input_json) == expected_output