from hdx.scraper.utilities import get_rowval, string_params_to_dict


class TestUtils:
    def test_get_rowval(self):
        row = {"header": "lalala"}
        result = get_rowval(row, "{{header}}")
        assert result == "lalala"

    def test_string_params_to_dict(self):
        result = string_params_to_dict("a: 123, b: 345")
        assert result == {"a": "123", "b": "345"}
        result = string_params_to_dict("a:123,b:345")
        assert result == {"a": "123", "b": "345"}
