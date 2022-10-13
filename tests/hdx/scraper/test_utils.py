from hdx.scraper.utilities import string_params_to_dict


class TestUtils:
    def test_string_params_to_dict(self):
        result = string_params_to_dict("a: 123, b: 345")
        assert result == {"a": "123", "b": "345"}
        result = string_params_to_dict("a:123,b:345")
        assert result == {"a": "123", "b": "345"}
