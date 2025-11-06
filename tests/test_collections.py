import pytest

from gfw.common.collections import DeepChainMap


def test_deep_chain_map():
    config_args = {"nested1": {"key1": 123, "key2": 456}}
    cli_args = {"nested1": {"key1": 789}}
    expected_dict = {"nested1": {"key1": 789, "key2": 456}}

    config = DeepChainMap(cli_args, config_args)
    assert config["nested1"]["key1"] == 789
    assert config["nested1"]["key2"] == 456

    assert isinstance(config["nested1"], DeepChainMap)

    config_dict = config.to_dict()
    assert config_dict == expected_dict

    assert isinstance(config_dict["nested1"], dict)

    with pytest.raises(KeyError):
        config["missing"]
