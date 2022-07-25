"""Library Unittests"""
import pytest
from csvms import __version__, pyproject

@pytest.fixture
def toml() -> dict():
    yield pyproject("pyproject.toml")

def test_version(toml):
    """ Check Version """
    assert __version__ == toml['tool.poetry'].get('version')

def test_config(toml):
    """ Check Poetry configuration file """
    assert toml['tool.poetry'].get('name') is not None
    assert toml['tool.poetry'].get('version') is not None
    assert toml['tool.poetry'].get('description') is not None
    assert toml['tool.poetry'].get('authors') is not None
    assert toml['tool.poetry'].get('license') is not None
    assert toml['tool.poetry'].get('readme') is not None

def test_config_license(toml):
    """ Check License """
    assert toml['tool.poetry'].get('license') == "MIT"
