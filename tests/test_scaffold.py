from windex.config import Settings
from windex.index.qdrant import alias_name, collection_name, slug


def test_settings_paths_derive_from_data_root(tmp_path):
    s = Settings(data_root=tmp_path, _env_file=None)
    assert s.ccnews_downloads_dir == tmp_path / "downloads" / "ccnews"
    assert s.news_staging_dir == tmp_path / "staging" / "news"


def test_github_token_list_parses_csv():
    s = Settings(github_tokens=" tok1, tok2 ,", _env_file=None)
    assert s.github_token_list() == ["tok1", "tok2"]


def test_collection_naming_is_slugged_and_aliased():
    assert slug("BAAI/bge-m3") == "baai-bge-m3"
    assert collection_name("news", "BAAI/bge-m3") == "news__baai-bge-m3"
    assert alias_name("news") == "news_current"
