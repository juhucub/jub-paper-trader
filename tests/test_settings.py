from backend.core.settings import Settings, clear_settings_cache, get_settings


def test_settings_normalize_runtime_style_debug_strings():
    assert Settings(_env_file=None, debug="release").debug is False
    assert Settings(_env_file=None, debug="debug").debug is True
    assert Settings(_env_file=None, debug_bot_cycle="production").debug_bot_cycle is False
    assert Settings(_env_file=None, bot_order_replace_enabled="development").bot_order_replace_enabled is True


def test_get_settings_cache_can_be_cleared_between_env_changes(monkeypatch):
    clear_settings_cache()
    monkeypatch.setenv("DEBUG", "release")
    assert get_settings().debug is False

    monkeypatch.setenv("DEBUG", "debug")
    assert get_settings().debug is False

    clear_settings_cache()
    assert get_settings().debug is True

    clear_settings_cache()
