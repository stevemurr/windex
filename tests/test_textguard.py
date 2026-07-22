"""Pure unit tests for the empty-text guard (Defect A)."""

from windex.textguard import is_empty_text


def test_is_empty_text_true_for_none_empty_and_whitespace():
    assert is_empty_text(None) is True
    assert is_empty_text("") is True
    assert is_empty_text("   \n\t ") is True


def test_is_empty_text_false_when_any_visible_content():
    assert is_empty_text("hi") is False
    assert is_empty_text("  Latvia Startups  ") is False  # a legit short HN title


def test_is_empty_text_strips_smuggled_code_points_before_checking():
    # A string of only Tags-block code points has no visible content.
    smuggled = "".join(chr(c) for c in range(0xE0020, 0xE0030))
    assert is_empty_text(smuggled) is True
