# type: ignore
"""Tests for aredis_om.model.token_escaper – RediSearch token escaping."""

import re

from aredis_om.model.token_escaper import TokenEscaper


class TestTokenEscaper:
    def test_default_escaper_escapes_comma(self):
        esc = TokenEscaper()
        assert esc.escape("a,b") == r"a\,b"

    def test_default_escaper_escapes_period(self):
        esc = TokenEscaper()
        assert esc.escape("a.b") == r"a\.b"

    def test_default_escaper_escapes_angle_brackets(self):
        esc = TokenEscaper()
        assert esc.escape("<tag>") == r"\<tag\>"

    def test_default_escaper_escapes_curly_braces(self):
        esc = TokenEscaper()
        assert esc.escape("{a}") == r"\{a\}"

    def test_default_escaper_escapes_square_brackets(self):
        esc = TokenEscaper()
        assert esc.escape("[idx]") == r"\[idx\]"

    def test_default_escaper_escapes_backslash(self):
        esc = TokenEscaper()
        assert esc.escape("a\\b") == r"a\\b"

    def test_default_escaper_escapes_double_quote(self):
        esc = TokenEscaper()
        assert esc.escape('"hello"') == '\\"hello\\"'

    def test_default_escaper_escapes_single_quote(self):
        esc = TokenEscaper()
        assert esc.escape("it's") == "it\\'s"

    def test_default_escaper_escapes_colon(self):
        esc = TokenEscaper()
        assert esc.escape("key:val") == r"key\:val"

    def test_default_escaper_escapes_semicolon(self):
        esc = TokenEscaper()
        assert esc.escape("a;b") == r"a\;b"

    def test_default_escaper_escapes_exclamation(self):
        esc = TokenEscaper()
        assert esc.escape("wow!") == r"wow\!"

    def test_default_escaper_escapes_at_sign(self):
        esc = TokenEscaper()
        assert esc.escape("a@b") == r"a\@b"

    def test_default_escaper_escapes_hash(self):
        esc = TokenEscaper()
        assert esc.escape("#tag") == r"\#tag"

    def test_default_escaper_escapes_dollar(self):
        esc = TokenEscaper()
        assert esc.escape("$100") == r"\$100"

    def test_default_escaper_escapes_percent(self):
        esc = TokenEscaper()
        assert esc.escape("50%") == r"50\%"

    def test_default_escaper_escapes_caret(self):
        esc = TokenEscaper()
        assert esc.escape("a^b") == r"a\^b"

    def test_default_escaper_escapes_ampersand(self):
        esc = TokenEscaper()
        assert esc.escape("a&b") == r"a\&b"

    def test_default_escaper_escapes_asterisk(self):
        esc = TokenEscaper()
        assert esc.escape("a*b") == r"a\*b"

    def test_default_escaper_escapes_parentheses(self):
        esc = TokenEscaper()
        assert esc.escape("(a)") == r"\(a\)"

    def test_default_escaper_escapes_hyphen(self):
        esc = TokenEscaper()
        assert esc.escape("a-b") == r"a\-b"

    def test_default_escaper_escapes_plus(self):
        esc = TokenEscaper()
        assert esc.escape("a+b") == r"a\+b"

    def test_default_escaper_escapes_equals(self):
        esc = TokenEscaper()
        assert esc.escape("a=b") == r"a\=b"

    def test_default_escaper_escapes_tilde(self):
        esc = TokenEscaper()
        assert esc.escape("~home") == r"\~home"

    def test_default_escaper_escapes_slash(self):
        esc = TokenEscaper()
        assert esc.escape("a/b") == r"a\/b"

    def test_default_escaper_escapes_space(self):
        esc = TokenEscaper()
        assert esc.escape("hello world") == r"hello\ world"

    def test_plain_alphanumeric_unchanged(self):
        esc = TokenEscaper()
        assert esc.escape("hello123") == "hello123"

    def test_empty_string(self):
        esc = TokenEscaper()
        assert esc.escape("") == ""

    def test_multiple_special_chars(self):
        esc = TokenEscaper()
        result = esc.escape("a@b.c")
        assert result == r"a\@b\.c"

    def test_custom_escape_pattern(self):
        custom_re = re.compile(r"[xy]")
        esc = TokenEscaper(escape_chars_re=custom_re)
        assert esc.escape("axybz") == r"a\x\ybz"
        # Characters not in the custom pattern should remain unescaped
        assert esc.escape("a.b") == "a.b"
