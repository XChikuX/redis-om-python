# type: ignore
"""Tests for aredis_om.model.render_tree – tree rendering utility."""

from aredis_om.model.render_tree import render_tree


class SimpleNode:
    """A minimal tree node used for testing."""

    def __init__(self, name, left=None, right=None):
        self.name = name
        self.left = left
        self.right = right


# ---------------------------------------------------------------------------
# Leaf node (no children)
# ---------------------------------------------------------------------------


def test_render_single_leaf_node():
    node = SimpleNode("root")
    output = render_tree(node)
    assert "root" in output


def test_render_leaf_returns_string():
    node = SimpleNode("leaf")
    result = render_tree(node)
    assert isinstance(result, str)


# ---------------------------------------------------------------------------
# Node with only a left child
# ---------------------------------------------------------------------------


def test_render_node_with_left_child_only():
    child = SimpleNode("left_child")
    root = SimpleNode("root", left=child)
    output = render_tree(root)
    assert "root" in output
    assert "left_child" in output


# ---------------------------------------------------------------------------
# Node with only a right child
# ---------------------------------------------------------------------------


def test_render_node_with_right_child_only():
    child = SimpleNode("right_child")
    root = SimpleNode("root", right=child)
    output = render_tree(root)
    assert "root" in output
    assert "right_child" in output


# ---------------------------------------------------------------------------
# Node with both children
# ---------------------------------------------------------------------------


def test_render_node_with_both_children():
    left = SimpleNode("L")
    right = SimpleNode("R")
    root = SimpleNode("parent", left=left, right=right)
    output = render_tree(root)
    assert "parent" in output
    assert "L" in output
    assert "R" in output


# ---------------------------------------------------------------------------
# Deeper tree (3 levels)
# ---------------------------------------------------------------------------


def test_render_three_level_tree():
    ll = SimpleNode("LL")
    lr = SimpleNode("LR")
    left = SimpleNode("L", left=ll, right=lr)
    right = SimpleNode("R")
    root = SimpleNode("root", left=left, right=right)
    output = render_tree(root)
    for name in ("root", "L", "R", "LL", "LR"):
        assert name in output


# ---------------------------------------------------------------------------
# Custom attribute names
# ---------------------------------------------------------------------------


def test_render_custom_nameattr():
    class Custom:
        def __init__(self, label, left=None, right=None):
            self.label = label
            self.left = left
            self.right = right

    root = Custom("hello")
    output = render_tree(root, nameattr="label")
    assert "hello" in output


def test_render_custom_child_attrs():
    class Custom:
        def __init__(self, name, lhs=None, rhs=None):
            self.name = name
            self.lhs = lhs
            self.rhs = rhs

    child = Custom("kid")
    root = Custom("dad", lhs=child)
    output = render_tree(root, left_child="lhs", right_child="rhs")
    assert "dad" in output
    assert "kid" in output


# ---------------------------------------------------------------------------
# Node without the nameattr → falls back to str()
# ---------------------------------------------------------------------------


def test_render_node_without_nameattr_uses_str():
    class Stringable:
        def __init__(self, val, left=None, right=None):
            self._val = val
            self.left = left
            self.right = right

        def __str__(self):
            return self._val

    root = Stringable("strval")
    output = render_tree(root, nameattr="nonexistent")
    assert "strval" in output


# ---------------------------------------------------------------------------
# Tree shape markers – ensure connector characters exist
# ---------------------------------------------------------------------------


def test_render_tree_contains_box_drawing_chars():
    left = SimpleNode("L")
    right = SimpleNode("R")
    root = SimpleNode("P", left=left, right=right)
    output = render_tree(root)
    # When both children exist the root line ends with ┤
    assert "┤" in output
    # Left child starts with ┌
    assert "┌" in output
    # Right child starts with └
    assert "└" in output


def test_render_left_only_uses_bottom_end():
    left = SimpleNode("L")
    root = SimpleNode("P", left=left)
    output = render_tree(root)
    # Only left child → end_shape is ┘
    assert "┘" in output


def test_render_right_only_uses_top_end():
    right = SimpleNode("R")
    root = SimpleNode("P", right=right)
    output = render_tree(root)
    # Only right child → end_shape is ┐
    assert "┐" in output


# ---------------------------------------------------------------------------
# External StringIO buffer
# ---------------------------------------------------------------------------


def test_render_tree_with_external_buffer():
    import io

    buf = io.StringIO()
    node = SimpleNode("buf_test")
    result = render_tree(node, buffer=buf)
    assert "buf_test" in result
    assert "buf_test" in buf.getvalue()


# ---------------------------------------------------------------------------
# Multiline output
# ---------------------------------------------------------------------------


def test_intermediate_sibling_marker():
    """The ├ marker appears when render_tree is called with a non-standard
    `last` parameter (e.g. '' or 'other'), triggering the else branch."""
    node = SimpleNode("N")
    output = render_tree(node, last="other")
    assert "├" in output


def test_output_is_multiline_for_nontrivial_trees():
    left = SimpleNode("L")
    right = SimpleNode("R")
    root = SimpleNode("P", left=left, right=right)
    output = render_tree(root)
    lines = [ln for ln in output.strip().split("\n") if ln.strip()]
    assert len(lines) >= 3  # at least root + 2 children
