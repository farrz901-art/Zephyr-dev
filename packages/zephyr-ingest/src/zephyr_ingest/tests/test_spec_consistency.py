from __future__ import annotations

import argparse
from typing import Any, cast

from zephyr_ingest import cli
from zephyr_ingest.config.constants import UNS_API_KEY_ENV_NAMES, WEAVIATE_API_KEY_ENV_NAMES
from zephyr_ingest.spec.registry import get_spec, list_spec_ids


def _get_subparser(
    *, root: argparse.ArgumentParser, dest: str, name: str
) -> argparse.ArgumentParser:
    """
    Find a subparser by looking for an action with .choices and matching dest.
    Avoid relying on argparse private classes (pyright strict-friendly).
    """
    for act in root._actions:  # noqa: SLF001 (argparse internal; acceptable in tests)
        if getattr(act, "dest", None) != dest:
            continue
        choices = getattr(act, "choices", None)
        if isinstance(choices, dict):
            choices_dict: dict[str, Any] = cast(dict[str, Any], choices)
            sp = choices_dict.get(name)
            if isinstance(sp, argparse.ArgumentParser):
                return sp
    raise AssertionError(f"subparser not found: dest={dest} name={name}")


def _flag_to_action(p: argparse.ArgumentParser) -> dict[str, argparse.Action]:
    out: dict[str, argparse.Action] = {}
    for act in p._actions:  # noqa: SLF001
        for s in getattr(act, "option_strings", []):
            if isinstance(s, str) and s.startswith("--"):
                out[s] = act
    return out


def test_spec_cli_flags_exist_in_run_parser() -> None:
    root = cli.build_parser()  # noqa: SLF001
    run_p = _get_subparser(root=root, dest="cmd", name="run")
    flag_map = _flag_to_action(run_p)

    for spec_id in list_spec_ids():
        spec = get_spec(spec_id=spec_id)
        assert spec is not None
        for f in spec["fields"]:
            for flag in f.get("cli_flags", []):
                assert flag in flag_map, f"spec {spec_id} references missing cli flag {flag}"


def test_spec_env_names_match_constants() -> None:
    s = get_spec(spec_id="backend.uns_api.v1")
    assert s is not None
    api_field = next(f for f in s["fields"] if f["name"] == "backend.api_key")
    assert api_field.get("env_names") == list(UNS_API_KEY_ENV_NAMES)

    s2 = get_spec(spec_id="destination.weaviate.v1")
    assert s2 is not None
    wv_field = next(f for f in s2["fields"] if f["name"] == "destinations.weaviate.api_key")
    assert wv_field.get("env_names") == list(WEAVIATE_API_KEY_ENV_NAMES)


def test_spec_defaults_match_argparse_defaults() -> None:
    root = cli.build_parser()  # noqa: SLF001
    run_p = _get_subparser(root=root, dest="cmd", name="run")
    flag_map = _flag_to_action(run_p)

    for spec_id in list_spec_ids():
        spec = get_spec(spec_id=spec_id)
        assert spec is not None
        for f in spec["fields"]:
            if "default" not in f:
                continue
            cli_flags = f.get("cli_flags", [])
            if not cli_flags:
                continue
            flag = cli_flags[0]
            act = flag_map[flag]
            assert act.default == f["default"], (
                f"default drift for {spec_id}:{f['name']} via {flag}"
            )


def test_spec_choices_match_argparse_choices_when_provided() -> None:
    root = cli.build_parser()  # noqa: SLF001
    run_p = _get_subparser(root=root, dest="cmd", name="run")
    flag_map = _flag_to_action(run_p)

    for spec_id in list_spec_ids():
        spec = get_spec(spec_id=spec_id)
        assert spec is not None
        for f in spec["fields"]:
            if "choices" not in f:
                continue
            cli_flags = f.get("cli_flags", [])
            if not cli_flags:
                continue
            flag = cli_flags[0]
            act = flag_map[flag]
            assert act.choices is not None, (
                f"choices missing for {flag} (spec {spec_id}:{f['name']})"
            )
            assert list(act.choices) == f["choices"], f"choices drift for {flag}"


def test_spec_help_matches_argparse_help_for_documented_fields() -> None:
    """
    Enforce help consistency only when the spec provides `help`
    and the argparse action also has help.
    """
    root = cli._build_parser()  # pyright: ignore[reportPrivateUsage] # noqa: SLF001
    run_p = _get_subparser(root=root, dest="cmd", name="run")
    flag_map = _flag_to_action(run_p)

    for spec_id in list_spec_ids():
        spec = get_spec(spec_id=spec_id)
        assert spec is not None
        for f in spec["fields"]:
            help_text = f.get("help")
            if help_text is None:
                continue
            cli_flags = f.get("cli_flags", [])
            if not cli_flags:
                continue
            flag = cli_flags[0]
            act = flag_map[flag]
            # If act.help is None, we treat that as drift: spec says it should be documented.
            assert act.help is not None, (
                f"argparse help missing for {flag} (spec {spec_id}:{f['name']})"
            )
            assert act.help == help_text, f"help drift for {flag}: '{act.help}' != '{help_text}'"
