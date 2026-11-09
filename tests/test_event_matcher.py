
import pytest

from backend.event_matcher import (
    EventTemplate,
    compile_template,
    load_event_templates,
    match_event_template,
)


def test_template_matches_variable_text():
    template = "[*]Receiving block[*]src:[*]dest:[*]"
    pattern = compile_template(template)

    message = (
        "Receiving block blk_123 "
        "src: /10.0.0.1:50000 "
        "dest: /10.0.0.2:50010"
    )

    assert pattern.fullmatch(message) is not None


def test_load_event_templates_from_csv(tmp_path):
    csv_file = tmp_path / "templates.csv"

    csv_file.write_text(
        "EventId,EventTemplate\n"
        "E5,[*]Receiving block[*]src:[*]dest:[*]\n"
        "E6,[*]Received block[*]of size[*]from[*]\n",
        encoding="utf-8",
    )

    templates = load_event_templates(csv_file)

    assert len(templates) == 2
    assert templates[0].event_id == "E5"
    assert templates[1].event_id == "E6"


def test_known_message_returns_correct_event_id():
    template_text = "[*]Receiving block[*]src:[*]dest:[*]"

    templates = [
        EventTemplate(
            event_id="E5",
            template=template_text,
            pattern=compile_template(template_text),
        )
    ]

    message = (
        "Receiving block blk_123 "
        "src: /10.0.0.1:50000 "
        "dest: /10.0.0.2:50010"
    )

    assert match_event_template(message, templates) == "E5"


def test_unmatched_message_returns_none():
    templates = [
        EventTemplate(
            event_id="E5",
            template="[*]Receiving block[*]src:[*]dest:[*]",
            pattern=compile_template(
                "[*]Receiving block[*]src:[*]dest:[*]"
            ),
        )
    ]

    message = "Some completely unknown Hadoop event"

    assert match_event_template(message, templates) is None


def test_ambiguous_match_raises_error():
    template_text = "[*]Receiving block[*]"

    templates = [
        EventTemplate(
            event_id="E5",
            template=template_text,
            pattern=compile_template(template_text),
        ),
        EventTemplate(
            event_id="E99",
            template=template_text,
            pattern=compile_template(template_text),
        ),
    ]

    with pytest.raises(ValueError, match="Ambiguous"):
        match_event_template("Receiving block blk_123", templates)


def test_duplicate_event_ids_are_rejected(tmp_path):
    csv_file = tmp_path / "duplicate_templates.csv"

    csv_file.write_text(
        "EventId,EventTemplate\n"
        "E5,[*]Receiving block[*]\n"
        "E5,[*]Deleting block[*]\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate"):
        load_event_templates(csv_file)
