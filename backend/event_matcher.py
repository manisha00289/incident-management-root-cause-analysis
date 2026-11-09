
import csv
import re
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class EventTemplate:
    event_id: str
    template: str
    pattern: re.Pattern


def compile_template(template: str) -> re.Pattern:
    """
    Convert LogHub [*] wildcards into regular expressions.

    Example:
    [*]Receiving block[*]src:[*]

    becomes a pattern that matches variable text
    around the required phrases.
    """

    parts = template.split("[*]")

    regex = ".*?".join(
        re.escape(part) for part in parts
    )

    return re.compile(f"^{regex}$")


def load_event_templates(
    template_file: str | Path
) -> list[EventTemplate]:
    """
    Load HDFS event templates from the LogHub CSV file.
    """

    path = Path(template_file)

    if not path.is_file():
        raise FileNotFoundError(
            f"Event template file not found: {path}"
        )

    templates = []

    with path.open(
        "r",
        newline="",
        encoding="utf-8"
    ) as file:

        reader = csv.DictReader(file)

        for row in reader:
            event_id = row["EventId"].strip()
            template = row["EventTemplate"].strip()

            templates.append(
                EventTemplate(
                    event_id=event_id,
                    template=template,
                    pattern=compile_template(template)
                )
            )

    if not templates:
        raise ValueError("No event templates were loaded.")

    event_ids = [item.event_id for item in templates]

    if len(event_ids) != len(set(event_ids)):
        raise ValueError("Duplicate event IDs found.")

    return templates


def match_event_template(
    message: str,
    templates: list[EventTemplate]
) -> str | None:
    """
    Match a raw HDFS message against the event catalog.

    Returns:
        Event ID if exactly one template matches.
        None if no template matches.

    Raises:
        ValueError if multiple templates match.
    """

    matches = [
        template.event_id
        for template in templates
        if template.pattern.fullmatch(message)
    ]

    if len(matches) > 1:
        raise ValueError(
            f"Ambiguous event template match: {matches}"
        )

    if not matches:
        return None

    return matches[0]
