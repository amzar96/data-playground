import csv
import uuid
from pathlib import Path
from typing import Annotated

import typer
from faker import Faker

app = typer.Typer()
fake = Faker()

FIELD_GENERATORS: dict[str, object] = {
    "name": lambda: fake.name(),
    "email": lambda: fake.email(),
    "address": lambda: fake.address().replace("\n", ", "),
    "city": lambda: fake.city(),
    "country": lambda: fake.country(),
    "phone": lambda: fake.phone_number(),
    "date": lambda: fake.date(),
    "datetime": lambda: fake.date_time().isoformat(),
    "uuid": lambda: str(uuid.uuid4()),
    "int": lambda: fake.random_int(min=1, max=100_000),
    "float": lambda: round(fake.pyfloat(min_value=0, max_value=10_000), 4),
    "str": lambda: fake.word(),
    "text": lambda: fake.text(max_nb_chars=200),
    "bool": lambda: fake.boolean(),
    "company": lambda: fake.company(),
    "price": lambda: round(fake.pyfloat(min_value=0.01, max_value=9999.99), 2),
}


def _parse_field(spec: str) -> tuple[str, str]:
    parts = spec.split(":", 1)
    if len(parts) != 2:
        raise typer.BadParameter(f"Field must be 'name:type', got: {spec!r}")
    name, ftype = parts
    if ftype not in FIELD_GENERATORS:
        valid = ", ".join(sorted(FIELD_GENERATORS))
        raise typer.BadParameter(f"Unknown type {ftype!r}. Valid types: {valid}")
    return name, ftype


@app.command()
def generate(
    fields: Annotated[
        list[str],
        typer.Argument(help="Field definitions as 'name:type' pairs (e.g. id:uuid price:price)"),
    ],
    records: Annotated[int, typer.Option("--records", "-n", help="Number of rows")] = 1000,
    output: Annotated[
        Path,
        typer.Option("--output", "-o", help="Output CSV file path"),
    ] = Path("data/sample.csv"),
) -> None:
    parsed = [_parse_field(f) for f in fields]
    output.parent.mkdir(parents=True, exist_ok=True)

    with output.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=[name for name, _ in parsed])
        writer.writeheader()
        for _ in range(records):
            writer.writerow({name: FIELD_GENERATORS[ftype]() for name, ftype in parsed})

    typer.echo(f"Generated {records:,} records → {output}")


if __name__ == "__main__":
    app()
