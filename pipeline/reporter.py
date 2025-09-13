# reporter.py
"""
Reporter: compile un résumé lisible de l'état actuel.
Standardised entrypoint: run(conn).
"""
import sqlite3
import logging
from datetime import datetime, timezone

logger = logging.getLogger("pipeline.reporter")


def render_report(conn: sqlite3.Connection) -> str:
    # ... (code inchangé, juste la logique de génération du rapport)
    # => garder ton implémentation existante telle quelle.
    ...


def run(conn: sqlite3.Connection):
    """Standardised entrypoint."""
    report = render_report(conn)
    print(report)
    return report


def main():
    conn = sqlite3.connect("data/crypto.db")
    run(conn)
    conn.close()


if __name__ == "__main__":
    main()
