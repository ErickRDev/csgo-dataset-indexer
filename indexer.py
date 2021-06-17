import json
import os
import sqlite3
import sys

from lookups import TEAMS
from pathlib import Path


def index_dataset(dataset_path: str):
    """
    The dataset directory path.

    Dataset hierarchy structre:

    /<event_id>
    --/<match_name>
    ------/<match_map_name>
    ----------<bomb_lifecycle.parquet>
    ----------<player_death.parquet>
    ----------<tick.parquet>
    ----------<utility_parquet.parquet>
    ----------<weapon_fire.parquet>
    """
    db_fp = os.path.join(dataset_path, "index.db")
    print(db_fp)
    conn = sqlite3.connect(db_fp)

    cursor = conn.cursor()

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events
        (
            id INTEGER PRIMARY KEY,
            url TEXT,
            name TEXT,
            date TEXT,
            teams_amount INTEGER,
            path_on_fs TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS teams
        (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        )
        """
    )

    for team_idx, team in enumerate(TEAMS):
        conn.execute(
            f"""
            INSERT INTO teams VALUES ({team_idx}, '{team}')
            ON CONFLICT DO NOTHING;
            """
        )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS players
        (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS matches
        (
            id INTEGER PRIMARY KEY,
            event_id INTEGER,
            url TEXT,
            demo_url TEXT,
            date TEXT,
            path_on_fs TEXT,
            FOREIGN KEY(event_id) REFERENCES events(id)
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS match_teams
        (
            match_id INTEGER,
            team_id TEXT,
            PRIMARY KEY(match_id, team_id),
            FOREIGN KEY(match_id) REFERENCES matches(id),
            FOREIGN KEY(team_id) REFERENCES teams(id)
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS maps
        (
            match_id INTEGER,
            map_name TEXT,
            path_on_fs TEXT,
            PRIMARY KEY(match_id, map_name),
            FOREIGN KEY(match_id) REFERENCES matches(id)
        )
        """
    )

    for d in os.listdir(dataset_path):
        if os.path.isfile(d):
            continue

        event_id = d
        print(f"Indexing event {event_id}")

        event_path_on_fs = os.path.join(dataset_path, event_id)
        manifest_path_on_fs = os.path.join(event_path_on_fs, "manifest.json")

        if not os.path.isfile(manifest_path_on_fs):
            print("\tMissing manifest file, skipping")
            continue

        with open(manifest_path_on_fs, "r") as f:
            manifest = json.load(f)

            cursor.execute(
                f"""
                INSERT INTO events VALUES
                (
                    {manifest["event_id"]},
                    '{manifest["event_url"]}',
                    '{manifest["event_name"]}',
                    '{manifest["date"]}',
                    {manifest["teams_amount"]},
                    '{event_path_on_fs}'
                ) ON CONFLICT DO NOTHING;
                """
            )

            for match in manifest["matches"]:
                match_title = match["match_url"].split("/")[-1]
                match_path_on_fs = os.path.join(event_path_on_fs, match_title)

                cursor.execute(
                    f"""
                    INSERT INTO matches VALUES
                    (
                        {match["match_id"]},
                        {manifest["event_id"]},
                        '{match["match_url"]}',
                        '{match["gotv_demo_url"]}',
                        '{match["match_date"]}',
                        '{match_path_on_fs}'
                    ) ON CONFLICT DO NOTHING;
                    """
                )

                for team in match["lineups"].keys():
                    cursor.execute(
                        f"""
                        INSERT INTO match_teams VALUES
                        (
                            {match["match_id"]},
                            '{TEAMS.index(team)}'
                        ) ON CONFLICT DO NOTHING;
                        """
                    )

                for map_name in map(lambda x: x["map"], match["maps"]):
                    try:
                        map_path_on_fs = list(
                            Path(match_path_on_fs).glob(f"*{map_name}")
                        )[0]
                        cursor.execute(
                            f"""
                            INSERT INTO maps VALUES
                            (
                                {match["match_id"]},
                                '{map_name}',
                                '{map_path_on_fs}'
                            ) ON CONFLICT DO NOTHING;
                            """
                        )
                    except Exception as _:
                        print(f"Missing map {map_name} in {match_path_on_fs}")
                        pass

        conn.commit()
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print(f"Invalid arguments: dataset directory path expected")
        sys.exit(0)

    dataset_path = sys.argv[1]

    index_dataset(dataset_path)
