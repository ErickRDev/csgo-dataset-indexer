import json
import os
import sys

from pathlib import Path


def extract_distinct_teams(manifest_fp: Path):
    """
    Extracts a list of all distinct strings used as teams in a manifest file.
    """
    with open(manifest_fp, "r") as f:
        manifest = json.load(f)

        teams = set()

        for match in manifest["matches"]:
            for lineup in match["lineups"].keys():
                teams.add(lineup)

        return sorted(list(teams))


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print(f"Invalid arguments: dataset directory path expected")
        sys.exit(0)

    manifest_fp = sys.argv[1]

    if not os.path.isfile(manifest_fp):
        print("Invalid arguments: invalid manifest file path")
        sys.exit(0)

    teams = extract_distinct_teams(manifest_fp)
    print(teams)
