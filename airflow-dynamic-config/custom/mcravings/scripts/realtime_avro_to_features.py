#!/usr/bin/env python3
"""Example: convert recent Empatica AVRO files to a feature matrix."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from mcraving.realtime import avro_files_to_feature_matrix


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "avro_files",
        nargs="+",
        type=Path,
        help="Newest AVRO plus any available preceding AVRO files.",
    )
    parser.add_argument(
        "--latest",
        type=Path,
        help="The newest AVRO file. Defaults to the final positional path.",
    )
    parser.add_argument("--participant-id")
    parser.add_argument(
        "--sociodemographics",
        type=Path,
        help="Optional sociodemographics Excel file.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("realtime_features.parquet"),
    )
    parser.add_argument(
        "--drop-insufficient",
        action="store_true",
        help="Write only windows meeting the default 80% sensor coverage threshold.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    sociodemographics = (
        pd.read_excel(args.sociodemographics)
        if args.sociodemographics
        else None
    )
    features = avro_files_to_feature_matrix(
        args.avro_files,
        latest_avro_file=args.latest or args.avro_files[-1],
        participant_id=args.participant_id,
        sociodemographics=sociodemographics,
        drop_insufficient=args.drop_insufficient,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    features.to_parquet(args.output, index=False)
    print(f"Wrote {len(features)} feature rows to {args.output}")
    if not features.empty:
        print(
            features[
                [
                    "window_start",
                    "window_end",
                    "minimum_sensor_usable_fraction",
                    "sufficient_data",
                ]
            ].to_string(index=False)
        )


if __name__ == "__main__":
    main()
