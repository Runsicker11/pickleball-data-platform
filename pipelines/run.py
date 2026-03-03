"""
CLI entry point: python -m pipelines.run

Usage:
    python -m pipelines.run amazon-ads --days 7
    python -m pipelines.run amazon-ads --days 7 --destination duckdb
    python -m pipelines.run amazon-ads --days 7 --reports campaigns
    python -m pipelines.run amazon-seller --days 30
    python -m pipelines.run amazon-seller --days 30 --destination duckdb
"""

import argparse
import logging


def main():
    parser = argparse.ArgumentParser(description="Pickleball Data Platform — pipeline runner")
    subparsers = parser.add_subparsers(dest="pipeline", required=True)

    # ── amazon-ads ───────────────────────────────────────────────────
    aa_parser = subparsers.add_parser("amazon-ads", help="Run Amazon Ads pipeline")
    aa_parser.add_argument(
        "--days", type=int, default=7, help="Days of history to pull (default: 7)"
    )
    aa_parser.add_argument(
        "--destination",
        choices=["bigquery", "duckdb"],
        default="bigquery",
        help="Load destination (default: bigquery)",
    )
    aa_parser.add_argument(
        "--dataset", default="raw_amazon", help="Target dataset name (default: raw_amazon)"
    )
    aa_parser.add_argument(
        "--reports",
        choices=["all", "campaigns", "asin"],
        default="all",
        help="Which report subset to run (default: all)",
    )

    # ── amazon-seller ─────────────────────────────────────────────────
    as_parser = subparsers.add_parser("amazon-seller", help="Run Amazon Seller Central pipeline")
    as_parser.add_argument(
        "--days", type=int, default=30, help="Days of order history to pull (default: 30)"
    )
    as_parser.add_argument(
        "--destination",
        choices=["bigquery", "duckdb"],
        default="bigquery",
        help="Load destination (default: bigquery)",
    )
    as_parser.add_argument(
        "--dataset", default="raw_amazon", help="Target dataset name (default: raw_amazon)"
    )

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.pipeline == "amazon-ads":
        from .amazon_ads.pipeline import run_pipeline
        from .amazon_ads.report_configs import ALL_REPORTS, ASIN_REPORTS, CAMPAIGN_REPORTS

        report_map = {
            "all": ALL_REPORTS,
            "campaigns": CAMPAIGN_REPORTS,
            "asin": ASIN_REPORTS,
        }

        load_info = run_pipeline(
            destination=args.destination,
            dataset_name=args.dataset,
            days_back=args.days,
            reports=report_map[args.reports],
        )
        print(f"\nPipeline finished. Load info:\n{load_info}")

    elif args.pipeline == "amazon-seller":
        from .amazon_seller.pipeline import run_pipeline

        load_info = run_pipeline(
            destination=args.destination,
            dataset_name=args.dataset,
            days_back=args.days,
        )
        print(f"\nPipeline finished. Load info:\n{load_info}")


if __name__ == "__main__":
    main()
