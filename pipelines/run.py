"""
CLI entry point: python -m pipelines.run

Usage:
    python -m pipelines.run amazon-ads --days 7
    python -m pipelines.run amazon-ads --days 7 --destination duckdb
    python -m pipelines.run amazon-ads --days 7 --reports campaigns
    python -m pipelines.run amazon-seller --days 30
    python -m pipelines.run amazon-seller --days 30 --destination duckdb
    python -m pipelines.run shopify --days 3
    python -m pipelines.run meta-ads --days 3
    python -m pipelines.run google-ads --days 3
    python -m pipelines.run search-console --days 7
    python -m pipelines.run quickbooks --days 90
    python -m pipelines.run paypal --days 365
    python -m pipelines.run google-trends
    python -m pipelines.run merchant-center
    python -m pipelines.run klaviyo --days 7
    python -m pipelines.run youtube --days 30
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

    # ── shopify ──────────────────────────────────────────────────────
    sh_parser = subparsers.add_parser("shopify", help="Run Shopify pipeline")
    sh_parser.add_argument(
        "--days", type=int, default=3, help="Days of order history to pull (default: 3)"
    )
    sh_parser.add_argument(
        "--destination",
        choices=["bigquery", "duckdb"],
        default="bigquery",
        help="Load destination (default: bigquery)",
    )
    sh_parser.add_argument(
        "--dataset", default="raw_shopify", help="Target dataset name (default: raw_shopify)"
    )

    # ── meta-ads ────────────────────────────────────────────────────
    ma_parser = subparsers.add_parser("meta-ads", help="Run Meta Ads pipeline")
    ma_parser.add_argument(
        "--days", type=int, default=3, help="Days of insights to pull (default: 3)"
    )
    ma_parser.add_argument(
        "--destination",
        choices=["bigquery", "duckdb"],
        default="bigquery",
        help="Load destination (default: bigquery)",
    )
    ma_parser.add_argument(
        "--dataset", default="raw_meta", help="Target dataset name (default: raw_meta)"
    )

    # ── google-ads ──────────────────────────────────────────────────
    ga_parser = subparsers.add_parser("google-ads", help="Run Google Ads pipeline")
    ga_parser.add_argument(
        "--days", type=int, default=3, help="Days of insights to pull (default: 3)"
    )
    ga_parser.add_argument(
        "--destination",
        choices=["bigquery", "duckdb"],
        default="bigquery",
        help="Load destination (default: bigquery)",
    )
    ga_parser.add_argument(
        "--dataset", default="raw_google_ads",
        help="Target dataset name (default: raw_google_ads)",
    )

    # ── search-console ──────────────────────────────────────────────
    sc_parser = subparsers.add_parser("search-console", help="Run Search Console pipeline")
    sc_parser.add_argument(
        "--days", type=int, default=7, help="Days of data to pull (default: 7)"
    )
    sc_parser.add_argument(
        "--destination",
        choices=["bigquery", "duckdb"],
        default="bigquery",
        help="Load destination (default: bigquery)",
    )
    sc_parser.add_argument(
        "--dataset", default="raw_search_console",
        help="Target dataset name (default: raw_search_console)",
    )

    # ── quickbooks ─────────────────────────────────────────────────
    qb_parser = subparsers.add_parser("quickbooks", help="Run QuickBooks pipeline")
    qb_parser.add_argument(
        "--days", type=int, default=90, help="Days of history to pull (default: 90)"
    )
    qb_parser.add_argument(
        "--destination",
        choices=["bigquery", "duckdb"],
        default="bigquery",
        help="Load destination (default: bigquery)",
    )
    qb_parser.add_argument(
        "--dataset", default="raw_quickbooks",
        help="Target dataset name (default: raw_quickbooks)",
    )

    # ── paypal ──────────────────────────────────────────────────
    pp_parser = subparsers.add_parser("paypal", help="Run PayPal pipeline")
    pp_parser.add_argument(
        "--days", type=int, default=365, help="Days of history to pull (default: 365)"
    )
    pp_parser.add_argument(
        "--destination",
        choices=["bigquery", "duckdb"],
        default="bigquery",
        help="Load destination (default: bigquery)",
    )
    pp_parser.add_argument(
        "--dataset", default="raw_paypal", help="Target dataset name (default: raw_paypal)"
    )

    # ── google-trends ───────────────────────────────────────────
    gt_parser = subparsers.add_parser("google-trends", help="Run Google Trends pipeline (weekly, 5-year history)")
    gt_parser.add_argument(
        "--destination",
        choices=["bigquery", "duckdb"],
        default="bigquery",
        help="Load destination (default: bigquery)",
    )
    gt_parser.add_argument(
        "--dataset", default="raw_google_trends",
        help="Target dataset name (default: raw_google_trends)",
    )

    # ── merchant-center ─────────────────────────────────────────
    mc_parser = subparsers.add_parser("merchant-center", help="Run Merchant Center pipeline")
    mc_parser.add_argument(
        "--destination",
        choices=["bigquery", "duckdb"],
        default="bigquery",
        help="Load destination (default: bigquery)",
    )
    mc_parser.add_argument(
        "--dataset", default="raw_merchant_center",
        help="Target dataset name (default: raw_merchant_center)",
    )

    # ── klaviyo ─────────────────────────────────────────────────
    kl_parser = subparsers.add_parser("klaviyo", help="Run Klaviyo email pipeline")
    kl_parser.add_argument(
        "--days", type=int, default=7, help="Days of history to pull (default: 7)"
    )
    kl_parser.add_argument(
        "--destination",
        choices=["bigquery", "duckdb"],
        default="bigquery",
        help="Load destination (default: bigquery)",
    )
    kl_parser.add_argument(
        "--dataset", default="raw_klaviyo", help="Target dataset name (default: raw_klaviyo)"
    )
    kl_parser.add_argument(
        "--full-sync", action="store_true",
        help="Pull all profiles since account creation (backfill, sets days=2000)"
    )

    # ── youtube ─────────────────────────────────────────────────
    yt_parser = subparsers.add_parser("youtube", help="Run YouTube pipeline")
    yt_parser.add_argument(
        "--days", type=int, default=30, help="Days of analytics history to pull (default: 30)"
    )
    yt_parser.add_argument(
        "--destination",
        choices=["bigquery", "duckdb"],
        default="bigquery",
        help="Load destination (default: bigquery)",
    )
    yt_parser.add_argument(
        "--dataset", default="raw_youtube", help="Target dataset name (default: raw_youtube)"
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

    elif args.pipeline == "shopify":
        from .shopify.pipeline import run_pipeline

        load_info = run_pipeline(
            destination=args.destination,
            dataset_name=args.dataset,
            days_back=args.days,
        )
        print(f"\nPipeline finished. Load info:\n{load_info}")

    elif args.pipeline == "meta-ads":
        from .meta_ads.pipeline import run_pipeline

        load_info = run_pipeline(
            destination=args.destination,
            dataset_name=args.dataset,
            days_back=args.days,
        )
        print(f"\nPipeline finished. Load info:\n{load_info}")

    elif args.pipeline == "google-ads":
        from .google_ads.pipeline import run_pipeline

        load_info = run_pipeline(
            destination=args.destination,
            dataset_name=args.dataset,
            days_back=args.days,
        )
        print(f"\nPipeline finished. Load info:\n{load_info}")

    elif args.pipeline == "search-console":
        from .search_console.pipeline import run_pipeline

        load_info = run_pipeline(
            destination=args.destination,
            dataset_name=args.dataset,
            days_back=args.days,
        )
        print(f"\nPipeline finished. Load info:\n{load_info}")

    elif args.pipeline == "quickbooks":
        from .quickbooks.pipeline import run_pipeline

        load_info = run_pipeline(
            destination=args.destination,
            dataset_name=args.dataset,
            days_back=args.days,
        )
        print(f"\nPipeline finished. Load info:\n{load_info}")

    elif args.pipeline == "paypal":
        from .paypal.pipeline import run_pipeline

        load_info = run_pipeline(
            destination=args.destination,
            dataset_name=args.dataset,
            days_back=args.days,
        )
        print(f"\nPipeline finished. Load info:\n{load_info}")

    elif args.pipeline == "google-trends":
        from .google_trends.pipeline import run_pipeline

        load_info = run_pipeline(
            destination=args.destination,
            dataset_name=args.dataset,
        )
        print(f"\nPipeline finished. Load info:\n{load_info}")

    elif args.pipeline == "merchant-center":
        from .merchant_center.pipeline import run_pipeline

        load_info = run_pipeline(
            destination=args.destination,
            dataset_name=args.dataset,
        )
        print(f"\nPipeline finished. Load info:\n{load_info}")

    elif args.pipeline == "klaviyo":
        from .klaviyo.pipeline import run_pipeline

        load_info = run_pipeline(
            destination=args.destination,
            dataset_name=args.dataset,
            days_back=None if args.full_sync else args.days,
            full_profile_sync=args.full_sync,
        )
        print(f"\nPipeline finished. Load info:\n{load_info}")

    elif args.pipeline == "youtube":
        from .youtube.pipeline import run_pipeline

        load_info = run_pipeline(
            destination=args.destination,
            dataset_name=args.dataset,
            days_back=args.days,
        )
        print(f"\nPipeline finished. Load info:\n{load_info}")


if __name__ == "__main__":
    main()
