"""CSCS HPC Storage synchronization script.

This script fetches all storage resources from Waldur API and generates
the complete all.json file. It should be run separately from individual
order processing to efficiently handle bulk resource synchronization.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from waldur_cscs_hpc_storage.utils import load_configuration
from waldur_cscs_hpc_storage.backend import CscsHpcStorageBackend
from waldur_cscs_hpc_storage.waldur_storage_proxy.config import (
    HpcUserApiConfig,
    WaldurApiConfig,
)

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False) -> None:
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def sync_offering_resources(offering_config: dict, dry_run: bool = False) -> bool:
    """Sync resources for a single offering.

    Args:
        offering_config: Configuration dictionary for the offering
        dry_run: If True, don't write files, just log what would be done

    Returns:
        True if successful, False otherwise
    """
    offering_name = offering_config.get("name", "Unknown")
    offering_uuid = offering_config.get("waldur_offering_uuid")
    backend_type = offering_config.get("backend_type")

    if backend_type != "cscs-hpc-storage":
        logger.debug(
            "Skipping offering %s (backend_type: %s)", offering_name, backend_type
        )
        return True

    if not offering_uuid:
        logger.error("No offering UUID configured for %s", offering_name)
        return False

    logger.info("Syncing resources for offering: %s", offering_name)

    try:
        # Prepare Waldur API settings
        # Prepare Waldur API settings
        waldur_api_settings = WaldurApiConfig(
            api_url=offering_config["waldur_api_url"],
            access_token=offering_config["waldur_api_token"],
        )

        # Initialize backend
        backend_settings = offering_config.get("backend_settings", {})
        backend_components = offering_config.get("backend_components", {})

        # Extract HPC User API settings if present (legacy support)
        hpc_user_api_settings = None
        if "hpc_user_api_url" in backend_settings:
            hpc_user_api_settings = HpcUserApiConfig(
                api_url=backend_settings.get("hpc_user_api_url"),
                client_id=backend_settings.get("hpc_user_client_id"),
                client_secret=backend_settings.get("hpc_user_client_secret"),
                oidc_token_url=backend_settings.get("hpc_user_oidc_token_url"),
                oidc_scope=backend_settings.get("hpc_user_oidc_scope"),
                socks_proxy=backend_settings.get("hpc_user_socks_proxy"),
            )

        backend = CscsHpcStorageBackend(
            backend_settings,
            backend_components,
            hpc_user_api_settings=hpc_user_api_settings,
            waldur_api_settings=waldur_api_settings,
        )

        if dry_run:
            logger.info(
                "DRY RUN: Would generate all.json for offering %s", offering_name
            )
            # Still fetch resources to validate API access
            resources, _ = backend._get_all_storage_resources(offering_uuid)
            logger.info("DRY RUN: Would write %d resources to all.json", len(resources))
        else:
            # Generate the all.json file
            backend.generate_all_resources_json(offering_uuid)
            logger.info(
                "Successfully generated all.json for offering %s", offering_name
            )

        return True

    except Exception:
        logger.exception("Failed to sync offering %s", offering_name)
        return False


def main() -> None:
    """Main entry point for the sync script."""
    parser = argparse.ArgumentParser(
        description="Sync CSCS HPC Storage resources and generate all.json files"
    )
    parser.add_argument(
        "-c", "--config", required=True, help="Path to the configuration file"
    )
    parser.add_argument("--offering-uuid", help="Sync only the offering with this UUID")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write files, just show what would be done",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    # Initialize Sentry if configured
    from waldur_cscs_hpc_storage.sentry_config import initialize_sentry

    sentry_dsn = os.getenv("SENTRY_DSN")
    if sentry_dsn:
        try:
            initialize_sentry(
                dsn=sentry_dsn,
                environment=os.getenv("SENTRY_ENVIRONMENT"),
                traces_sample_rate=float(os.getenv("SENTRY_TRACES_SAMPLE_RATE", "0.1")),
                release="waldur-cscs-hpc-storage-sync",
            )
        except Exception as e:
            logger.error("Failed to initialize Sentry: %s", e)
            # Continue without Sentry

    # Load configuration
    config_path = Path(args.config)
    if not config_path.exists():
        logger.error("Configuration file not found: %s", config_path)
        sys.exit(1)

    try:
        configuration = load_configuration(str(config_path), user_agent_suffix="sync")
    except Exception:
        logger.exception("Failed to load configuration")
        sys.exit(1)

    # Filter offerings if specific UUID provided
    offerings_to_sync = []
    offering_list = configuration.get("offerings", [])

    for offering_info in offering_list:
        # Only process cscs-hpc-storage offerings
        if offering_info.get("backend_type") != "cscs-hpc-storage":
            continue

        offering_dict = {
            "name": offering_info.get("name"),
            "waldur_api_url": offering_info.get("waldur_api_url"),
            "waldur_api_token": offering_info.get("waldur_api_token"),
            "waldur_offering_uuid": offering_info.get("waldur_offering_uuid"),
            "backend_type": offering_info.get("backend_type"),
            "backend_settings": offering_info.get("backend_settings", {}),
            "backend_components": offering_info.get("backend_components", {}),
        }

        if args.offering_uuid:
            if offering_info.get("waldur_offering_uuid") == args.offering_uuid:
                offerings_to_sync.append(offering_dict)
                break
        else:
            offerings_to_sync.append(offering_dict)

    if not offerings_to_sync:
        if args.offering_uuid:
            logger.error("No offering found with UUID: %s", args.offering_uuid)
        else:
            logger.error("No offerings configured")
        sys.exit(1)

    logger.info("Starting sync for %d offering(s)", len(offerings_to_sync))

    # Sync each offering
    success_count = 0
    for offering_config in offerings_to_sync:
        if sync_offering_resources(offering_config, args.dry_run):
            success_count += 1

    logger.info(
        "Sync completed: %d/%d offerings successful",
        success_count,
        len(offerings_to_sync),
    )

    if success_count != len(offerings_to_sync):
        sys.exit(1)


if __name__ == "__main__":
    main()
