from datetime import datetime, timedelta, timezone
from decimal import Decimal

from dagster import Config, ResourceParam, get_dagster_logger, job, op
from dagster_open_platform.defs.maxio.py.resources import (
    MaxioResource,
    format_amount,
    needs_correction,
)
from dagster_slack import SlackResource

log = get_dagster_logger()

DEFAULT_CONTRACTED_VALUE_ITEM_IDS = [9428, 9431]


class ArrCorrectionConfig(Config):
    lookback_days: int = 180
    dry_run: bool = False
    contracted_value_item_ids: list[int] = DEFAULT_CONTRACTED_VALUE_ITEM_IDS
    test_transaction_id: int | None = None


SLACK_CHANNEL = "C03EPUD3T7Y"


@op
def correct_maxio_arr_amounts(
    config: ArrCorrectionConfig,
    maxio: ResourceParam[MaxioResource],
    slack: ResourceParam[SlackResource],
) -> None:
    if config.dry_run:
        log.info("*** DRY RUN MODE -- no changes will be written to Maxio ***")

    contracted_ids = set(config.contracted_value_item_ids)
    if not contracted_ids:
        log.warning("No contracted_value_item_ids configured -- nothing to correct.")
        return

    log.info("Contracted-value item IDs: %s", contracted_ids)

    since = datetime.now(timezone.utc) - timedelta(days=config.lookback_days)
    created_gte = since.strftime("%Y-%m-%dT%H:%M:%S.000000")
    log.info("Looking back %d days to %s", config.lookback_days, created_gte)

    session = maxio.get_session()
    transactions = maxio.list_transactions(session, created_gte=created_gte)
    log.info("Found %d transactions to evaluate.", len(transactions))

    corrected = skipped = failed = 0

    for txn in transactions:
        txn_id = txn["id"]

        if config.test_transaction_id is not None and txn_id != config.test_transaction_id:
            skipped += 1
            continue

        if not needs_correction(txn, contracted_ids):
            skipped += 1
            continue

        local_amount = txn.get("local_amount")
        correct_arr = format_amount(local_amount)
        correct_mrr = format_amount(Decimal(str(local_amount)) / 12)

        if config.dry_run:
            log.info(
                "[DRY RUN] Would correct transaction %d: ARR %s -> %s  |  MRR %s -> %s",
                txn_id,
                txn.get("local_arr_amount"),
                correct_arr,
                txn.get("local_normalized_amount"),
                correct_mrr,
            )
            corrected += 1
        else:
            success = maxio.patch_transaction(session, txn_id, correct_arr, correct_mrr)
            if success:
                corrected += 1
                log.info("Transaction %d updated.", txn_id)
            else:
                failed += 1

    log.info("Done. corrected=%d  skipped=%d  failed=%d", corrected, skipped, failed)

    if corrected > 0 and not config.dry_run:
        slack.get_client().chat_postMessage(
            channel=SLACK_CHANNEL,
            text=f":white_check_mark: *Maxio ARR correction*: corrected {corrected} transaction(s) "
            f"(skipped {skipped}, failed {failed}).",
        )


@job(
    description=(
        "Correct local_arr_amount and local_normalized_amount (MRR) for contracted-value"
        " item types in Maxio. ARR = local_amount, MRR = local_amount / 12."
    )
)
def maxio_arr_correction() -> None:
    correct_maxio_arr_amounts()
