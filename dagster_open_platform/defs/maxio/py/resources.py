from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation

import requests
from dagster import ConfigurableResource, get_dagster_logger

log = get_dagster_logger()


class MaxioResource(ConfigurableResource):
    """Resource for interacting with the Maxio (SaaSOptics) API."""

    api_token: str
    base_url: str = "https://y12.saasoptics.com/dagsterlabs/api/v1.0"

    def get_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f"Token {self.api_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        return session

    def list_transactions(self, session: requests.Session, created_gte: str) -> list[dict]:
        """Fetch all transactions created since `created_gte` (ISO 8601), handling pagination."""
        url = f"{self.base_url}/transactions/"
        params: dict = {
            "auditentry__created__gte": created_gte,
            "sort": "start_date",
            "page": 1,
        }

        transactions: list[dict] = []
        while url:
            resp = session.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            batch = data.get("results", [])
            transactions.extend(batch)
            log.info(
                "Fetched page %s -- %d transactions (total so far: %d)",
                params.get("page", "?"),
                len(batch),
                len(transactions),
            )

            next_url = data.get("next")
            if next_url:
                url = next_url
                params = {}
            else:
                break

        return transactions

    def patch_transaction(
        self,
        session: requests.Session,
        transaction_id: int,
        arr_amount: str,
        mrr_amount: str,
    ) -> bool:
        """PATCH a transaction's local_arr_amount and local_normalized_amount."""
        url = f"{self.base_url}/transactions/{transaction_id}/"
        payload = {
            "local_arr_amount": arr_amount,
            "local_normalized_amount": mrr_amount,
        }
        resp = session.patch(url, json=payload)
        if resp.status_code == 200:
            return True
        log.error(
            "PATCH failed for transaction %d: HTTP %d -- %s",
            transaction_id,
            resp.status_code,
            resp.text[:200],
        )
        return False


def term_months(start_date: str, end_date: str) -> int:
    """Calendar-month length of a transaction's term.

    Maxio represents a N-year term as end_date = start_date + N years - 1 day
    (e.g. a 12-month term runs 2026-01-23 to 2027-01-22), so we add a day back
    to end_date before diffing to get exact calendar months.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end_exclusive = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
    months = (end_exclusive.year - start.year) * 12 + (end_exclusive.month - start.month)
    if end_exclusive.day < start.day:
        months -= 1
    return months


def needs_correction(
    transaction: dict, contracted_ids: set[int], max_term_months: int = 12
) -> bool:
    item_id = transaction.get("item")
    if item_id not in contracted_ids:
        return False

    local_amount = transaction.get("local_amount")
    if local_amount is None:
        log.warning("Transaction %d has null local_amount -- skipping.", transaction["id"])
        return False

    start_date = transaction.get("start_date")
    end_date = transaction.get("end_date")
    if not start_date or not end_date:
        log.warning("Transaction %d is missing start/end date -- skipping.", transaction["id"])
        return False

    months = term_months(start_date, end_date)
    if months > max_term_months:
        log.info(
            "Transaction %d has a %d-month term (%s to %s) -- exceeds %d months, skipping.",
            transaction["id"],
            months,
            start_date,
            end_date,
            max_term_months,
        )
        return False

    try:
        amount = Decimal(str(local_amount))
        correct_arr = amount.quantize(Decimal("0.01"))
        correct_mrr = (amount / 12).quantize(Decimal("0.01"))

        arr_wrong = (
            transaction.get("local_arr_amount") is None
            or Decimal(str(transaction["local_arr_amount"])) != correct_arr
        )
        mrr_wrong = (
            transaction.get("local_normalized_amount") is None
            or Decimal(str(transaction["local_normalized_amount"])) != correct_mrr
        )
        return arr_wrong or mrr_wrong
    except (InvalidOperation, TypeError):
        log.warning("Could not compare amounts on transaction %d", transaction["id"])
        return False


def format_amount(value: object) -> str:
    return str(Decimal(str(value)).quantize(Decimal("0.01")))
