from __future__ import annotations

from datetime import timedelta
from typing import List, Optional

import numpy as np
import pandas as pd
import pendulum
import pymsteams
from airflow.decorators import dag, task
from airflow.sdk import get_current_context
from airflow.operators.email import EmailOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook


WEBHOOK_URL = "https://predigerlicht.webhook.office.com/webhookb2/deb92327-8060-49cf-8a80-d83d4476b7bb@a49c30ec-fbcb-49ed-aa08-109e364b37fe/IncomingWebhook/8b19c09de52a484da331172ea680a2d9/ef820e5a-5e03-49c3-8bf7-6e8255d3eaca"
ALERT_EMAIL = "development@prediger.de"


def send_teams_warning(folder_path: str, max_time: int, warning_text: str) -> None:
    """Send an alert to Microsoft Teams for an expired folder timestamp."""

    teams_message = pymsteams.connectorcard(WEBHOOK_URL)
    teams_message.color("#FF0000")

    message_section = pymsteams.cardsection()
    message_section.addFact("Ordner", folder_path)
    message_section.addFact("Max Dauer [Sek]", max_time)
    message_section.text("Bitte überprüfe den OLSI-Dienst")

    teams_message.addSection(message_section)
    teams_message.text("**OLSI Dienst crashed**")
    teams_message.send()


@task()
def send_email_notifications(warnings: List[str]) -> None:
    """Send warning emails via EmailOperator when warnings exist."""

    if not warnings:
        return

    context = get_current_context()
    subject = "OLSI Dienst crashed"
    html_lines = [
        "<p>Bitte überprüfe den OLSI-Dienst.</p>",
        "<ul>",
    ]

    for w in warnings:
        html_lines.append(f"<li>{w}</li>")

    html_lines.append("</ul>")

    EmailOperator(
        task_id="email_warning_operator",
        to=[ALERT_EMAIL],
        subject=subject,
        html_content="\n".join(html_lines),
    ).execute(context=context)


@task()
def check_folder(folder_path: str, max_time: int) -> Optional[str]:
    """Check a single folder for stale files and notify via Teams if needed."""

    hook = SFTPHook(ftp_conn_id="magento_sftp")
    warning: Optional[str] = None

    try:
        folder_list = hook.describe_directory(folder_path)
        folder_df = pd.DataFrame.from_dict(folder_list, orient="index")

        if folder_df.empty:
            return None

        files = folder_df[folder_df["type"] == "file"].copy()
        if files.empty:
            return None

        files["modify_2"] = pd.to_datetime(
            files["modify"].astype(str), format="%Y%m%d%H%M%S"
        ).dt.tz_localize("UTC").dt.tz_convert("Europe/Berlin")

        now = pd.Timestamp.now(tz="UTC").tz_convert("Europe/Berlin")
        files["warning"] = np.where(now > files["modify_2"] + pd.Timedelta(max_time, "s"), True, False)

        if files["warning"].any():
            warning = f"Warning on Folder: {folder_path}, Max time expired: {max_time}"
            send_teams_warning(folder_path=folder_path, max_time=max_time, warning_text=warning)

        return warning
    finally:
        hook.close_conn()


@task()
def collect_warnings(warnings: List[Optional[str]]) -> List[str]:
    """Collect non-empty warnings for downstream observability or logging."""

    return [w for w in warnings if w]


@dag(
    dag_id="check_olsi",
    description="Check OLSI",
    start_date=pendulum.datetime(2020, 8, 2, tz="Europe/Berlin"),
    schedule="*/5 7-20 * * *",
    catchup=False,
    default_args={
        "owner": "m.tesche",
        "email": ["m.tesche@prediger.de", "1501da79.prediger.de@emea.teams.ms"],
        "email_on_failure": True,
        "email_on_retry": True,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        "depends_on_past": False,
    },
    tags=["monitoring", "olsi"],
)
def check_olsi():
    checks = [
        {"folder_path": "/data1/exchange/export/wawi/order/order", "max_time": 2100},
        {"folder_path": "/data1/exchange/export/wawi/order/webdirect", "max_time": 360},
        {"folder_path": "/data1/exchange/export/wawi/order/catalog", "max_time": 3900},
        {"folder_path": "/data1/exchange/export/wawi/order/wishlist", "max_time": 360},
    ]

    warnings = check_folder.expand(
        folder_path=[c["folder_path"] for c in checks],
        max_time=[c["max_time"] for c in checks],
    )

    collected_warnings = collect_warnings(warnings)
    send_email_notifications(collected_warnings)


dag = check_olsi()
