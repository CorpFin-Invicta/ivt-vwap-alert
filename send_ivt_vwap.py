import os
import smtplib
from email.message import EmailMessage
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf

TICKER = "IVT.JO"
TZ = ZoneInfo("Africa/Johannesburg")


def format_zar_from_cents(value: float) -> str:
    return f"R{value / 100:.2f}"


def get_data() -> pd.DataFrame:
    df = yf.download(
        TICKER,
        period="1mo",
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )

    if df is None or df.empty:
        raise RuntimeError("No data returned from Yahoo Finance.")

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    required = ["High", "Low", "Close", "Volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing required columns: {missing}")

    df = df[required].copy()
    df = df.dropna()
    df = df[df["Volume"] > 0]

    if len(df) < 5:
        raise RuntimeError(f"Need at least 5 valid trading days, got {len(df)}.")

    return df.tail(5).copy()


def calc_vwap(df: pd.DataFrame) -> dict:
    df["TypicalPrice"] = (df["High"] + df["Low"] + df["Close"]) / 3.0
    df["PV"] = df["TypicalPrice"] * df["Volume"]

    total_volume = df["Volume"].sum()
    if total_volume <= 0:
        raise RuntimeError("Total volume is zero; cannot compute VWAP.")

    vwap = df["PV"].sum() / total_volume
    latest = df.iloc[-1]
    latest_close = float(latest["Close"])
    deviation = ((latest_close / vwap) - 1.0) * 100.0

    return {
        "vwap": float(vwap),
        "latest_close": latest_close,
        "deviation_pct": deviation,
        "rows": df.copy(),
    }


def build_email_body(result: dict) -> str:
    now_local = datetime.now(TZ).strftime("%Y/%m/%d %H:%M")
    rows = result["rows"]

    lines = [
        "IVT.JO 5-Day VWAP Report",
        f"Run time: {now_local} Africa/Johannesburg",
        "",
        f"Latest close: {format_zar_from_cents(result['latest_close'])}",
        f"5-day VWAP: {format_zar_from_cents(result['vwap'])}",
        f"Deviation vs VWAP: {result['deviation_pct']:+.2f}%",
        "",
        "5 trading days used:",
    ]

    for idx, row in rows.iterrows():
        date_str = pd.Timestamp(idx).strftime("%Y/%m/%d")
        lines.append(
            f"- {date_str} | High {format_zar_from_cents(float(row['High']))} | "
            f"Low {format_zar_from_cents(float(row['Low']))} | "
            f"Close {format_zar_from_cents(float(row['Close']))} | "
            f"Volume {int(row['Volume']):,}"
        )

    return "\n".join(lines)


def send_email(subject: str, body: str) -> None:
    smtp_host = os.environ["SMTP_HOST"]
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ["SMTP_USER"]
    smtp_pass = os.environ["SMTP_PASS"]
    email_from = os.environ["EMAIL_FROM"]
    email_to = os.environ["EMAIL_TO"]

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = email_to
    msg.set_content(body)

    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)


def main() -> None:
    df = get_data()
    result = calc_vwap(df)

    today_str = datetime.now(TZ).strftime("%Y/%m/%d")
    subject = f"IVT.JO 5D VWAP - {today_str}"
    body = build_email_body(result)

    print(body)
    send_email(subject, body)


if __name__ == "__main__":
    main()
