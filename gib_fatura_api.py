#!/usr/bin/env python3
"""
FastAPI backend for P2P Fatura Paneli.
Çalıştırmak için: python gib_fatura_api.py
Tarayıcıda: http://localhost:8000
"""

from contextlib import closing
import base64
import binascii
from datetime import date, datetime, timedelta
from importlib.util import find_spec
from io import BytesIO
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import sqlite3
from types import MethodType
from typing import Optional
import unicodedata
import zipfile

import pandas as pd
import requests
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ── Sabitler ──────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
DATABASE_PATH = BASE_DIR / "gib_fatura.db"
ARCHIVE_DIR = BASE_DIR / "arsivler"
EXPENSE_DIR = BASE_DIR / "giderler"
EXPENSE_INVOICE_DIR = EXPENSE_DIR / "faturalar"
BACKUP_DIR = BASE_DIR / "yedekler"
HTML_PATH = BASE_DIR / "p2p_panel.html"
STYLES_DIR = BASE_DIR / "styles"
PANEL_AUTH_PATH = BASE_DIR / "panel_auth.json"

EXPORT_KOLONLARI = [
    "İşlem Tarihi", "Müşteri Adı", "T.C. Kimlik No",
    "Vergisiz Bedel", "KDV", "Toplam Fatura", "GİB Durumu",
]
NUMERIC_KOLONLAR = [
    "Vergisiz Bedel", "KDV", "Toplam Fatura",
    "Satılan USDT", "Alış Kuru", "Satış Kuru",
]
ARCHIVE_MATCH_KOLONLARI = [
    "İşlem Tarihi", "Müşteri Adı", "T.C. Kimlik No",
    "Vergisiz Bedel", "KDV", "Toplam Fatura",
]
DEFAULT_TC = "11111111111"
GIB_REQUEST_TIMEOUT_SECONDS = 20
PANEL_SESSION_TTL_HOURS = 12
TRACKING_COLUMN_DEFINITIONS = {
    "gib_ettn": "TEXT",
    "gib_belge_numarasi": "TEXT",
    "gib_son_senkron": "TEXT",
}
EXCLUDED_STATS_GIB_STATUSES = {
    "Uyumluluk Hatası",
    "GİB Hatası",
    "Bağlantı Hatası",
    "Zaman Aşımı",
    "Kütüphane Eksik",
}
EXPENSE_CATEGORIES = ["Araç", "Yemek", "Teknoloji", "Market", "Genel"]
DEFAULT_TAX_SHIELD_RATE = 0.20
PUBLIC_API_PATHS = {
    "/api/auth/login",
    "/api/auth/status",
    "/api/auth/logout",
}
ACTIVE_PANEL_SESSIONS: dict[str, dict[str, object]] = {}
UNSET = object()

# ── Uygulama ──────────────────────────────────────────────────────────────────
app = FastAPI(title="P2P Fatura API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/styles", StaticFiles(directory=STYLES_DIR), name="styles")


@app.middleware("http")
async def panel_auth_middleware(request: Request, call_next):
    path = request.url.path
    if request.method == "OPTIONS" or not path.startswith("/api/") or path in PUBLIC_API_PATHS:
        return await call_next(request)

    token = str(request.headers.get("X-Panel-Token", "")).strip() or str(request.query_params.get("panel_token", "")).strip()
    session = get_panel_session(token) if token else None
    if session is None:
        return JSONResponse(status_code=401, content={"detail": "Oturum gerekli."})

    request.state.panel_user = session.get("username")
    return await call_next(request)

# ── DB yardımcıları ───────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat(sep=" ")


def now_utc() -> datetime:
    return datetime.utcnow().replace(microsecond=0)


def ensure_panel_auth_config(config_path: Path = PANEL_AUTH_PATH) -> dict:
    if config_path.exists():
        return json.loads(config_path.read_text(encoding="utf-8"))

    default_password = secrets.token_urlsafe(9)
    config = {
        "username": "admin",
        "password": default_password,
        "created_at": now_iso(),
        "password_changed": False,
    }
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    return config


def load_panel_auth_config(config_path: Path = PANEL_AUTH_PATH) -> dict:
    return ensure_panel_auth_config(config_path)


def verify_panel_credentials(
    username: str,
    password: str,
    config_path: Path = PANEL_AUTH_PATH,
) -> bool:
    config = load_panel_auth_config(config_path)
    return secrets.compare_digest(username.strip(), str(config.get("username", "")).strip()) and secrets.compare_digest(
        password,
        str(config.get("password", "")),
    )


def prune_panel_sessions() -> None:
    current = now_utc()
    expired_tokens = [
        token
        for token, payload in ACTIVE_PANEL_SESSIONS.items()
        if current >= payload.get("expires_at", current)
    ]
    for token in expired_tokens:
        ACTIVE_PANEL_SESSIONS.pop(token, None)


def create_panel_session(username: str) -> str:
    prune_panel_sessions()
    token = secrets.token_urlsafe(32)
    ACTIVE_PANEL_SESSIONS[token] = {
        "username": username.strip(),
        "expires_at": now_utc() + timedelta(hours=PANEL_SESSION_TTL_HOURS),
    }
    return token


def get_panel_session(token: str) -> Optional[dict[str, object]]:
    prune_panel_sessions()
    session = ACTIVE_PANEL_SESSIONS.get(token.strip())
    if not session:
        return None
    return session


def invalidate_panel_session(token: str) -> None:
    ACTIVE_PANEL_SESSIONS.pop(token.strip(), None)


def get_db_connection(db_path: Path = DATABASE_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_transaction_columns(conn: sqlite3.Connection) -> None:
    existing_columns = {
        str(row["name"])
        for row in conn.execute("PRAGMA table_info(transactions)").fetchall()
    }
    for column_name, column_definition in TRACKING_COLUMN_DEFINITIONS.items():
        if column_name not in existing_columns:
            conn.execute(
                f"ALTER TABLE transactions ADD COLUMN {column_name} {column_definition}"
            )


def ensure_expense_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            islem_tarihi TEXT NOT NULL,
            aciklama TEXT NOT NULL,
            kategori TEXT NOT NULL,
            toplam_tutar REAL NOT NULL,
            kdv_orani REAL NOT NULL,
            kdv_tutari REAL NOT NULL,
            net_gider REAL NOT NULL,
            gider_yazim_orani REAL NOT NULL,
            vergi_matrahi REAL NOT NULL,
            indirilecek_kdv REAL NOT NULL,
            vergi_kalkani REAL NOT NULL,
            ticari_arac INTEGER NOT NULL DEFAULT 0,
            fatura_dosya_yolu TEXT,
            fatura_orijinal_adi TEXT,
            olusturulma_zamani TEXT NOT NULL,
            guncellenme_zamani TEXT NOT NULL
        )
        """
    )


def ensure_database(db_path: Path = DATABASE_PATH) -> None:
    with closing(get_db_connection(db_path)) as conn, conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                islem_tarihi TEXT NOT NULL,
                musteri_adi TEXT NOT NULL,
                musteri_tc TEXT NOT NULL,
                satilan_usdt REAL NOT NULL DEFAULT 0,
                alis_kuru REAL NOT NULL DEFAULT 0,
                satis_kuru REAL NOT NULL DEFAULT 0,
                vergisiz_bedel REAL NOT NULL DEFAULT 0,
                kdv REAL NOT NULL DEFAULT 0,
                toplam_fatura REAL NOT NULL DEFAULT 0,
                gib_durumu TEXT NOT NULL DEFAULT 'Kaydedildi',
                durum_mesaji TEXT NOT NULL DEFAULT '',
                arsiv_hafta_kodu TEXT,
                arsiv_etiketi TEXT,
                kaynak TEXT NOT NULL DEFAULT 'api',
                olusturulma_zamani TEXT NOT NULL,
                guncellenme_zamani TEXT NOT NULL
            )
            """
        )
        ensure_transaction_columns(conn)
        ensure_expense_tables(conn)


def df_to_xlsx_bytes(df: pd.DataFrame, sheet_name: str = "Sayfa1") -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()


def slugify_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", normalized.lower()).strip("_")
    return slug or "gider"


def calculate_expense_breakdown(
    toplam_tutar: float,
    kdv_orani: float,
    kategori: str,
    ticari_arac: bool = False,
    tax_shield_rate: float = DEFAULT_TAX_SHIELD_RATE,
) -> dict:
    safe_total = round(float(toplam_tutar), 2)
    safe_rate = float(kdv_orani)
    kdv_tutari = round(safe_total - (safe_total / (1 + safe_rate / 100)), 2)
    net_gider = round(safe_total - kdv_tutari, 2)
    gider_yazim_orani = 1.0
    vergi_matrahi = round(net_gider * gider_yazim_orani, 2)
    indirilecek_kdv = round(kdv_tutari, 2)
    vergi_kalkani = round(vergi_matrahi * float(tax_shield_rate), 2)
    return {
        "kdv_tutari": kdv_tutari,
        "net_gider": net_gider,
        "gider_yazim_orani": gider_yazim_orani,
        "vergi_matrahi": vergi_matrahi,
        "indirilecek_kdv": indirilecek_kdv,
        "vergi_kalkani": vergi_kalkani,
    }


def validate_expense_input(
    islem_tarihi: date,
    aciklama: str,
    kategori: str,
    toplam_tutar: float,
    kdv_orani: float,
    file_name: str,
) -> None:
    if islem_tarihi > date.today():
        raise HTTPException(400, "Gider tarihi gelecekte olamaz.")
    if not aciklama.strip():
        raise HTTPException(400, "Açıklama boş bırakılamaz.")
    if kategori not in EXPENSE_CATEGORIES:
        raise HTTPException(400, "Geçersiz gider kategorisi seçildi.")
    if float(toplam_tutar) <= 0:
        raise HTTPException(400, "Toplam tutar sıfırdan büyük olmalıdır.")
    if not 10 <= float(kdv_orani) <= 20:
        raise HTTPException(400, "KDV oranı %10 ile %20 arasında olmalıdır.")
    extension = Path(file_name or "").suffix.lower()
    if extension not in {".pdf", ".jpg", ".jpeg", ".png"}:
        raise HTTPException(400, "Fatura dosyası PDF, JPG, JPEG veya PNG olmalıdır.")


def save_expense(record: dict, db_path: Path = DATABASE_PATH) -> int:
    ts = now_iso()
    with closing(get_db_connection(db_path)) as conn, conn:
        cursor = conn.execute(
            """
            INSERT INTO expenses (
                islem_tarihi, aciklama, kategori, toplam_tutar, kdv_orani,
                kdv_tutari, net_gider, gider_yazim_orani, vergi_matrahi,
                indirilecek_kdv, vergi_kalkani, ticari_arac,
                fatura_dosya_yolu, fatura_orijinal_adi,
                olusturulma_zamani, guncellenme_zamani
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["islem_tarihi"],
                record["aciklama"],
                record["kategori"],
                record["toplam_tutar"],
                record["kdv_orani"],
                record["kdv_tutari"],
                record["net_gider"],
                record["gider_yazim_orani"],
                record["vergi_matrahi"],
                record["indirilecek_kdv"],
                record["vergi_kalkani"],
                1 if record.get("ticari_arac") else 0,
                record.get("fatura_dosya_yolu"),
                record.get("fatura_orijinal_adi"),
                ts,
                ts,
            ),
        )
        return int(cursor.lastrowid)


def attach_expense_invoice(
    expense_id: int,
    file_name: str,
    file_bytes: bytes,
    *,
    islem_tarihi: date,
    aciklama: str,
    db_path: Path = DATABASE_PATH,
) -> Path:
    extension = Path(file_name).suffix.lower()
    safe_name = f"{islem_tarihi.isoformat()}_{expense_id}_{slugify_text(aciklama)}{extension}"
    target_path = EXPENSE_INVOICE_DIR / safe_name
    EXPENSE_INVOICE_DIR.mkdir(parents=True, exist_ok=True)
    target_path.write_bytes(file_bytes)

    with closing(get_db_connection(db_path)) as conn, conn:
        conn.execute(
            """
            UPDATE expenses
            SET fatura_dosya_yolu = ?, fatura_orijinal_adi = ?, guncellenme_zamani = ?
            WHERE id = ?
            """,
            (str(target_path.resolve()), file_name, now_iso(), expense_id),
        )
    return target_path


def load_expenses(db_path: Path = DATABASE_PATH, month_key: Optional[str] = None) -> pd.DataFrame:
    where_clause = ""
    params: list[object] = []
    if month_key:
        where_clause = "WHERE substr(islem_tarihi, 1, 7) = ?"
        params.append(month_key)

    query = f"""
        SELECT
            id,
            islem_tarihi AS tarih,
            aciklama,
            kategori,
            toplam_tutar,
            kdv_orani,
            kdv_tutari,
            net_gider,
            gider_yazim_orani,
            vergi_matrahi,
            indirilecek_kdv,
            vergi_kalkani,
            ticari_arac,
            fatura_dosya_yolu,
            fatura_orijinal_adi,
            olusturulma_zamani
        FROM expenses
        {where_clause}
        ORDER BY date(islem_tarihi) DESC, id DESC
    """
    with closing(get_db_connection(db_path)) as conn:
        df = pd.read_sql_query(query, conn, params=params)

    if df.empty:
        return pd.DataFrame(columns=[
            "id", "tarih", "aciklama", "kategori", "toplam_tutar", "kdv_orani", "kdv_tutari",
            "net_gider", "gider_yazim_orani", "vergi_matrahi", "indirilecek_kdv", "vergi_kalkani",
            "ticari_arac", "fatura_dosya_yolu", "fatura_orijinal_adi", "olusturulma_zamani",
        ])

    numeric_columns = [
        "toplam_tutar", "kdv_orani", "kdv_tutari", "net_gider", "gider_yazim_orani",
        "vergi_matrahi", "indirilecek_kdv", "vergi_kalkani",
    ]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
    df["ticari_arac"] = df["ticari_arac"].fillna(0).astype(int).astype(bool)
    return df


def summarize_expenses(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "toplam_gider": 0.0,
            "toplam_kdv_iadesi": 0.0,
            "toplam_vergi_matrahi": 0.0,
            "toplam_vergi_kalkani": 0.0,
            "kayit_adedi": 0,
        }
    return {
        "toplam_gider": round(float(df["toplam_tutar"].sum()), 2),
        "toplam_kdv_iadesi": round(float(df["indirilecek_kdv"].sum()), 2),
        "toplam_vergi_matrahi": round(float(df["vergi_matrahi"].sum()), 2),
        "toplam_vergi_kalkani": round(float(df["vergi_kalkani"].sum()), 2),
        "kayit_adedi": int(len(df)),
    }


def build_expense_report_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[
            "Tarih", "Açıklama", "Kategori", "Toplam Tutar", "KDV Oranı", "KDV Tutarı", "Net Gider",
            "Gider Yazım Oranı", "Vergi Matrahı", "İndirilecek KDV", "Vergi Kalkanı", "Ticari Araç", "Fatura Dosya Yolu",
        ])

    export_df = df.copy()
    export_df["Tarih"] = pd.to_datetime(export_df["tarih"], errors="coerce").dt.strftime("%Y-%m-%d")
    export_df["Açıklama"] = export_df["aciklama"]
    export_df["Kategori"] = export_df["kategori"]
    export_df["Toplam Tutar"] = export_df["toplam_tutar"]
    export_df["KDV Oranı"] = export_df["kdv_orani"]
    export_df["KDV Tutarı"] = export_df["kdv_tutari"]
    export_df["Net Gider"] = export_df["net_gider"]
    export_df["Gider Yazım Oranı"] = (export_df["gider_yazim_orani"] * 100).round(0).astype(int).astype(str) + "%"
    export_df["Vergi Matrahı"] = export_df["vergi_matrahi"]
    export_df["İndirilecek KDV"] = export_df["indirilecek_kdv"]
    export_df["Vergi Kalkanı"] = export_df["vergi_kalkani"]
    export_df["Ticari Araç"] = export_df["ticari_arac"].map({True: "Evet", False: "Hayır"})
    export_df["Fatura Dosya Yolu"] = export_df["fatura_dosya_yolu"]
    export_df = export_df[
        [
            "Tarih", "Açıklama", "Kategori", "Toplam Tutar", "KDV Oranı", "KDV Tutarı", "Net Gider",
            "Gider Yazım Oranı", "Vergi Matrahı", "İndirilecek KDV", "Vergi Kalkanı", "Ticari Araç", "Fatura Dosya Yolu",
        ]
    ]
    summary = summarize_expenses(df)
    total_row = {column: "" for column in export_df.columns}
    total_row["Açıklama"] = "TOPLAM"
    total_row["Toplam Tutar"] = summary["toplam_gider"]
    total_row["Vergi Matrahı"] = summary["toplam_vergi_matrahi"]
    total_row["İndirilecek KDV"] = summary["toplam_kdv_iadesi"]
    total_row["Vergi Kalkanı"] = summary["toplam_vergi_kalkani"]
    return pd.concat([export_df, pd.DataFrame([total_row])], ignore_index=True)


def build_expense_report_xlsx_bytes(df: pd.DataFrame, month_key: str) -> bytes:
    summary = summarize_expenses(df)
    summary_df = pd.DataFrame(
        [
            {"Metrik": "Dönem", "Değer": month_key},
            {"Metrik": "Toplam Gider", "Değer": summary["toplam_gider"]},
            {"Metrik": "Toplam KDV İadesi", "Değer": summary["toplam_kdv_iadesi"]},
            {"Metrik": "Toplam Vergi Matrahı", "Değer": summary["toplam_vergi_matrahi"]},
            {"Metrik": "Vergi Kalkanı", "Değer": summary["toplam_vergi_kalkani"]},
            {"Metrik": "Kayıt Adedi", "Değer": summary["kayit_adedi"]},
        ]
    )
    detail_df = build_expense_report_dataframe(df)
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary_df.to_excel(writer, index=False, sheet_name="Özet")
        detail_df.to_excel(writer, index=False, sheet_name="Giderler")
    return output.getvalue()


def build_expense_report_csv_bytes(df: pd.DataFrame) -> bytes:
    return build_expense_report_dataframe(df).to_csv(index=False).encode("utf-8-sig")


def get_expense_months(db_path: Path = DATABASE_PATH) -> list[str]:
    df = load_expenses(db_path)
    if df.empty:
        return [date.today().strftime("%Y-%m")]
    months = (
        pd.to_datetime(df["tarih"], errors="coerce")
        .dropna()
        .dt.strftime("%Y-%m")
        .drop_duplicates()
        .sort_values(ascending=False)
        .tolist()
    )
    return months or [date.today().strftime("%Y-%m")]


def open_file_with_default_app(file_path: Path) -> None:
    if sys.platform.startswith("win"):
        os.startfile(str(file_path))
        return
    if sys.platform == "darwin":
        subprocess.Popen(["open", str(file_path)])
        return
    subprocess.Popen(["xdg-open", str(file_path)])


def build_expense_audit(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "summary": {
                "kayit_adedi": 0,
                "sorunlu_kayit": 0,
                "dosyasi_olmayan": 0,
                "diskte_olmayan": 0,
                "gecersiz_kdv": 0,
                "gecersiz_kategori": 0,
                "eksik_aciklama": 0,
            },
            "records": [],
        }

    records: list[dict] = []
    summary = {
        "kayit_adedi": int(len(df)),
        "sorunlu_kayit": 0,
        "dosyasi_olmayan": 0,
        "diskte_olmayan": 0,
        "gecersiz_kdv": 0,
        "gecersiz_kategori": 0,
        "eksik_aciklama": 0,
    }

    for _, row in df.iterrows():
        issues: list[str] = []
        file_path_value = str(row.get("fatura_dosya_yolu") or "").strip()
        if not file_path_value:
            issues.append("Fatura dosyası ilişkili değil")
            summary["dosyasi_olmayan"] += 1
        elif not Path(file_path_value).exists():
            issues.append("Fatura dosyası diskte bulunamadı")
            summary["diskte_olmayan"] += 1

        if str(row.get("aciklama") or "").strip() == "":
            issues.append("Açıklama eksik")
            summary["eksik_aciklama"] += 1

        if str(row.get("kategori") or "") not in EXPENSE_CATEGORIES:
            issues.append("Kategori geçersiz")
            summary["gecersiz_kategori"] += 1

        vat_rate = float(row.get("kdv_orani") or 0)
        if not 10 <= vat_rate <= 20:
            issues.append("KDV oranı 10-20 dışında")
            summary["gecersiz_kdv"] += 1

        if issues:
            summary["sorunlu_kayit"] += 1
            records.append(
                {
                    "id": int(row.get("id") or 0),
                    "tarih": str(row.get("tarih") or ""),
                    "aciklama": str(row.get("aciklama") or ""),
                    "kategori": str(row.get("kategori") or ""),
                    "fatura_dosya_yolu": file_path_value,
                    "issues": issues,
                }
            )

    return {"summary": summary, "records": records}


def add_path_to_zip(archive: zipfile.ZipFile, path: Path, root_name: str) -> None:
    if not path.exists():
        return
    if path.is_file():
        archive.write(path, arcname=f"{root_name}/{path.name}")
        return
    for child in path.rglob("*"):
        if child.is_file():
            archive.write(child, arcname=f"{root_name}/{child.relative_to(path).as_posix()}")


def create_backup_archive(
    *,
    db_path: Path = DATABASE_PATH,
    archive_dir: Path = ARCHIVE_DIR,
    expense_invoice_dir: Path = EXPENSE_INVOICE_DIR,
    backup_dir: Path = BACKUP_DIR,
    auth_config_path: Path = PANEL_AUTH_PATH,
    prefix: str = "manuel_yedek",
    reference_time: Optional[datetime] = None,
) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    current_time = reference_time or datetime.now()
    file_name = f"{prefix}_{current_time.strftime('%Y-%m-%d_%H%M%S')}.zip"
    output_path = backup_dir / file_name

    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        add_path_to_zip(archive, db_path, "veritabani")
        add_path_to_zip(archive, archive_dir, "arsivler")
        add_path_to_zip(archive, expense_invoice_dir, "giderler/faturalar")
        add_path_to_zip(archive, auth_config_path, "guvenlik")
        manifest = {
            "created_at": current_time.isoformat(timespec="seconds"),
            "database": str(db_path),
            "expense_invoice_dir": str(expense_invoice_dir),
            "archive_dir": str(archive_dir),
        }
        archive.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))

    return output_path


def ensure_daily_backup() -> Optional[Path]:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    daily_prefix = f"oto_yedek_{date.today().strftime('%Y-%m-%d')}"
    existing_backup = next(BACKUP_DIR.glob(f"{daily_prefix}*.zip"), None)
    if existing_backup is not None:
        return existing_backup
    return create_backup_archive(prefix=daily_prefix)


def get_hafta_bilgisi(referans_tarihi: date | None = None) -> dict:
    bugun = referans_tarihi or date.today()
    yil, hafta_no, _ = bugun.isocalendar()
    pazartesi = bugun - timedelta(days=bugun.weekday())
    pazar = pazartesi + timedelta(days=6)
    return {
        "hafta_kodu": f"{yil}_{hafta_no:02d}",
        "etiket": f"{pazartesi.strftime('%d.%m.%Y')} - {pazar.strftime('%d.%m.%Y')}",
        "dosya_adi": (
            f"arsiv_hafta_{yil}_{hafta_no:02d}"
            f"_{pazartesi.strftime('%d.%m')}_{pazar.strftime('%d.%m')}.xlsx"
        ),
        "pazartesi": pazartesi,
        "pazar": pazar,
    }


def make_archive_key(label: str, record_date: date) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", label.strip().lower())
    normalized = normalized.strip("_") or "arsiv"
    return f"manuel_{record_date.strftime('%Y%m%d')}_{normalized}"


def normalize_invoice_identity(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    normalized["İşlem Tarihi"] = (
        pd.to_datetime(normalized["İşlem Tarihi"], errors="coerce")
        .dt.strftime("%Y-%m-%d")
        .fillna("")
    )
    normalized["Müşteri Adı"] = (
        normalized["Müşteri Adı"].fillna("").astype(str).str.strip().str.casefold()
    )
    normalized["T.C. Kimlik No"] = (
        normalized["T.C. Kimlik No"]
        .fillna(DEFAULT_TC)
        .astype(str)
        .str.replace(".0", "", regex=False)
    )
    for col in ["Vergisiz Bedel", "KDV", "Toplam Fatura"]:
        normalized[col] = pd.to_numeric(normalized[col], errors="coerce").fillna(0.0).round(2)
    return normalized


def filter_transactions_for_statistics(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "GİB Durumu" not in df.columns:
        return df.copy()
    return df[~df["GİB Durumu"].isin(EXCLUDED_STATS_GIB_STATUSES)].copy()


# ── Yükleme fonksiyonları ─────────────────────────────────────────────────────

def load_transactions(
    db_path: Path = DATABASE_PATH, archived: bool | None = None
) -> pd.DataFrame:
    if archived is True:
        where = "WHERE arsiv_hafta_kodu IS NOT NULL"
    elif archived is False:
        where = "WHERE arsiv_hafta_kodu IS NULL"
    else:
        where = ""

    query = f"""
        SELECT
            id,
            islem_tarihi          AS "İşlem Tarihi",
            musteri_adi           AS "Müşteri Adı",
            musteri_tc            AS "T.C. Kimlik No",
            satilan_usdt          AS "Satılan USDT",
            alis_kuru             AS "Alış Kuru",
            satis_kuru            AS "Satış Kuru",
            vergisiz_bedel        AS "Vergisiz Bedel",
            kdv                   AS "KDV",
            toplam_fatura         AS "Toplam Fatura",
            gib_durumu            AS "GİB Durumu",
            durum_mesaji          AS "Durum Mesajı",
            gib_ettn              AS "GİB ETTN",
            gib_belge_numarasi    AS "GİB Belge No",
            gib_son_senkron       AS "GİB Son Senkron",
            arsiv_hafta_kodu      AS "Arşiv Hafta Kodu",
            arsiv_etiketi         AS "Arşiv Etiketi",
            olusturulma_zamani    AS "Kayıt Zamanı"
        FROM transactions
        {where}
        ORDER BY date(islem_tarihi) DESC, id DESC
    """
    with closing(get_db_connection(db_path)) as conn:
        df = pd.read_sql_query(query, conn)

    if df.empty:
        return pd.DataFrame(columns=[
            "id", "İşlem Tarihi", "Müşteri Adı", "T.C. Kimlik No",
            "Satılan USDT", "Alış Kuru", "Satış Kuru",
            "Vergisiz Bedel", "KDV", "Toplam Fatura",
            "GİB Durumu", "Durum Mesajı", "GİB ETTN", "GİB Belge No", "GİB Son Senkron", "Arşiv Hafta Kodu",
            "Arşiv Etiketi", "Kayıt Zamanı",
        ])

    df["İşlem Tarihi"] = (
        pd.to_datetime(df["İşlem Tarihi"], errors="coerce")
        .dt.strftime("%Y-%m-%d")
        .fillna("")
    )
    for col in NUMERIC_KOLONLAR:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df


def load_archive_groups(db_path: Path = DATABASE_PATH) -> pd.DataFrame:
    query = """
        SELECT
            arsiv_hafta_kodu                      AS "Arşiv Hafta Kodu",
            COALESCE(arsiv_etiketi, arsiv_hafta_kodu) AS "Arşiv Etiketi",
            COUNT(*)                              AS "Kayıt Adedi",
            ROUND(SUM(vergisiz_bedel), 2)         AS "Vergisiz Bedel",
            ROUND(SUM(kdv), 2)                    AS "KDV",
            ROUND(SUM(toplam_fatura), 2)          AS "Toplam Fatura",
            MAX(guncellenme_zamani)               AS "Güncelleme Zamanı"
        FROM transactions
        WHERE arsiv_hafta_kodu IS NOT NULL
        GROUP BY arsiv_hafta_kodu, arsiv_etiketi
        ORDER BY MAX(guncellenme_zamani) DESC
    """
    with closing(get_db_connection(db_path)) as conn:
        return pd.read_sql_query(query, conn)


def load_popular_usdt_values(limit: int = 8, db_path: Path = DATABASE_PATH) -> list[float]:
    query = """
        SELECT
            ROUND(satilan_usdt, 2)   AS satilan_usdt,
            COUNT(*)                 AS kullanim_adedi,
            MAX(guncellenme_zamani)  AS son_kullanim
        FROM transactions
        WHERE satilan_usdt > 0
        GROUP BY ROUND(satilan_usdt, 2)
        ORDER BY kullanim_adedi DESC, son_kullanim DESC, satilan_usdt DESC
        LIMIT ?
    """
    with closing(get_db_connection(db_path)) as conn:
        rows = conn.execute(query, (limit,)).fetchall()
    return [round(float(row[0]), 2) for row in rows]


def build_export_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    export_df = df.copy()
    if "İşlem Tarihi" in export_df.columns:
        export_df["İşlem Tarihi"] = (
            pd.to_datetime(export_df["İşlem Tarihi"], errors="coerce")
            .dt.strftime("%Y-%m-%d")
            .fillna("")
        )
    cols = [c for c in EXPORT_KOLONLARI if c in export_df.columns]
    export_df = export_df[cols].copy() if cols else pd.DataFrame(columns=EXPORT_KOLONLARI)
    total_row: dict = {c: "" for c in export_df.columns}
    if "Müşteri Adı" in total_row:
        total_row["Müşteri Adı"] = "TOPLAM"
    for c in ["Vergisiz Bedel", "KDV", "Toplam Fatura"]:
        if c in export_df.columns:
            total_row[c] = round(
                pd.to_numeric(export_df[c], errors="coerce").fillna(0.0).sum(), 2
            )
    return pd.concat([export_df, pd.DataFrame([total_row])], ignore_index=True)


# ── Hesap / GİB ───────────────────────────────────────────────────────────────

def calculate_invoice(satilan_usdt: float, alis_kuru: float, satis_kuru: float) -> dict:
    marj = satis_kuru - alis_kuru
    toplam = round(satilan_usdt * marj, 2)
    vergisiz = round(toplam / 1.20, 2)
    kdv = round(toplam - vergisiz, 2)
    return {"vergisiz_bedel": vergisiz, "kdv": kdv, "toplam_fatura": toplam}


def reconcile_automatic_invoice_totals(db_path: Path = DATABASE_PATH) -> int:
    with closing(get_db_connection(db_path)) as conn, conn:
        rows = conn.execute(
            """
            SELECT id, satilan_usdt, alis_kuru, satis_kuru, vergisiz_bedel, kdv, toplam_fatura
            FROM transactions
            WHERE satilan_usdt > 0 AND alis_kuru > 0 AND satis_kuru > 0
            """
        ).fetchall()

        updated_count = 0
        for row in rows:
            satilan_usdt = float(row["satilan_usdt"])
            alis_kuru = float(row["alis_kuru"])
            satis_kuru = float(row["satis_kuru"])
            eski_vergisiz = round(satilan_usdt * (satis_kuru - alis_kuru), 2)
            eski_kdv = round(eski_vergisiz * 0.20, 2)
            eski_toplam = round(eski_vergisiz + eski_kdv, 2)

            if not (
                abs(float(row["vergisiz_bedel"]) - eski_vergisiz) <= 0.01
                and abs(float(row["kdv"]) - eski_kdv) <= 0.01
                and abs(float(row["toplam_fatura"]) - eski_toplam) <= 0.01
            ):
                continue

            corrected_invoice = calculate_invoice(satilan_usdt, alis_kuru, satis_kuru)
            conn.execute(
                """
                UPDATE transactions
                SET vergisiz_bedel = ?, kdv = ?, toplam_fatura = ?, guncellenme_zamani = ?
                WHERE id = ?
                """,
                (
                    corrected_invoice["vergisiz_bedel"],
                    corrected_invoice["kdv"],
                    corrected_invoice["toplam_fatura"],
                    now_iso(),
                    int(row["id"]),
                ),
            )
            updated_count += 1

        return updated_count


def split_customer_name(full_name: str) -> tuple[str, str]:
    parts = full_name.strip().split()
    if len(parts) <= 1:
        return full_name.strip(), ""
    return " ".join(parts[:-1]), parts[-1]


def normalize_person_name(value: str) -> str:
    return " ".join(str(value or "").split()).casefold()


def normalize_tc_value(value: object) -> str:
    return str(value or DEFAULT_TC).strip().replace(".0", "")


def normalize_portal_date(value: object) -> str:
    parsed = pd.to_datetime(str(value or "").strip(), errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def portal_model_to_dict(item: object) -> dict:
    if isinstance(item, dict):
        return item
    if hasattr(item, "dict"):
        return item.dict()
    return {}


def extract_first_value(data: dict, *keys: str) -> object:
    lowered = {str(key).casefold(): value for key, value in data.items()}
    for key in keys:
        value = lowered.get(str(key).casefold())
        if value not in (None, ""):
            return value
    return None


def normalize_gib_drafts(drafts: list[object]) -> pd.DataFrame:
    rows: list[dict] = []
    for draft in drafts:
        data = portal_model_to_dict(draft)
        if not data:
            continue

        musteri_adi = extract_first_value(data, "aliciUnvanAdSoyad")
        if not musteri_adi:
            parts = [
                str(extract_first_value(data, "aliciAdi") or "").strip(),
                str(extract_first_value(data, "aliciSoyadi") or "").strip(),
            ]
            musteri_adi = " ".join(part for part in parts if part).strip()
        if not musteri_adi:
            musteri_adi = str(extract_first_value(data, "aliciUnvan") or "").strip()

        rows.append(
            {
                "ettn": str(extract_first_value(data, "ettn") or "").strip(),
                "belge_numarasi": str(extract_first_value(data, "belgeNumarasi") or "").strip(),
                "musteri_tc": normalize_tc_value(extract_first_value(data, "aliciVknTckn", "vknTckn")),
                "musteri_adi": str(musteri_adi or "").strip(),
                "islem_tarihi": normalize_portal_date(
                    extract_first_value(data, "belgeTarihi", "faturaTarihi")
                ),
                "onay_durumu": str(extract_first_value(data, "onayDurumu") or "").strip(),
            }
        )

    return pd.DataFrame(
        rows,
        columns=[
            "ettn",
            "belge_numarasi",
            "musteri_tc",
            "musteri_adi",
            "islem_tarihi",
            "onay_durumu",
        ],
    )


def build_gib_sync_key(record_date: object, musteri_tc: object, musteri_adi: object) -> str:
    return "|".join(
        [
            normalize_portal_date(record_date),
            normalize_tc_value(musteri_tc),
            normalize_person_name(str(musteri_adi or "")),
        ]
    )


def map_gib_sync_status(onay_durumu: str) -> str:
    normalized = str(onay_durumu or "").strip().casefold()
    if "onaylandı" in normalized or "onaylandi" in normalized:
        return "İmzalandı"
    if "onaylanmadı" in normalized or "onaylanmadi" in normalized:
        return "Taslak Oluşturuldu"
    if "silinmiş" in normalized or "silinmis" in normalized:
        return "GİB Taslağı Silinmiş"
    return str(onay_durumu or "Taslak Oluşturuldu").strip() or "Taslak Oluşturuldu"


def build_gib_sync_message(draft_row: dict) -> str:
    parts = [f"GİB durumu senkronize edildi: {draft_row.get('onay_durumu') or 'Bilinmiyor'}"]
    if draft_row.get("belge_numarasi"):
        parts.append(f"Belge No: {draft_row['belge_numarasi']}")
    if draft_row.get("ettn"):
        parts.append(f"ETTN: {draft_row['ettn']}")
    return " | ".join(parts)


def match_gib_drafts_to_transactions(
    transactions_df: pd.DataFrame,
    gib_drafts_df: pd.DataFrame,
) -> list[tuple[int, dict]]:
    if transactions_df.empty or gib_drafts_df.empty:
        return []

    matches: list[tuple[int, dict]] = []
    matched_transaction_ids: set[int] = set()
    matched_draft_keys: set[str] = set()

    if "GİB ETTN" in transactions_df.columns:
        draft_by_ettn = {
            str(row["ettn"]).strip(): row.to_dict()
            for _, row in gib_drafts_df.iterrows()
            if str(row.get("ettn") or "").strip()
        }
        for _, transaction in transactions_df.iterrows():
            ettn = str(transaction.get("GİB ETTN") or "").strip()
            if not ettn:
                continue
            draft_row = draft_by_ettn.get(ettn)
            if draft_row is None:
                continue
            transaction_id = int(transaction["id"])
            matches.append((transaction_id, draft_row))
            matched_transaction_ids.add(transaction_id)
            matched_draft_keys.add(ettn)

    remaining_transactions = transactions_df[
        ~transactions_df["id"].astype(int).isin(matched_transaction_ids)
    ].copy()
    remaining_drafts = gib_drafts_df[
        ~gib_drafts_df["ettn"].fillna("").astype(str).isin(matched_draft_keys)
    ].copy()

    if remaining_transactions.empty or remaining_drafts.empty:
        return matches

    remaining_transactions["sync_key"] = remaining_transactions.apply(
        lambda row: build_gib_sync_key(
            row.get("İşlem Tarihi"),
            row.get("T.C. Kimlik No"),
            row.get("Müşteri Adı"),
        ),
        axis=1,
    )
    remaining_drafts["sync_key"] = remaining_drafts.apply(
        lambda row: build_gib_sync_key(
            row.get("islem_tarihi"),
            row.get("musteri_tc"),
            row.get("musteri_adi"),
        ),
        axis=1,
    )

    unique_transaction_keys = remaining_transactions["sync_key"].value_counts()
    unique_draft_keys = remaining_drafts["sync_key"].value_counts()
    candidate_keys = [
        key
        for key in unique_transaction_keys.index
        if key
        and unique_transaction_keys.get(key, 0) == 1
        and unique_draft_keys.get(key, 0) == 1
    ]

    for sync_key in candidate_keys:
        transaction = remaining_transactions[remaining_transactions["sync_key"] == sync_key].iloc[0]
        draft_row = remaining_drafts[remaining_drafts["sync_key"] == sync_key].iloc[0].to_dict()
        matches.append((int(transaction["id"]), draft_row))

    return matches


def create_gib_portal_session(gib_kullanici: str, gib_sifre: str):
    from eArsivPortal import eArsivPortal
    from eArsivPortal.Libs.Oturum import legacy_session
    from eArsivPortal.Models.Komutlar import Komutlar

    portal = eArsivPortal.__new__(eArsivPortal)
    portal.kullanici_kodu = gib_kullanici
    portal.sifre = gib_sifre
    portal.test_modu = False
    portal.url = "https://earsivportal.efatura.gov.tr"
    portal.oturum = legacy_session()
    wrap_session_post_with_timeout(portal.oturum, GIB_REQUEST_TIMEOUT_SECONDS)
    portal.komutlar = Komutlar()
    portal.oturum.headers.update({"User-Agent": "https://github.com/keyiflerolsun/eArsivPortal"})
    portal.token = None
    portal.giris_yap()
    return portal


def wrap_session_post_with_timeout(session: requests.Session, timeout_seconds: int) -> None:
    original_post = session.post

    def post_with_timeout(self, *args, **kwargs):
        kwargs.setdefault("timeout", timeout_seconds)
        return original_post(*args, **kwargs)

    session.post = MethodType(post_with_timeout, session)


def try_create_gib_draft(
    *,
    gib_kullanici: str,
    gib_sifre: str,
    musteri_adi: str,
    musteri_tc: str,
    islem_tarihi: date,
    toplam_fatura: float,
) -> tuple[str, str, str | None]:
    if not gib_kullanici or not gib_sifre:
        return (
            "Kimlik Bekleniyor",
            "GİB kullanıcı kodu ve şifresi girilmediği için kayıt yalnızca veritabanına yazıldı.",
            None,
        )
    if find_spec("eArsivPortal") is None:
        return (
            "Kütüphane Eksik",
            "eArsivPortal kurulu değil. Terminalden `pip install eArsivPortal` çalıştırın.",
            None,
        )
    try:
        from eArsivPortal.Libs.FaturaVer import fatura_ver

        portal = create_gib_portal_session(gib_kullanici, gib_sifre)

        gib_ad, gib_soyad = split_customer_name(musteri_adi)
        kisi_bilgi = portal.kisi_getir(musteri_tc)
        payload = fatura_ver(
            tarih=islem_tarihi.strftime("%d/%m/%Y"),
            saat="12:00:00",
            vkn_veya_tckn=musteri_tc,
            ad=kisi_bilgi.adi or gib_ad,
            soyad=kisi_bilgi.soyadi or gib_soyad,
            unvan=kisi_bilgi.unvan or "",
            vergi_dairesi=kisi_bilgi.vergiDairesi or "",
            urun_adi="Dijital Hizmet Bedeli",
            fiyat=round(toplam_fatura, 2),
            fatura_notu="",
        )
        response = portal._eArsivPortal__kod_calistir(
            komut=portal.komutlar.FATURA_OLUSTUR, jp=payload
        )
        response_text = str(response.get("data", "")).strip()
        success_markers = (
            "başarıyla oluşturulmuştur",
            "basariyla olusturulmustur",
            "başarıyla oluşturulmu",
            "basariyla olusturulmu",
        )
        if any(m in response_text.casefold() for m in success_markers):
            return (
                "Taslak Oluşturuldu",
                "GİB Portalında taslak fatura başarıyla oluşturuldu.",
                str(payload.get("faturaUuid") or "").strip() or None,
            )
        if response_text:
            return "GİB Hatası", response_text, None
        return "GİB Hatası", "GİB Portalı taslak oluşturma isteğini tamamlamadı.", None
    except requests.Timeout:
        return (
            "Zaman Aşımı",
            f"GİB Portalı {GIB_REQUEST_TIMEOUT_SECONDS} saniye içinde yanıt vermedi.",
            None,
        )
    except requests.RequestException as exc:
        return "Bağlantı Hatası", f"GİB Portalına bağlanırken ağ hatası oluştu: {exc}", None
    except Exception as exc:
        msg = str(exc)
        if "unable to infer type" in msg or "ConfigError" in msg:
            if not (sys.version_info >= (3, 11) and sys.version_info < (3, 13)):
                return (
                    "Uyumluluk Hatası",
                    "eArsivPortal mevcut Python sürümüyle uyumlu görünmüyor. Python 3.11 veya 3.12 kullanın.",
                    None,
                )
        return "GİB Hatası", msg, None


def save_transaction(record: dict, db_path: Path = DATABASE_PATH) -> int:
    ts = now_iso()
    with closing(get_db_connection(db_path)) as conn, conn:
        cursor = conn.execute(
            """
            INSERT INTO transactions (
                islem_tarihi, musteri_adi, musteri_tc,
                satilan_usdt, alis_kuru, satis_kuru,
                vergisiz_bedel, kdv, toplam_fatura,
                gib_durumu, durum_mesaji,
                gib_ettn, gib_belge_numarasi, gib_son_senkron,
                arsiv_hafta_kodu, arsiv_etiketi,
                kaynak, olusturulma_zamani, guncellenme_zamani
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["İşlem Tarihi"],
                record["Müşteri Adı"],
                record["T.C. Kimlik No"],
                record.get("Satılan USDT", 0.0),
                record.get("Alış Kuru", 0.0),
                record.get("Satış Kuru", 0.0),
                record["Vergisiz Bedel"],
                record["KDV"],
                record["Toplam Fatura"],
                record["GİB Durumu"],
                record["Durum Mesajı"],
                record.get("GİB ETTN"),
                record.get("GİB Belge No"),
                record.get("GİB Son Senkron"),
                record.get("Arşiv Hafta Kodu"),
                record.get("Arşiv Etiketi"),
                record.get("Kaynak", "api"),
                ts,
                ts,
            ),
        )
        return int(cursor.lastrowid)


def update_gib_status_by_ids(
    transaction_ids: list[int],
    gib_durumu: str,
    durum_mesaji: str,
    *,
    db_path: Path = DATABASE_PATH,
) -> int:
    if not transaction_ids:
        return 0
    placeholders = ",".join("?" for _ in transaction_ids)
    with closing(get_db_connection(db_path)) as conn, conn:
        cursor = conn.execute(
            f"UPDATE transactions SET gib_durumu=?, durum_mesaji=?, guncellenme_zamani=? WHERE id IN ({placeholders})",
            (gib_durumu, durum_mesaji, now_iso(), *transaction_ids),
        )
    return max(cursor.rowcount, 0)


def update_gib_tracking(
    transaction_id: int,
    gib_durumu: str,
    durum_mesaji: str,
    *,
    gib_ettn: str | object = UNSET,
    gib_belge_numarasi: str | object = UNSET,
    gib_son_senkron: str | object = UNSET,
    db_path: Path = DATABASE_PATH,
) -> None:
    set_clauses = [
        "gib_durumu = ?",
        "durum_mesaji = ?",
        "guncellenme_zamani = ?",
    ]
    params: list[object] = [gib_durumu, durum_mesaji, now_iso()]
    if gib_ettn is not UNSET:
        set_clauses.append("gib_ettn = ?")
        params.append(gib_ettn)
    if gib_belge_numarasi is not UNSET:
        set_clauses.append("gib_belge_numarasi = ?")
        params.append(gib_belge_numarasi)
    if gib_son_senkron is not UNSET:
        set_clauses.append("gib_son_senkron = ?")
        params.append(gib_son_senkron)
    params.append(transaction_id)

    with closing(get_db_connection(db_path)) as conn, conn:
        conn.execute(
            f"UPDATE transactions SET {', '.join(set_clauses)} WHERE id = ?",
            params,
        )


def synchronize_gib_statuses(
    *,
    gib_kullanici: str,
    gib_sifre: str,
    transaction_ids: list[int] | None = None,
    db_path: Path = DATABASE_PATH,
) -> dict:
    transaction_df = load_transactions(db_path, archived=False)
    if transaction_ids:
        transaction_df = transaction_df[transaction_df["id"].isin(transaction_ids)].copy()
    if transaction_df.empty:
        return {
            "scanned_records": 0,
            "fetched_drafts": 0,
            "matched_records": 0,
            "signed_records": 0,
            "draft_records": 0,
            "deleted_records": 0,
            "unmatched_records": 0,
        }

    working_df = transaction_df.copy()
    working_df["İşlem Tarihi"] = pd.to_datetime(working_df["İşlem Tarihi"], errors="coerce")
    working_df = working_df.dropna(subset=["İşlem Tarihi"]).reset_index(drop=True)
    if working_df.empty:
        return {
            "scanned_records": 0,
            "fetched_drafts": 0,
            "matched_records": 0,
            "signed_records": 0,
            "draft_records": 0,
            "deleted_records": 0,
            "unmatched_records": 0,
        }

    portal = create_gib_portal_session(gib_kullanici, gib_sifre)
    try:
        drafts = portal.faturalari_getir(
            baslangic_tarihi=working_df["İşlem Tarihi"].min().strftime("%d/%m/%Y"),
            bitis_tarihi=working_df["İşlem Tarihi"].max().strftime("%d/%m/%Y"),
        )
    finally:
        try:
            portal.cikis_yap()
        except Exception:
            pass

    gib_drafts_df = normalize_gib_drafts(drafts)
    matches = match_gib_drafts_to_transactions(working_df, gib_drafts_df)
    sync_timestamp = now_iso()

    signed_records = 0
    draft_records = 0
    deleted_records = 0
    for transaction_id, draft_row in matches:
        local_status = map_gib_sync_status(str(draft_row.get("onay_durumu") or ""))
        if local_status == "İmzalandı":
            signed_records += 1
        elif local_status == "Taslak Oluşturuldu":
            draft_records += 1
        elif local_status == "GİB Taslağı Silinmiş":
            deleted_records += 1

        update_gib_tracking(
            transaction_id,
            local_status,
            build_gib_sync_message(draft_row),
            gib_ettn=str(draft_row.get("ettn") or "").strip() or None,
            gib_belge_numarasi=str(draft_row.get("belge_numarasi") or "").strip() or None,
            gib_son_senkron=sync_timestamp,
            db_path=db_path,
        )

    matched_records = len(matches)
    return {
        "scanned_records": int(len(working_df)),
        "fetched_drafts": int(len(gib_drafts_df)),
        "matched_records": matched_records,
        "signed_records": signed_records,
        "draft_records": draft_records,
        "deleted_records": deleted_records,
        "unmatched_records": int(len(working_df) - matched_records),
    }


def try_synchronize_gib_statuses(
    *,
    gib_kullanici: str,
    gib_sifre: str,
    transaction_ids: list[int] | None = None,
    db_path: Path = DATABASE_PATH,
) -> dict:
    if not gib_kullanici or not gib_sifre:
        return {
            "ok": False,
            "status": "Kimlik Bekleniyor",
            "message": "GİB senkronizasyonu için kullanıcı kodu ve şifre girin.",
        }
    if find_spec("eArsivPortal") is None:
        return {
            "ok": False,
            "status": "Kütüphane Eksik",
            "message": "eArsivPortal kurulu değil. Terminalden `pip install eArsivPortal` çalıştırın.",
        }

    try:
        result = synchronize_gib_statuses(
            gib_kullanici=gib_kullanici,
            gib_sifre=gib_sifre,
            transaction_ids=transaction_ids,
            db_path=db_path,
        )
    except requests.Timeout:
        return {
            "ok": False,
            "status": "Zaman Aşımı",
            "message": f"GİB Portalı {GIB_REQUEST_TIMEOUT_SECONDS} saniye içinde yanıt vermedi.",
        }
    except requests.RequestException as exc:
        return {
            "ok": False,
            "status": "Bağlantı Hatası",
            "message": f"GİB Portalına bağlanırken ağ hatası oluştu: {exc}",
        }
    except Exception as exc:
        msg = str(exc)
        if "unable to infer type" in msg or "ConfigError" in msg:
            if not (sys.version_info >= (3, 11) and sys.version_info < (3, 13)):
                return {
                    "ok": False,
                    "status": "Uyumluluk Hatası",
                    "message": "eArsivPortal mevcut Python sürümüyle uyumlu görünmüyor. Python 3.11 veya 3.12 kullanın.",
                }
        return {"ok": False, "status": "GİB Hatası", "message": msg}

    result["ok"] = True
    result["status"] = "Senkronize Edildi"
    if result["matched_records"]:
        result["message"] = (
            f"{result['matched_records']} kayıt GİB ile eşleşti. "
            f"İmzalı: {result['signed_records']}, taslak: {result['draft_records']}, "
            f"silinmiş: {result['deleted_records']}, eşleşmeyen: {result['unmatched_records']}."
        )
    else:
        result["message"] = (
            f"{result['scanned_records']} kayıt tarandı ancak eşleşen GİB taslağı bulunamadı. "
            f"Portalda çekilen taslak sayısı: {result['fetched_drafts']}."
        )
    return result


def get_finance_summary(df: pd.DataFrame) -> dict:
    filtered_df = filter_transactions_for_statistics(df)
    if filtered_df.empty:
        return {
            "toplam_kayit": 0,
            "toplam_fatura": 0.0,
            "vergisiz_bedel": 0.0,
            "toplam_kdv": 0.0,
            "ortalama_fatura": 0.0,
            "aktif_toplam_fatura": 0.0,
            "arsiv_toplam_fatura": 0.0,
            "bu_ay_toplam": 0.0,
            "onceki_ay_toplam": 0.0,
            "ay_degisim_orani": 0.0,
            "imzali_kayit": 0,
            "taslak_kayit": 0,
            "imza_orani": 0.0,
            "daily_summary": [],
            "monthly_summary": [],
            "status_summary": [],
            "top_customers": [],
        }

    working_df = filtered_df.copy()
    working_df["İşlem Tarihi"] = pd.to_datetime(working_df["İşlem Tarihi"], errors="coerce")
    working_df = working_df.dropna(subset=["İşlem Tarihi"]).reset_index(drop=True)
    if working_df.empty:
        return get_finance_summary(pd.DataFrame(columns=df.columns))

    month_start = pd.Timestamp(date.today().replace(day=1))
    previous_month_end = month_start - pd.Timedelta(days=1)
    previous_month_start = pd.Timestamp(previous_month_end.date().replace(day=1))
    active_mask = working_df["Arşiv Hafta Kodu"].isna() | working_df["Arşiv Hafta Kodu"].astype(str).str.strip().eq("")
    current_month_df = working_df[working_df["İşlem Tarihi"] >= month_start]
    previous_month_df = working_df[
        (working_df["İşlem Tarihi"] >= previous_month_start)
        & (working_df["İşlem Tarihi"] <= previous_month_end)
    ]

    toplam_kayit = int(len(working_df))
    toplam_fatura = round(float(working_df["Toplam Fatura"].sum()), 2)
    bu_ay_toplam = round(float(current_month_df["Toplam Fatura"].sum()), 2)
    onceki_ay_toplam = round(float(previous_month_df["Toplam Fatura"].sum()), 2)
    ay_degisim_orani = 0.0
    if onceki_ay_toplam:
        ay_degisim_orani = round(((bu_ay_toplam - onceki_ay_toplam) / onceki_ay_toplam) * 100, 2)

    daily_summary = (
        working_df.assign(Tarih=working_df["İşlem Tarihi"].dt.strftime("%Y-%m-%d"))
        .groupby("Tarih")[["Toplam Fatura", "KDV"]]
        .sum()
        .sort_index()
        .tail(30)
        .reset_index()
    )
    daily_summary[["Toplam Fatura", "KDV"]] = daily_summary[["Toplam Fatura", "KDV"]].round(2)

    monthly_summary = (
        working_df.assign(Dönem=working_df["İşlem Tarihi"].dt.strftime("%Y-%m"))
        .groupby("Dönem")
        .agg(
            kayit_adedi=("id", "count"),
            vergisiz_bedel=("Vergisiz Bedel", "sum"),
            toplam_kdv=("KDV", "sum"),
            toplam_fatura=("Toplam Fatura", "sum"),
        )
        .reset_index()
        .sort_values("Dönem", ascending=False)
        .head(6)
    )
    for column in ["vergisiz_bedel", "toplam_kdv", "toplam_fatura"]:
        monthly_summary[column] = monthly_summary[column].round(2)

    status_summary = (
        working_df.groupby("GİB Durumu")
        .agg(kayit_adedi=("id", "count"), toplam_fatura=("Toplam Fatura", "sum"))
        .reset_index()
        .sort_values(["kayit_adedi", "toplam_fatura"], ascending=[False, False])
    )
    status_summary["toplam_fatura"] = status_summary["toplam_fatura"].round(2)

    top_customers = (
        working_df.groupby("Müşteri Adı")
        .agg(kayit_adedi=("id", "count"), toplam_fatura=("Toplam Fatura", "sum"), toplam_kdv=("KDV", "sum"))
        .reset_index()
        .sort_values(["toplam_fatura", "kayit_adedi"], ascending=[False, False])
        .head(8)
    )
    top_customers[["toplam_fatura", "toplam_kdv"]] = top_customers[["toplam_fatura", "toplam_kdv"]].round(2)

    imzali_kayit = int((working_df["GİB Durumu"] == "İmzalandı").sum())
    taslak_kayit = int((working_df["GİB Durumu"] == "Taslak Oluşturuldu").sum())

    return {
        "toplam_kayit": toplam_kayit,
        "toplam_fatura": toplam_fatura,
        "vergisiz_bedel": round(float(working_df["Vergisiz Bedel"].sum()), 2),
        "toplam_kdv": round(float(working_df["KDV"].sum()), 2),
        "ortalama_fatura": round(toplam_fatura / toplam_kayit, 2) if toplam_kayit else 0.0,
        "aktif_toplam_fatura": round(float(working_df.loc[active_mask, "Toplam Fatura"].sum()), 2),
        "arsiv_toplam_fatura": round(float(working_df.loc[~active_mask, "Toplam Fatura"].sum()), 2),
        "bu_ay_toplam": bu_ay_toplam,
        "onceki_ay_toplam": onceki_ay_toplam,
        "ay_degisim_orani": ay_degisim_orani,
        "imzali_kayit": imzali_kayit,
        "taslak_kayit": taslak_kayit,
        "imza_orani": round((imzali_kayit / toplam_kayit) * 100, 2) if toplam_kayit else 0.0,
        "daily_summary": daily_summary.to_dict(orient="records"),
        "monthly_summary": monthly_summary.to_dict(orient="records"),
        "status_summary": status_summary.to_dict(orient="records"),
        "top_customers": top_customers.to_dict(orient="records"),
    }


# ── Pydantic modelleri ────────────────────────────────────────────────────────

class AutoTransactionIn(BaseModel):
    musteri_adi: str
    musteri_tc: str = DEFAULT_TC
    islem_tarihi: str
    satilan_usdt: float
    alis_kuru: float
    satis_kuru: float
    gib_kullanici: str = ""
    gib_sifre: str = ""


class ManualTransactionIn(BaseModel):
    musteri_adi: str
    musteri_tc: str = DEFAULT_TC
    islem_tarihi: str
    vergisiz_bedel: float
    kdv: float
    toplam_fatura: float
    gib_kullanici: str = ""
    gib_sifre: str = ""
    archive_key: Optional[str] = None
    archive_label: Optional[str] = None


class StatusUpdateIn(BaseModel):
    ids: list[int]
    gib_durumu: str
    durum_mesaji: str = ""


class GibSyncIn(BaseModel):
    ids: list[int] = []
    gib_kullanici: str = ""
    gib_sifre: str = ""


class DeleteIn(BaseModel):
    ids: list[int]


class ArchiveIn(BaseModel):
    ids: list[int]
    archive_key: Optional[str] = None
    archive_label: str = ""


class LoginIn(BaseModel):
    username: str
    password: str


class ExpenseIn(BaseModel):
    islem_tarihi: str
    aciklama: str
    kategori: str
    toplam_tutar: float
    kdv_orani: float
    ticari_arac: bool = False
    file_name: str
    file_content_base64: str


# ── Rotalar ───────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def serve_html():
    if HTML_PATH.exists():
        return FileResponse(HTML_PATH)
    return HTMLResponse("<h1>p2p_panel.html bulunamadı.</h1>", status_code=404)


@app.post("/api/auth/login")
def auth_login(data: LoginIn):
    if not verify_panel_credentials(data.username, data.password):
        raise HTTPException(401, "Kullanıcı adı veya şifre hatalı.")
    token = create_panel_session(data.username)
    return {
        "authenticated": True,
        "token": token,
        "username": data.username.strip(),
        "expires_in_hours": PANEL_SESSION_TTL_HOURS,
    }


@app.get("/api/auth/status")
def auth_status(request: Request):
    token = str(request.headers.get("X-Panel-Token", "")).strip()
    session = get_panel_session(token) if token else None
    return {
        "authenticated": session is not None,
        "username": session.get("username") if session else None,
    }


@app.post("/api/auth/logout")
def auth_logout(request: Request):
    token = str(request.headers.get("X-Panel-Token", "")).strip()
    if token:
        invalidate_panel_session(token)
    return {"authenticated": False}


@app.get("/api/dashboard/metrics")
def dashboard_metrics():
    df = load_transactions(archived=None)
    df = filter_transactions_for_statistics(df)
    if df.empty:
        return {
            "bugun": 0, "hafta": 0, "ay": 0,
            "toplam_kdv": 0, "toplam_kayit": 0, "en_aktif_musteri": "",
        }

    today_str = date.today().strftime("%Y-%m-%d")
    hafta = get_hafta_bilgisi()
    pazartesi_str = hafta["pazartesi"].strftime("%Y-%m-%d")
    ay_bas = date.today().replace(day=1).strftime("%Y-%m-%d")

    def flt_sum(start: str, end: str) -> float:
        mask = (df["İşlem Tarihi"] >= start) & (df["İşlem Tarihi"] <= end)
        return float(df.loc[mask, "Toplam Fatura"].sum())

    bugun = flt_sum(today_str, today_str)
    hafta_toplam = flt_sum(pazartesi_str, today_str)
    ay_toplam = flt_sum(ay_bas, today_str)
    toplam_kdv = float(df["KDV"].sum())
    toplam_kayit = int(len(df))
    en_aktif = ""
    if not df.empty:
        counts = df["Müşteri Adı"].value_counts()
        if not counts.empty:
            en_aktif = str(counts.idxmax())

    return {
        "bugun": round(bugun, 2),
        "hafta": round(hafta_toplam, 2),
        "ay": round(ay_toplam, 2),
        "toplam_kdv": round(toplam_kdv, 2),
        "toplam_kayit": toplam_kayit,
        "en_aktif_musteri": en_aktif,
    }


@app.get("/api/transactions")
def get_transactions(archived: Optional[str] = Query(None)):
    if archived == "true":
        df = load_transactions(archived=True)
    elif archived == "false":
        df = load_transactions(archived=False)
    else:
        df = load_transactions(archived=None)
    return df.to_dict(orient="records")


@app.get("/api/transactions/popular-usdt")
def popular_usdt():
    return load_popular_usdt_values()


@app.post("/api/transactions/auto")
def create_auto_transaction(data: AutoTransactionIn):
    try:
        islem_tarihi = date.fromisoformat(data.islem_tarihi)
    except ValueError:
        raise HTTPException(400, "Geçersiz tarih formatı.")

    tc = data.musteri_tc.strip()
    if not data.musteri_adi.strip():
        raise HTTPException(400, "Müşteri adı boş bırakılamaz.")
    if not tc.isdigit() or len(tc) != 11:
        raise HTTPException(400, "T.C. Kimlik No 11 haneli rakam olmalıdır.")
    if islem_tarihi > date.today():
        raise HTTPException(400, "İşlem tarihi gelecekte olamaz.")
    if data.satilan_usdt <= 0:
        raise HTTPException(400, "Satılan USDT sıfırdan büyük olmalıdır.")
    if data.alis_kuru <= 0 or data.satis_kuru <= 0:
        raise HTTPException(400, "Alış ve satış kuru sıfırdan büyük olmalıdır.")
    if data.satis_kuru < data.alis_kuru:
        raise HTTPException(400, "Satış kuru, alış kurundan küçük olamaz.")

    invoice = calculate_invoice(data.satilan_usdt, data.alis_kuru, data.satis_kuru)
    gib_durumu, gib_mesaji, gib_ettn = try_create_gib_draft(
        gib_kullanici=data.gib_kullanici,
        gib_sifre=data.gib_sifre,
        musteri_adi=data.musteri_adi,
        musteri_tc=tc,
        islem_tarihi=islem_tarihi,
        toplam_fatura=invoice["toplam_fatura"],
    )
    record = {
        "İşlem Tarihi": islem_tarihi.isoformat(),
        "Müşteri Adı": data.musteri_adi.strip(),
        "T.C. Kimlik No": tc,
        "Satılan USDT": data.satilan_usdt,
        "Alış Kuru": data.alis_kuru,
        "Satış Kuru": data.satis_kuru,
        "Vergisiz Bedel": invoice["vergisiz_bedel"],
        "KDV": invoice["kdv"],
        "Toplam Fatura": invoice["toplam_fatura"],
        "GİB Durumu": gib_durumu,
        "Durum Mesajı": gib_mesaji,
        "GİB ETTN": gib_ettn,
        "GİB Son Senkron": now_iso() if gib_ettn else None,
        "Kaynak": "api",
    }
    new_id = save_transaction(record)
    return {**record, "id": new_id, "gib_durumu": gib_durumu, "gib_mesaji": gib_mesaji}


@app.post("/api/transactions/manual")
def create_manual_transaction(data: ManualTransactionIn):
    try:
        islem_tarihi = date.fromisoformat(data.islem_tarihi)
    except ValueError:
        raise HTTPException(400, "Geçersiz tarih formatı.")

    tc = data.musteri_tc.strip()
    if not data.musteri_adi.strip():
        raise HTTPException(400, "Müşteri adı boş bırakılamaz.")
    if not tc.isdigit() or len(tc) != 11:
        raise HTTPException(400, "T.C. Kimlik No 11 haneli rakam olmalıdır.")
    if islem_tarihi > date.today():
        raise HTTPException(400, "İşlem tarihi gelecekte olamaz.")

    beklenen = round(data.vergisiz_bedel + data.kdv, 2)
    if round(data.toplam_fatura, 2) != beklenen:
        raise HTTPException(400, f"Toplam fatura {beklenen:.2f} TL olmalıdır.")

    archive_key = data.archive_key
    archive_label = (data.archive_label or "").strip() or None

    if archive_key == "new":
        archive_key = make_archive_key(archive_label or "arsiv", islem_tarihi)
    elif not archive_key:
        archive_key = None
        archive_label = None

    gib_durumu, gib_mesaji, gib_ettn = try_create_gib_draft(
        gib_kullanici=data.gib_kullanici,
        gib_sifre=data.gib_sifre,
        musteri_adi=data.musteri_adi,
        musteri_tc=tc,
        islem_tarihi=islem_tarihi,
        toplam_fatura=data.toplam_fatura,
    )
    record = {
        "İşlem Tarihi": islem_tarihi.isoformat(),
        "Müşteri Adı": data.musteri_adi.strip(),
        "T.C. Kimlik No": tc,
        "Satılan USDT": 0.0,
        "Alış Kuru": 0.0,
        "Satış Kuru": 0.0,
        "Vergisiz Bedel": round(data.vergisiz_bedel, 2),
        "KDV": round(data.kdv, 2),
        "Toplam Fatura": round(data.toplam_fatura, 2),
        "GİB Durumu": gib_durumu,
        "Durum Mesajı": gib_mesaji,
        "GİB ETTN": gib_ettn,
        "GİB Son Senkron": now_iso() if gib_ettn else None,
        "Arşiv Hafta Kodu": archive_key,
        "Arşiv Etiketi": archive_label,
        "Kaynak": "api",
    }
    new_id = save_transaction(record)
    return {**record, "id": new_id, "gib_durumu": gib_durumu, "gib_mesaji": gib_mesaji}


@app.put("/api/transactions/status")
def update_status(data: StatusUpdateIn):
    if not data.ids:
        raise HTTPException(400, "ID listesi boş olamaz.")
    updated = update_gib_status_by_ids(data.ids, data.gib_durumu, data.durum_mesaji)
    return {"updated": updated}


@app.post("/api/transactions/sync-gib")
def sync_gib_statuses_endpoint(data: GibSyncIn):
    result = try_synchronize_gib_statuses(
        gib_kullanici=data.gib_kullanici,
        gib_sifre=data.gib_sifre,
        transaction_ids=data.ids or None,
    )
    if not result.get("ok"):
        status_code = 400 if result.get("status") in {"Kimlik Bekleniyor", "Kütüphane Eksik"} else 502
        raise HTTPException(status_code, result.get("message", "Senkronizasyon başarısız."))
    return result


@app.delete("/api/transactions")
def delete_transactions_endpoint(data: DeleteIn):
    if not data.ids:
        raise HTTPException(400, "ID listesi boş olamaz.")
    placeholders = ",".join("?" for _ in data.ids)
    with closing(get_db_connection()) as conn, conn:
        cursor = conn.execute(
            f"DELETE FROM transactions WHERE arsiv_hafta_kodu IS NULL AND id IN ({placeholders})",
            tuple(data.ids),
        )
        conn.commit()
    return {"deleted": max(cursor.rowcount, 0)}


@app.post("/api/transactions/archive")
def move_to_archive(data: ArchiveIn):
    if not data.ids:
        raise HTTPException(400, "ID listesi boş olamaz.")

    if data.archive_key and data.archive_key != "new":
        archive_key = data.archive_key
        groups = load_archive_groups()
        match = groups[groups["Arşiv Hafta Kodu"] == archive_key]
        archive_label = (
            str(match["Arşiv Etiketi"].iloc[0]) if not match.empty else archive_key
        )
    else:
        archive_label = (
            data.archive_label.strip()
            or f"Arşiv {date.today().strftime('%d.%m.%Y')}"
        )
        archive_key = make_archive_key(archive_label, date.today())

    placeholders = ",".join("?" for _ in data.ids)
    with closing(get_db_connection()) as conn, conn:
        cursor = conn.execute(
            f"UPDATE transactions SET arsiv_hafta_kodu=?, arsiv_etiketi=?, guncellenme_zamani=?"
            f" WHERE id IN ({placeholders})",
            (archive_key, archive_label, now_iso(), *data.ids),
        )
        conn.commit()

    return {
        "moved": max(cursor.rowcount, 0),
        "archive_key": archive_key,
        "archive_label": archive_label,
    }


@app.get("/api/archives")
def list_archives():
    df = load_archive_groups()
    if df.empty:
        return []
    result = []
    for _, row in df.iterrows():
        result.append({
            "key": row["Arşiv Hafta Kodu"],
            "label": row["Arşiv Etiketi"],
            "kayit_adedi": int(row["Kayıt Adedi"]),
            "toplam_fatura": float(row["Toplam Fatura"]),
            "toplam_kdv": float(row["KDV"]),
            "vergisiz_bedel": float(row["Vergisiz Bedel"]),
        })
    return result


@app.post("/api/archives/close-week")
def close_week(archive_label: str = Query(default="")):
    active_df = load_transactions(archived=False)
    if active_df.empty:
        raise HTTPException(400, "Arşive taşınacak aktif işlem bulunamadı.")

    hafta = get_hafta_bilgisi()
    arc_key = hafta["hafta_kodu"]
    arc_label = archive_label.strip() or hafta["etiket"]

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    file_path = ARCHIVE_DIR / hafta["dosya_adi"]
    if file_path.exists():
        file_path = ARCHIVE_DIR / (
            f"{file_path.stem}_{datetime.now().strftime('%H%M%S')}{file_path.suffix}"
        )

    export_df = build_export_dataframe(active_df)
    file_path.write_bytes(df_to_xlsx_bytes(export_df, sheet_name="Arsiv"))

    active_ids = active_df["id"].astype(int).tolist()
    placeholders = ",".join("?" for _ in active_ids)
    with closing(get_db_connection()) as conn, conn:
        conn.execute(
            f"UPDATE transactions SET arsiv_hafta_kodu=?, arsiv_etiketi=?, guncellenme_zamani=?"
            f" WHERE arsiv_hafta_kodu IS NULL AND id IN ({placeholders})",
            (arc_key, arc_label, now_iso(), *active_ids),
        )
        conn.commit()

    return {
        "archived": len(active_ids),
        "archive_label": arc_label,
        "backup_file": file_path.name,
    }


@app.get("/api/archives/{archive_key:path}")
def get_archive_detail(archive_key: str):
    with closing(get_db_connection()) as conn:
        rows = conn.execute(
            """
            SELECT id, islem_tarihi, musteri_adi, musteri_tc,
                   vergisiz_bedel, kdv, toplam_fatura,
                   gib_durumu, arsiv_hafta_kodu, arsiv_etiketi
            FROM transactions
            WHERE arsiv_hafta_kodu = ?
            ORDER BY date(islem_tarihi) DESC, id DESC
            """,
            (archive_key,),
        ).fetchall()

    return [
        {
            "id": r["id"],
            "İşlem Tarihi": r["islem_tarihi"],
            "Müşteri Adı": r["musteri_adi"],
            "T.C. Kimlik No": r["musteri_tc"],
            "Vergisiz Bedel": float(r["vergisiz_bedel"] or 0),
            "KDV": float(r["kdv"] or 0),
            "Toplam Fatura": float(r["toplam_fatura"] or 0),
            "GİB Durumu": r["gib_durumu"],
            "Arşiv Hafta Kodu": r["arsiv_hafta_kodu"],
            "Arşiv Etiketi": r["arsiv_etiketi"],
        }
        for r in rows
    ]


@app.delete("/api/archives/{archive_key:path}")
def restore_archive(archive_key: str):
    """Arşivi aktif listeye geri al (arsiv_hafta_kodu = NULL)."""
    with closing(get_db_connection()) as conn, conn:
        cursor = conn.execute(
            "UPDATE transactions SET arsiv_hafta_kodu=NULL, arsiv_etiketi=NULL,"
            " guncellenme_zamani=? WHERE arsiv_hafta_kodu=?",
            (now_iso(), archive_key),
        )
        conn.commit()
    return {"restored": max(cursor.rowcount, 0)}


@app.get("/api/export/xlsx")
def export_xlsx(
    archived: Optional[str] = Query(None),
    archive_key: Optional[str] = Query(None),
):
    if archive_key:
        df = load_transactions(archived=True)
        df = df[df["Arşiv Hafta Kodu"] == archive_key]
        filename = f"arsiv_{archive_key}.xlsx"
    elif archived == "false":
        df = load_transactions(archived=False)
        filename = f"aktif_{date.today()}.xlsx"
    else:
        df = load_transactions(archived=None)
        filename = f"tum_islemler_{date.today()}.xlsx"

    xlsx_bytes = df_to_xlsx_bytes(build_export_dataframe(df), sheet_name="Faturalar")
    safe_name = filename.encode("ascii", "replace").decode()
    return StreamingResponse(
        BytesIO(xlsx_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{safe_name}"'},
    )


@app.get("/api/statistics")
def statistics(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    df = load_transactions(archived=None)
    df = filter_transactions_for_statistics(df)
    if df.empty:
        return {"toplam_kayit": 0}

    if start_date:
        df = df[df["İşlem Tarihi"] >= start_date]
    if end_date:
        df = df[df["İşlem Tarihi"] <= end_date]

    if df.empty:
        return {"toplam_kayit": 0}

    toplam_fatura = float(df["Toplam Fatura"].sum())
    vergisiz_bedel = float(df["Vergisiz Bedel"].sum())
    toplam_kdv = float(df["KDV"].sum())
    toplam_kayit = int(len(df))
    ortalama_fatura = round(toplam_fatura / toplam_kayit, 2) if toplam_kayit else 0.0
    tekil_musteri = int(df["Müşteri Adı"].nunique())

    daily_trend = (
        df.groupby("İşlem Tarihi")["Toplam Fatura"]
        .sum()
        .reset_index()
        .rename(columns={"İşlem Tarihi": "Tarih"})
        .sort_values("Tarih")
    )
    daily_trend["Toplam Fatura"] = daily_trend["Toplam Fatura"].round(2)

    top_customers = (
        df.groupby("Müşteri Adı")
        .agg(kayit_adedi=("id", "count"), toplam_fatura=("Toplam Fatura", "sum"))
        .reset_index()
        .sort_values("toplam_fatura", ascending=False)
        .head(10)
    )
    top_customers["toplam_fatura"] = top_customers["toplam_fatura"].round(2)

    gib_distribution = df["GİB Durumu"].value_counts().to_dict()

    return {
        "toplam_kayit": toplam_kayit,
        "toplam_fatura": round(toplam_fatura, 2),
        "vergisiz_bedel": round(vergisiz_bedel, 2),
        "toplam_kdv": round(toplam_kdv, 2),
        "ortalama_fatura": ortalama_fatura,
        "tekil_musteri": tekil_musteri,
        "daily_trend": daily_trend.to_dict(orient="records"),
        "top_customers": top_customers.to_dict(orient="records"),
        "gib_distribution": gib_distribution,
    }


@app.get("/api/finance-summary")
def finance_summary():
    df = load_transactions(archived=None)
    return get_finance_summary(df)


@app.get("/api/expenses")
def get_expenses(month_key: Optional[str] = Query(None)):
    df = load_expenses(month_key=month_key)
    if df.empty:
        return []
    records = df.to_dict(orient="records")
    for record in records:
        record["tarih"] = str(record.get("tarih") or "")
    return records


@app.get("/api/expenses/months")
def expense_months():
    return get_expense_months()


@app.get("/api/expenses/summary")
def expense_summary(month_key: Optional[str] = Query(None)):
    return summarize_expenses(load_expenses(month_key=month_key))


@app.get("/api/expenses/audit")
def expense_audit(month_key: Optional[str] = Query(None)):
    return build_expense_audit(load_expenses(month_key=month_key))


@app.post("/api/expenses")
def create_expense(data: ExpenseIn):
    try:
        islem_tarihi = date.fromisoformat(data.islem_tarihi)
    except ValueError:
        raise HTTPException(400, "Geçersiz gider tarihi formatı.")

    validate_expense_input(
        islem_tarihi,
        data.aciklama,
        data.kategori,
        data.toplam_tutar,
        data.kdv_orani,
        data.file_name,
    )

    try:
        file_bytes = base64.b64decode(data.file_content_base64.encode("utf-8"), validate=True)
    except (ValueError, binascii.Error):
        raise HTTPException(400, "Fatura dosyası çözümlenemedi.")

    if not file_bytes:
        raise HTTPException(400, "Fatura dosyası boş olamaz.")

    breakdown = calculate_expense_breakdown(
        data.toplam_tutar,
        data.kdv_orani,
        data.kategori,
        data.ticari_arac,
    )
    record = {
        "islem_tarihi": islem_tarihi.isoformat(),
        "aciklama": data.aciklama.strip(),
        "kategori": data.kategori,
        "toplam_tutar": round(float(data.toplam_tutar), 2),
        "kdv_orani": round(float(data.kdv_orani), 2),
        "kdv_tutari": breakdown["kdv_tutari"],
        "net_gider": breakdown["net_gider"],
        "gider_yazim_orani": breakdown["gider_yazim_orani"],
        "vergi_matrahi": breakdown["vergi_matrahi"],
        "indirilecek_kdv": breakdown["indirilecek_kdv"],
        "vergi_kalkani": breakdown["vergi_kalkani"],
        "ticari_arac": data.ticari_arac,
        "fatura_dosya_yolu": None,
        "fatura_orijinal_adi": data.file_name,
    }
    expense_id = save_expense(record)
    saved_path = attach_expense_invoice(
        expense_id,
        data.file_name,
        file_bytes,
        islem_tarihi=islem_tarihi,
        aciklama=data.aciklama,
    )

    return {
        "id": expense_id,
        **record,
        **breakdown,
        "fatura_dosya_yolu": str(saved_path.resolve()),
        "fatura_orijinal_adi": data.file_name,
    }


@app.post("/api/expenses/{expense_id}/open")
def open_expense_invoice(expense_id: int):
    with closing(get_db_connection()) as conn:
        row = conn.execute(
            "SELECT fatura_dosya_yolu FROM expenses WHERE id = ? LIMIT 1",
            (expense_id,),
        ).fetchone()
    if row is None or not str(row["fatura_dosya_yolu"] or "").strip():
        raise HTTPException(404, "Fatura dosyası bulunamadı.")

    file_path = Path(str(row["fatura_dosya_yolu"])).resolve()
    if not file_path.exists():
        raise HTTPException(404, "Fatura dosyası diskte bulunamadı.")

    try:
        open_file_with_default_app(file_path)
    except Exception as exc:
        raise HTTPException(500, f"Fatura açılamadı: {exc}")

    return {"opened": True, "path": str(file_path)}


@app.get("/api/expenses/report/xlsx")
def expense_report_xlsx(month_key: Optional[str] = Query(None)):
    selected_month = month_key or date.today().strftime("%Y-%m")
    df = load_expenses(month_key=selected_month)
    xlsx_bytes = build_expense_report_xlsx_bytes(df, selected_month)
    filename = f"gider_raporu_{selected_month}.xlsx"
    return StreamingResponse(
        BytesIO(xlsx_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/expenses/report/csv")
def expense_report_csv(month_key: Optional[str] = Query(None)):
    selected_month = month_key or date.today().strftime("%Y-%m")
    df = load_expenses(month_key=selected_month)
    csv_bytes = build_expense_report_csv_bytes(df)
    filename = f"gider_raporu_{selected_month}.csv"
    return StreamingResponse(
        BytesIO(csv_bytes),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/system/backup")
def system_backup():
    backup_path = create_backup_archive()
    return FileResponse(
        backup_path,
        filename=backup_path.name,
        media_type="application/zip",
    )


# ── Başlangıç ─────────────────────────────────────────────────────────────────

@app.on_event("startup")
def startup():
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    EXPENSE_INVOICE_DIR.mkdir(parents=True, exist_ok=True)
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ensure_database()
    auth_config = ensure_panel_auth_config()
    daily_backup = ensure_daily_backup()
    print(f"✅ Veritabanı: {DATABASE_PATH}")
    print(f"✅ HTML      : {HTML_PATH}")
    print(f"✅ Giriş     : {PANEL_AUTH_PATH}")
    print(f"🔐 Kullanıcı : {auth_config.get('username')}")
    if not auth_config.get("password_changed"):
        print(f"🔐 Şifre     : {auth_config.get('password')}")
        print("⚠️ İlk girişten sonra panel_auth.json içinden şifreyi değiştirin.")
    if daily_backup is not None:
        print(f"✅ Günlük yedek: {daily_backup}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("gib_fatura_api:app", host="0.0.0.0", port=8000, reload=True)
