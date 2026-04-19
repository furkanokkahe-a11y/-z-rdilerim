from contextlib import closing
from datetime import date, datetime, timedelta
from importlib.util import find_spec
from io import BytesIO
import os
from pathlib import Path
import re
import subprocess
import sys
from types import MethodType
import sqlite3
import unicodedata

import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components


BASE_DIR = Path(__file__).resolve().parent
DATABASE_PATH = BASE_DIR / "gib_fatura.db"
LEGACY_CSV_PATH = BASE_DIR / "islem_gecmisi.csv"
ARCHIVE_DIR = BASE_DIR / "arsivler"
EXPENSE_DIR = BASE_DIR / "giderler"
EXPENSE_INVOICE_DIR = EXPENSE_DIR / "faturalar"
OLD_ARCHIVE_DIR = BASE_DIR / "arşiv"

ISTENEN_KOLONLAR = ["İşlem Tarihi", "Müşteri Adı", "Vergisiz Bedel", "KDV", "Toplam Fatura"]
GORUNEN_KOLONLAR = ["İşlem Tarihi", "Müşteri Adı", "Vergisiz Bedel", "KDV", "Toplam Fatura", "GİB Durumu"]
EXPORT_KOLONLARI = ["İşlem Tarihi", "Müşteri Adı", "T.C. Kimlik No", "Vergisiz Bedel", "KDV", "Toplam Fatura", "GİB Durumu"]
NUMERIC_KOLONLAR = ["Vergisiz Bedel", "KDV", "Toplam Fatura", "Satılan USDT", "Alış Kuru", "Satış Kuru"]
ARCHIVE_MATCH_KOLONLARI = ["İşlem Tarihi", "Müşteri Adı", "T.C. Kimlik No", "Vergisiz Bedel", "KDV", "Toplam Fatura"]
DEFAULT_TC = "11111111111"
ACTIVE_SELECTION_STATE_KEY = "active_selected_ids"
ACTIVE_SELECTION_VERSION_KEY = "active_selection_version"
DEFAULT_GIB_KULLANICI_KODU = "63704025"
GIB_REQUEST_TIMEOUT_SECONDS = 20
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
UNSET = object()


def now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat(sep=" ")


def df_to_xlsx_bytes(df: pd.DataFrame, sheet_name: str = "Sayfa1") -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()


def get_hafta_bilgisi(referans_tarihi: date | None = None) -> dict:
    bugun = referans_tarihi or date.today()
    yil, hafta_no, _ = bugun.isocalendar()
    pazartesi = bugun - timedelta(days=bugun.weekday())
    pazar = pazartesi + timedelta(days=6)
    return {
        "hafta_kodu": f"{yil}_{hafta_no:02d}",
        "etiket": f"{pazartesi.strftime('%d.%m.%Y')} - {pazar.strftime('%d.%m.%Y')}",
        "dosya_adi": (
            f"arsiv_hafta_{yil}_{hafta_no:02d}_{pazartesi.strftime('%d.%m')}_{pazar.strftime('%d.%m')}.xlsx"
        ),
        "pazartesi": pazartesi,
        "pazar": pazar,
    }


def get_db_connection(db_path: Path = DATABASE_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
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


def ensure_storage() -> None:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    EXPENSE_INVOICE_DIR.mkdir(parents=True, exist_ok=True)


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
                kaynak TEXT NOT NULL DEFAULT 'uygulama',
                olusturulma_zamani TEXT NOT NULL,
                guncellenme_zamani TEXT NOT NULL
            )
            """
        )
        ensure_transaction_columns(conn)
        ensure_expense_tables(conn)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS imported_files (
                dosya_anahtari TEXT PRIMARY KEY,
                aktarim_zamani TEXT NOT NULL
            )
            """
        )


def mark_file_imported(conn: sqlite3.Connection, file_key: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO imported_files (dosya_anahtari, aktarim_zamani) VALUES (?, ?)",
        (file_key, now_iso()),
    )


def is_file_imported(conn: sqlite3.Connection, file_key: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM imported_files WHERE dosya_anahtari = ? LIMIT 1",
        (file_key,),
    ).fetchone()
    return row is not None


def normalize_legacy_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    normalized = pd.DataFrame()
    normalized["İşlem Tarihi"] = pd.to_datetime(
        df.get("İşlem Tarihi", pd.Series(dtype=str)), errors="coerce"
    ).fillna(pd.Timestamp(date.today())).dt.strftime("%Y-%m-%d")
    normalized["Müşteri Adı"] = df.get("Müşteri Adı", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    normalized["Vergisiz Bedel"] = pd.to_numeric(
        df.get("Vergisiz Bedel", pd.Series(dtype=float)), errors="coerce"
    ).fillna(0.0)
    normalized["KDV"] = pd.to_numeric(df.get("KDV", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    normalized["Toplam Fatura"] = pd.to_numeric(
        df.get("Toplam Fatura", pd.Series(dtype=float)), errors="coerce"
    ).fillna(0.0)
    normalized["Müşteri Adı"] = normalized["Müşteri Adı"].astype(str).str.strip()
    normalized = normalized[
        (normalized["Müşteri Adı"] != "")
        & (normalized["Müşteri Adı"].str.upper() != "TOPLAM")
    ].reset_index(drop=True)
    return normalized


def read_legacy_table(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(file_path)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(file_path)
    raise ValueError(f"Desteklenmeyen dosya türü: {file_path.name}")


def import_dataframe_into_db(
    df: pd.DataFrame,
    *,
    db_path: Path = DATABASE_PATH,
    archive_week_key: str | None = None,
    archive_label: str | None = None,
    source: str,
    gib_status: str,
    status_message: str,
) -> None:
    if df.empty:
        return

    normalized = normalize_legacy_dataframe(df)
    inserted_at = now_iso()
    rows = []
    for _, row in normalized.iterrows():
        rows.append(
            (
                row["İşlem Tarihi"],
                row["Müşteri Adı"],
                DEFAULT_TC,
                0.0,
                0.0,
                0.0,
                round(float(row["Vergisiz Bedel"]), 2),
                round(float(row["KDV"]), 2),
                round(float(row["Toplam Fatura"]), 2),
                gib_status,
                status_message,
                archive_week_key,
                archive_label,
                source,
                inserted_at,
                inserted_at,
            )
        )

    with closing(get_db_connection(db_path)) as conn, conn:
        conn.executemany(
            """
            INSERT INTO transactions (
                islem_tarihi, musteri_adi, musteri_tc, satilan_usdt, alis_kuru, satis_kuru,
                vergisiz_bedel, kdv, toplam_fatura, gib_durumu, durum_mesaji,
                arsiv_hafta_kodu, arsiv_etiketi, kaynak, olusturulma_zamani, guncellenme_zamani
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def parse_archive_label_from_filename(file_name: str) -> tuple[str, str]:
    stem = Path(file_name).stem
    parts = stem.split("_")
    if len(parts) >= 6:
        week_key = f"{parts[2]}_{parts[3]}"
        label = f"{parts[4]} - {parts[5]}"
    else:
        week_key = stem
        label = stem.replace("arsiv_hafta_", "").replace("_", " ")
    return week_key, label


def get_legacy_archive_files() -> list[Path]:
    patterns = ["arsiv_hafta_*.csv", "arsiv_hafta_*.xlsx", "*.csv", "*.xlsx"]
    found_files: list[Path] = []
    seen: set[Path] = set()
    for directory in [BASE_DIR, ARCHIVE_DIR, OLD_ARCHIVE_DIR]:
        if not directory.exists():
            continue
        for pattern in patterns:
            for file_path in directory.glob(pattern):
                if not file_path.is_file() or file_path == LEGACY_CSV_PATH:
                    continue
                if file_path.name.startswith("~$"):
                    continue
                if file_path in seen:
                    continue
                seen.add(file_path)
                found_files.append(file_path)
    return sorted(found_files)


def migrate_legacy_files(db_path: Path = DATABASE_PATH) -> None:
    ensure_storage()
    ensure_database(db_path)

    active_file_key = f"active::{LEGACY_CSV_PATH.name}"
    if LEGACY_CSV_PATH.exists():
        with closing(get_db_connection(db_path)) as conn:
            active_file_imported = is_file_imported(conn, active_file_key)
        if not active_file_imported:
            df_legacy = read_legacy_table(LEGACY_CSV_PATH)
            import_dataframe_into_db(
                df_legacy,
                db_path=db_path,
                source="legacy_active_csv",
                gib_status="Geçmiş Kayıt",
                status_message="Eski CSV dosyasından SQLite veritabanına aktarıldı.",
            )
            with closing(get_db_connection(db_path)) as conn, conn:
                mark_file_imported(conn, active_file_key)
            backup_path = BASE_DIR / f"{LEGACY_CSV_PATH.stem}_legacy_backup{LEGACY_CSV_PATH.suffix}"
            try:
                if not backup_path.exists():
                    LEGACY_CSV_PATH.replace(backup_path)
                else:
                    LEGACY_CSV_PATH.unlink(missing_ok=True)
            except OSError:
                pass

    for archive_file in get_legacy_archive_files():
        archive_key = f"archive::{archive_file.resolve()}"
        with closing(get_db_connection(db_path)) as conn:
            archive_imported = is_file_imported(conn, archive_key)
        if archive_imported:
            continue

        df_archive = read_legacy_table(archive_file)
        week_key, label = parse_archive_label_from_filename(archive_file.name)
        import_dataframe_into_db(
            df_archive,
            db_path=db_path,
            archive_week_key=week_key,
            archive_label=label,
            source="legacy_archive_csv",
            gib_status="Arşiv Kayıt",
            status_message=f"{archive_file.name} dosyasından arşive aktarıldı.",
        )
        with closing(get_db_connection(db_path)) as conn, conn:
            mark_file_imported(conn, archive_key)


def load_transactions(db_path: Path = DATABASE_PATH, archived: bool | None = None) -> pd.DataFrame:
    where_clause = ""
    if archived is True:
        where_clause = "WHERE arsiv_hafta_kodu IS NOT NULL"
    elif archived is False:
        where_clause = "WHERE arsiv_hafta_kodu IS NULL"

    query = f"""
        SELECT
            id,
            islem_tarihi AS "İşlem Tarihi",
            musteri_adi AS "Müşteri Adı",
            musteri_tc AS "T.C. Kimlik No",
            satilan_usdt AS "Satılan USDT",
            alis_kuru AS "Alış Kuru",
            satis_kuru AS "Satış Kuru",
            vergisiz_bedel AS "Vergisiz Bedel",
            kdv AS "KDV",
            toplam_fatura AS "Toplam Fatura",
            gib_durumu AS "GİB Durumu",
            durum_mesaji AS "Durum Mesajı",
            gib_ettn AS "GİB ETTN",
            gib_belge_numarasi AS "GİB Belge No",
            gib_son_senkron AS "GİB Son Senkron",
            arsiv_hafta_kodu AS "Arşiv Hafta Kodu",
            arsiv_etiketi AS "Arşiv Etiketi",
            olusturulma_zamani AS "Kayıt Zamanı"
        FROM transactions
        {where_clause}
        ORDER BY date(islem_tarihi) DESC, id DESC
    """
    with closing(get_db_connection(db_path)) as conn:
        df = pd.read_sql_query(query, conn)

    if df.empty:
        return pd.DataFrame(
            columns=[
                "id",
                "İşlem Tarihi",
                "Müşteri Adı",
                "T.C. Kimlik No",
                "Satılan USDT",
                "Alış Kuru",
                "Satış Kuru",
                "Vergisiz Bedel",
                "KDV",
                "Toplam Fatura",
                "GİB Durumu",
                "Durum Mesajı",
                "GİB ETTN",
                "GİB Belge No",
                "GİB Son Senkron",
                "Arşiv Hafta Kodu",
                "Arşiv Etiketi",
                "Kayıt Zamanı",
            ]
        )

    df["İşlem Tarihi"] = pd.to_datetime(df["İşlem Tarihi"], errors="coerce")
    for column in NUMERIC_KOLONLAR:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
    return df


def load_archive_groups(db_path: Path = DATABASE_PATH) -> pd.DataFrame:
    query = """
        SELECT
            arsiv_hafta_kodu AS "Arşiv Hafta Kodu",
            COALESCE(arsiv_etiketi, arsiv_hafta_kodu) AS "Arşiv Etiketi",
            COUNT(*) AS "Kayıt Adedi",
            ROUND(SUM(vergisiz_bedel), 2) AS "Vergisiz Bedel",
            ROUND(SUM(kdv), 2) AS "KDV",
            ROUND(SUM(toplam_fatura), 2) AS "Toplam Fatura",
            MAX(guncellenme_zamani) AS "Güncelleme Zamanı"
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
            ROUND(satilan_usdt, 2) AS satilan_usdt,
            COUNT(*) AS kullanim_adedi,
            MAX(guncellenme_zamani) AS son_kullanim
        FROM transactions
        WHERE satilan_usdt > 0
        GROUP BY ROUND(satilan_usdt, 2)
        ORDER BY kullanim_adedi DESC, son_kullanim DESC, satilan_usdt DESC
        LIMIT ?
    """
    with closing(get_db_connection(db_path)) as conn:
        rows = conn.execute(query, (limit,)).fetchall()
    return [round(float(row[0]), 2) for row in rows]


def calculate_invoice(satilan_usdt: float, alis_kuru: float, satis_kuru: float) -> dict:
    gercek_marj = satis_kuru - alis_kuru
    toplam_fatura = round(satilan_usdt * gercek_marj, 2)
    vergisiz_bedel = round(toplam_fatura / 1.20, 2)
    kdv_tutari = round(toplam_fatura - vergisiz_bedel, 2)
    return {
        "vergisiz_bedel": vergisiz_bedel,
        "kdv": kdv_tutari,
        "toplam_fatura": toplam_fatura,
    }


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
    invoice_name: str,
) -> list[str]:
    errors: list[str] = []
    if islem_tarihi > date.today():
        errors.append("Gider tarihi gelecekte olamaz.")
    if not aciklama.strip():
        errors.append("Açıklama boş bırakılamaz.")
    if kategori not in EXPENSE_CATEGORIES:
        errors.append("Geçersiz gider kategorisi seçildi.")
    if toplam_tutar <= 0:
        errors.append("Toplam tutar sıfırdan büyük olmalıdır.")
    if not 10 <= float(kdv_orani) <= 20:
        errors.append("KDV oranı %10 ile %20 arasında olmalıdır.")
    if not invoice_name.strip():
        errors.append("PDF, JPG veya PNG formatında bir fatura dosyası seçin.")
    return errors


def save_expense(record: dict, db_path: Path = DATABASE_PATH) -> int:
    inserted_at = now_iso()
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
                record["İşlem Tarihi"],
                record["Açıklama"],
                record["Kategori"],
                round(float(record["Toplam Tutar"]), 2),
                round(float(record["KDV Oranı"]), 2),
                round(float(record["KDV Tutarı"]), 2),
                round(float(record["Net Gider"]), 2),
                round(float(record["Gider Yazım Oranı"]), 4),
                round(float(record["Vergi Matrahı"]), 2),
                round(float(record["İndirilecek KDV"]), 2),
                round(float(record["Vergi Kalkanı"]), 2),
                1 if record.get("Ticari Araç") else 0,
                record.get("Fatura Dosya Yolu"),
                record.get("Fatura Orijinal Adı"),
                inserted_at,
                inserted_at,
            ),
        )
        return int(cursor.lastrowid)


def delete_expense(expense_id: int, db_path: Path = DATABASE_PATH) -> None:
    with closing(get_db_connection(db_path)) as conn, conn:
        conn.execute("DELETE FROM expenses WHERE id = ?", (expense_id,))


def attach_expense_invoice(
    expense_id: int,
    invoice_name: str,
    invoice_bytes: bytes,
    *,
    islem_tarihi: date,
    aciklama: str,
    db_path: Path = DATABASE_PATH,
    invoice_dir: Path = EXPENSE_INVOICE_DIR,
) -> Path:
    invoice_dir.mkdir(parents=True, exist_ok=True)
    extension = Path(invoice_name).suffix.lower()
    if extension not in {".pdf", ".jpg", ".jpeg", ".png"}:
        raise ValueError("Fatura dosyası PDF, JPG, JPEG veya PNG olmalıdır.")

    safe_name = f"{islem_tarihi.isoformat()}_{expense_id}_{slugify_text(aciklama)}{extension}"
    target_path = invoice_dir / safe_name
    target_path.write_bytes(invoice_bytes)

    with closing(get_db_connection(db_path)) as conn, conn:
        conn.execute(
            """
            UPDATE expenses
            SET fatura_dosya_yolu = ?, fatura_orijinal_adi = ?, guncellenme_zamani = ?
            WHERE id = ?
            """,
            (str(target_path.resolve()), invoice_name, now_iso(), expense_id),
        )
    return target_path


def load_expenses(
    db_path: Path = DATABASE_PATH,
    month_key: str | None = None,
) -> pd.DataFrame:
    params: list[object] = []
    where_clause = ""
    if month_key:
        where_clause = "WHERE substr(islem_tarihi, 1, 7) = ?"
        params.append(month_key)

    query = f"""
        SELECT
            id,
            islem_tarihi AS "Tarih",
            aciklama AS "Açıklama",
            kategori AS "Kategori",
            toplam_tutar AS "Toplam Tutar",
            kdv_orani AS "KDV Oranı",
            kdv_tutari AS "KDV Tutarı",
            net_gider AS "Net Gider",
            gider_yazim_orani AS "Gider Yazım Oranı",
            vergi_matrahi AS "Vergi Matrahı",
            indirilecek_kdv AS "İndirilecek KDV",
            vergi_kalkani AS "Vergi Kalkanı",
            ticari_arac AS "Ticari Araç",
            fatura_dosya_yolu AS "Fatura Dosya Yolu",
            fatura_orijinal_adi AS "Fatura Orijinal Adı",
            olusturulma_zamani AS "Kayıt Zamanı"
        FROM expenses
        {where_clause}
        ORDER BY date(islem_tarihi) DESC, id DESC
    """
    with closing(get_db_connection(db_path)) as conn:
        df = pd.read_sql_query(query, conn, params=params)

    if df.empty:
        return pd.DataFrame(
            columns=[
                "id", "Tarih", "Açıklama", "Kategori", "Toplam Tutar", "KDV Oranı",
                "KDV Tutarı", "Net Gider", "Gider Yazım Oranı", "Vergi Matrahı",
                "İndirilecek KDV", "Vergi Kalkanı", "Ticari Araç", "Fatura Dosya Yolu",
                "Fatura Orijinal Adı", "Kayıt Zamanı",
            ]
        )

    df["Tarih"] = pd.to_datetime(df["Tarih"], errors="coerce")
    numeric_columns = [
        "Toplam Tutar", "KDV Oranı", "KDV Tutarı", "Net Gider", "Gider Yazım Oranı",
        "Vergi Matrahı", "İndirilecek KDV", "Vergi Kalkanı",
    ]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
    df["Ticari Araç"] = df["Ticari Araç"].fillna(0).astype(int).astype(bool)
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
        "toplam_gider": round(float(df["Toplam Tutar"].sum()), 2),
        "toplam_kdv_iadesi": round(float(df["İndirilecek KDV"].sum()), 2),
        "toplam_vergi_matrahi": round(float(df["Vergi Matrahı"].sum()), 2),
        "toplam_vergi_kalkani": round(float(df["Vergi Kalkanı"].sum()), 2),
        "kayit_adedi": int(len(df)),
    }


def build_expense_report_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "Tarih", "Açıklama", "Kategori", "Toplam Tutar", "KDV Oranı", "KDV Tutarı",
                "Net Gider", "Vergi Matrahı", "İndirilecek KDV", "Vergi Kalkanı",
                "Ticari Araç", "Fatura Dosya Yolu",
            ]
        )

    export_df = df.copy()
    export_df["Tarih"] = pd.to_datetime(export_df["Tarih"], errors="coerce").dt.strftime("%Y-%m-%d")
    export_df["Gider Yazım Oranı"] = (export_df["Gider Yazım Oranı"] * 100).round(0).astype(int).astype(str) + "%"
    export_df["Ticari Araç"] = export_df["Ticari Araç"].map({True: "Evet", False: "Hayır"})
    export_df = export_df[
        [
            "Tarih", "Açıklama", "Kategori", "Toplam Tutar", "KDV Oranı", "KDV Tutarı",
            "Net Gider", "Gider Yazım Oranı", "Vergi Matrahı", "İndirilecek KDV",
            "Vergi Kalkanı", "Ticari Araç", "Fatura Dosya Yolu",
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
    export_df = build_expense_report_dataframe(df)
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

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary_df.to_excel(writer, index=False, sheet_name="Özet")
        export_df.to_excel(writer, index=False, sheet_name="Giderler")
    return output.getvalue()


def build_expense_report_csv_bytes(df: pd.DataFrame) -> bytes:
    export_df = build_expense_report_dataframe(df)
    return export_df.to_csv(index=False).encode("utf-8-sig")


def get_expense_month_options(df: pd.DataFrame) -> list[str]:
    if df.empty:
        return [date.today().strftime("%Y-%m")]
    months = (
        pd.to_datetime(df["Tarih"], errors="coerce")
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


def validate_transaction_input(
    musteri_adi: str,
    musteri_tc: str,
    islem_tarihi: date,
    satilan_usdt: float,
    alis_kuru: float,
    satis_kuru: float,
) -> tuple[list[str], str]:
    errors: list[str] = []
    tc_no = musteri_tc.strip()

    if not musteri_adi.strip():
        errors.append("Müşteri adı boş bırakılamaz.")
    if not tc_no.isdigit() or len(tc_no) != 11:
        errors.append("T.C. Kimlik No 11 haneli ve sadece rakamlardan oluşmalıdır.")
    if islem_tarihi > date.today():
        errors.append("İşlem tarihi gelecekte olamaz.")
    if satilan_usdt <= 0:
        errors.append("Satılan USDT sıfırdan büyük olmalıdır.")
    if alis_kuru <= 0 or satis_kuru <= 0:
        errors.append("Alış ve satış kuru sıfırdan büyük olmalıdır.")
    if satis_kuru < alis_kuru:
        errors.append("Satış kuru, alış kurundan küçük olamaz.")

    return errors, tc_no


def validate_manual_invoice_input(
    musteri_adi: str,
    musteri_tc: str,
    islem_tarihi: date,
    vergisiz_bedel: float,
    kdv: float,
    toplam_fatura: float,
) -> tuple[list[str], str]:
    errors: list[str] = []
    tc_no = musteri_tc.strip()
    beklenen_toplam = round(float(vergisiz_bedel) + float(kdv), 2)

    if not musteri_adi.strip():
        errors.append("Müşteri adı boş bırakılamaz.")
    if not tc_no.isdigit() or len(tc_no) != 11:
        errors.append("T.C. Kimlik No 11 haneli ve sadece rakamlardan oluşmalıdır.")
    if islem_tarihi > date.today():
        errors.append("İşlem tarihi gelecekte olamaz.")
    if vergisiz_bedel <= 0:
        errors.append("Vergisiz bedel sıfırdan büyük olmalıdır.")
    if kdv < 0 or toplam_fatura <= 0:
        errors.append("KDV negatif olamaz ve toplam fatura sıfırdan büyük olmalıdır.")
    if round(float(toplam_fatura), 2) != beklenen_toplam:
        errors.append(f"Toplam fatura, vergisiz bedel + KDV ile aynı olmalıdır. Beklenen: {beklenen_toplam:,.2f} TL")

    return errors, tc_no


def split_customer_name(full_name: str) -> tuple[str, str]:
    parts = full_name.strip().split()
    if len(parts) <= 1:
        return full_name.strip(), ""
    return " ".join(parts[:-1]), parts[-1]


def wrap_session_post_with_timeout(session: requests.Session, timeout_seconds: int) -> None:
    original_post = session.post

    def post_with_timeout(self: requests.Session, *args, **kwargs):
        kwargs.setdefault("timeout", timeout_seconds)
        return original_post(*args, **kwargs)

    session.post = MethodType(post_with_timeout, session)


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
    portal.oturum.headers.update({
        "User-Agent": "https://github.com/keyiflerolsun/eArsivPortal"
    })
    portal.token = None
    portal.giris_yap()
    return portal


def create_gib_draft_once(
    *,
    gib_kullanici: str,
    gib_sifre: str,
    musteri_adi: str,
    musteri_tc: str,
    islem_tarihi: date,
    toplam_fatura: float,
) -> tuple[str, str, str | None]:
    from eArsivPortal.Libs.FaturaVer import fatura_ver

    gib_ad, gib_soyad = split_customer_name(musteri_adi)
    portal = create_gib_portal_session(gib_kullanici, gib_sifre)
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
        komut=portal.komutlar.FATURA_OLUSTUR,
        jp=payload,
    )
    response_text = str(response.get("data", "")).strip()
    normalized_text = response_text.casefold()
    success_markers = (
        "başarıyla oluşturulmuştur",
        "basariyla olusturulmustur",
        "başarıyla oluşturulmu",
        "basariyla olusturulmu",
    )

    if any(marker in normalized_text for marker in success_markers):
        return (
            "Taslak Oluşturuldu",
            "GİB Portalında taslak fatura başarıyla oluşturuldu.",
            str(payload.get("faturaUuid") or "").strip() or None,
        )

    if response_text:
        return "GİB Hatası", response_text, None
    return "GİB Hatası", "GİB Portalı taslak oluşturma isteğini tamamlamadı.", None


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
        return create_gib_draft_once(
            gib_kullanici=gib_kullanici,
            gib_sifre=gib_sifre,
            musteri_adi=musteri_adi,
            musteri_tc=musteri_tc,
            islem_tarihi=islem_tarihi,
            toplam_fatura=toplam_fatura,
        )
    except requests.Timeout:
        return (
            "Zaman Aşımı",
            f"GİB Portalı {GIB_REQUEST_TIMEOUT_SECONDS} saniye içinde yanıt vermedi. Kayıt veritabanına alındı, taslak oluşturulmadı.",
            None,
        )
    except requests.RequestException as exc:
        return "Bağlantı Hatası", f"GİB Portalına bağlanırken ağ hatası oluştu: {exc}", None
    except Exception as exc:
        message = str(exc)
        if "unable to infer type" in message or "ConfigError" in message:
            if not (sys.version_info >= (3, 11) and sys.version_info < (3, 13)):
                return (
                    "Uyumluluk Hatası",
                    "eArsivPortal mevcut Python sürümüyle uyumlu görünmüyor. Python 3.11 veya 3.12 kullanın.",
                    None,
                )
        return "GİB Hatası", message, None


def save_transaction(record: dict, db_path: Path = DATABASE_PATH) -> int:
    inserted_at = now_iso()
    with closing(get_db_connection(db_path)) as conn, conn:
        cursor = conn.execute(
            """
            INSERT INTO transactions (
                islem_tarihi, musteri_adi, musteri_tc, satilan_usdt, alis_kuru, satis_kuru,
                vergisiz_bedel, kdv, toplam_fatura, gib_durumu, durum_mesaji,
                gib_ettn, gib_belge_numarasi, gib_son_senkron,
                arsiv_hafta_kodu, arsiv_etiketi, kaynak, olusturulma_zamani, guncellenme_zamani
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["İşlem Tarihi"],
                record["Müşteri Adı"],
                record["T.C. Kimlik No"],
                record["Satılan USDT"],
                record["Alış Kuru"],
                record["Satış Kuru"],
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
                record.get("Kaynak", "uygulama"),
                inserted_at,
                inserted_at,
            ),
        )
        return int(cursor.lastrowid)


def make_archive_key(label: str, record_date: date) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", label.strip().lower())
    normalized = normalized.strip("_") or "arsiv"
    return f"manuel_{record_date.strftime('%Y%m%d')}_{normalized}"


def normalize_invoice_identity(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    normalized["İşlem Tarihi"] = pd.to_datetime(normalized["İşlem Tarihi"], errors="coerce").dt.strftime("%Y-%m-%d")
    normalized["İşlem Tarihi"] = normalized["İşlem Tarihi"].fillna("")
    normalized["Müşteri Adı"] = normalized["Müşteri Adı"].fillna("").astype(str).str.strip().str.casefold()
    normalized["T.C. Kimlik No"] = normalized["T.C. Kimlik No"].fillna(DEFAULT_TC).astype(str).str.replace(".0", "", regex=False)
    for column in ["Vergisiz Bedel", "KDV", "Toplam Fatura"]:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce").fillna(0.0).round(2)
    return normalized


def build_archive_conflict_message(conflict_df: pd.DataFrame) -> str:
    previews = []
    for _, row in conflict_df.head(3).iterrows():
        archive_label = str(row.get("Arşiv Etiketi", "")).strip() or str(row.get("Arşiv Hafta Kodu", "")).strip()
        islem_tarihi = pd.to_datetime(row["İşlem Tarihi"], errors="coerce")
        tarih_text = islem_tarihi.strftime("%Y-%m-%d") if pd.notna(islem_tarihi) else str(row["İşlem Tarihi"])
        previews.append(
            f"{row['Müşteri Adı']} | {tarih_text} | {float(row['Toplam Fatura']):,.2f} TL -> {archive_label}"
        )

    suffix = ""
    if len(conflict_df) > 3:
        suffix = f" ve {len(conflict_df) - 3} kayıt daha"
    return "Bu faturalar başka bir arşivde zaten mevcut: " + "; ".join(previews) + suffix


def find_archive_conflicts(candidate_df: pd.DataFrame, db_path: Path = DATABASE_PATH) -> pd.DataFrame:
    if candidate_df.empty:
        return pd.DataFrame()

    archived_df = load_transactions(db_path, archived=True)
    if archived_df.empty:
        return pd.DataFrame()

    candidate_work = candidate_df.copy().reset_index(drop=True)
    candidate_work["__candidate_index"] = candidate_work.index
    archived_work = archived_df.copy()

    candidate_normalized = normalize_invoice_identity(candidate_work)
    archived_normalized = normalize_invoice_identity(archived_work)
    archived_normalized["Arşiv Etiketi"] = archived_work["Arşiv Etiketi"]
    archived_normalized["Arşiv Hafta Kodu"] = archived_work["Arşiv Hafta Kodu"]

    internal_duplicate_indices = candidate_normalized[
        candidate_normalized.duplicated(subset=ARCHIVE_MATCH_KOLONLARI, keep=False)
    ]["__candidate_index"].drop_duplicates().tolist()
    internal_conflicts = pd.DataFrame()
    if internal_duplicate_indices:
        internal_conflicts = candidate_work.loc[
            internal_duplicate_indices,
            ["__candidate_index", "İşlem Tarihi", "Müşteri Adı", "Toplam Fatura"],
        ].copy()
        internal_conflicts["Arşiv Etiketi"] = "Seçilen kayıtlar içinde tekrar"
        internal_conflicts["Arşiv Hafta Kodu"] = "-"

    merged = candidate_normalized.merge(
        archived_normalized[ARCHIVE_MATCH_KOLONLARI + ["Arşiv Etiketi", "Arşiv Hafta Kodu"]],
        on=ARCHIVE_MATCH_KOLONLARI,
        how="inner",
        suffixes=("", "_arsiv"),
    )
    if merged.empty and internal_conflicts.empty:
        return pd.DataFrame()

    external_conflicts = pd.DataFrame()
    if not merged.empty:
        conflict_indices = merged["__candidate_index"].drop_duplicates().tolist()
        conflict_rows = candidate_work.loc[
            conflict_indices,
            ["__candidate_index", "İşlem Tarihi", "Müşteri Adı", "Toplam Fatura"],
        ].copy()
        archive_details = (
            merged.groupby("__candidate_index")[["Arşiv Etiketi_arsiv", "Arşiv Hafta Kodu_arsiv"]]
            .first()
            .rename(columns={"Arşiv Etiketi_arsiv": "Arşiv Etiketi", "Arşiv Hafta Kodu_arsiv": "Arşiv Hafta Kodu"})
            .reset_index()
        )
        external_conflicts = conflict_rows.merge(archive_details, on="__candidate_index", how="left")

    combined_conflicts = pd.concat([internal_conflicts, external_conflicts], ignore_index=True)
    return combined_conflicts.drop_duplicates(subset=["İşlem Tarihi", "Müşteri Adı", "Toplam Fatura", "Arşiv Etiketi"])


def find_cross_archive_duplicate_invoices(db_path: Path = DATABASE_PATH) -> pd.DataFrame:
    archived_df = load_transactions(db_path, archived=True)
    if archived_df.empty:
        return pd.DataFrame(
            columns=[
                "id",
                "İşlem Tarihi",
                "Müşteri Adı",
                "T.C. Kimlik No",
                "Toplam Fatura",
                "Arşiv Hafta Kodu",
                "Arşiv Etiketi",
                "Çakışan Arşiv Sayısı",
                "Çakışan Kayıt Sayısı",
            ]
        )

    display_df = archived_df.copy().reset_index(drop=True)
    normalized_df = normalize_invoice_identity(display_df)
    normalized_df["id"] = display_df["id"].astype(int)
    normalized_df["Arşiv Hafta Kodu"] = display_df["Arşiv Hafta Kodu"]

    duplicate_groups = (
        normalized_df.groupby(ARCHIVE_MATCH_KOLONLARI)
        .agg(
            Çakışan_Arşiv_Sayısı=("Arşiv Hafta Kodu", "nunique"),
            Çakışan_Kayıt_Sayısı=("id", "count"),
        )
        .reset_index()
    )
    duplicate_groups = duplicate_groups[duplicate_groups["Çakışan_Arşiv_Sayısı"] > 1]
    if duplicate_groups.empty:
        return pd.DataFrame(
            columns=[
                "id",
                "İşlem Tarihi",
                "Müşteri Adı",
                "T.C. Kimlik No",
                "Toplam Fatura",
                "Arşiv Hafta Kodu",
                "Arşiv Etiketi",
                "Çakışan Arşiv Sayısı",
                "Çakışan Kayıt Sayısı",
            ]
        )

    duplicate_rows = normalized_df.merge(duplicate_groups, on=ARCHIVE_MATCH_KOLONLARI, how="inner")
    result = display_df.merge(
        duplicate_rows[["id", "Çakışan_Arşiv_Sayısı", "Çakışan_Kayıt_Sayısı"]],
        on="id",
        how="inner",
    )
    result = result.rename(
        columns={
            "Çakışan_Arşiv_Sayısı": "Çakışan Arşiv Sayısı",
            "Çakışan_Kayıt_Sayısı": "Çakışan Kayıt Sayısı",
        }
    )
    result["İşlem Tarihi"] = pd.to_datetime(result["İşlem Tarihi"], errors="coerce").dt.strftime("%Y-%m-%d")
    result["İşlem Tarihi"] = result["İşlem Tarihi"].fillna("")
    return result.sort_values(
        ["İşlem Tarihi", "Müşteri Adı", "Toplam Fatura", "Arşiv Etiketi", "id"],
        ascending=[False, True, False, True, True],
    ).reset_index(drop=True)


def find_full_duplicate_deletion_risks(selected_transaction_ids: list[int], db_path: Path = DATABASE_PATH) -> pd.DataFrame:
    if not selected_transaction_ids:
        return pd.DataFrame(columns=["İşlem Tarihi", "Müşteri Adı", "Toplam Fatura", "Mevcut Kopya", "Silinen Kopya"])

    archived_df = load_transactions(db_path, archived=True)
    if archived_df.empty:
        return pd.DataFrame(columns=["İşlem Tarihi", "Müşteri Adı", "Toplam Fatura", "Mevcut Kopya", "Silinen Kopya"])

    working_df = archived_df.copy().reset_index(drop=True)
    normalized_df = normalize_invoice_identity(working_df)
    normalized_df["id"] = working_df["id"].astype(int)
    normalized_df["İşlem Tarihi Orijinal"] = pd.to_datetime(working_df["İşlem Tarihi"], errors="coerce").dt.strftime("%Y-%m-%d")
    normalized_df["İşlem Tarihi Orijinal"] = normalized_df["İşlem Tarihi Orijinal"].fillna("")
    normalized_df["Müşteri Adı Orijinal"] = working_df["Müşteri Adı"].fillna("").astype(str).str.strip()
    normalized_df["Toplam Fatura Orijinal"] = pd.to_numeric(working_df["Toplam Fatura"], errors="coerce").fillna(0.0).round(2)
    normalized_df["Seçili"] = normalized_df["id"].isin([int(transaction_id) for transaction_id in selected_transaction_ids])

    grouped = (
        normalized_df.groupby(ARCHIVE_MATCH_KOLONLARI)
        .agg(
            Mevcut_Kopya=("id", "count"),
            Silinen_Kopya=("Seçili", "sum"),
            İşlem_Tarihi=("İşlem Tarihi Orijinal", "first"),
            Müşteri_Adı=("Müşteri Adı Orijinal", "first"),
            Toplam_Fatura=("Toplam Fatura Orijinal", "first"),
        )
        .reset_index(drop=True)
    )
    risky_groups = grouped[(grouped["Silinen_Kopya"] > 0) & (grouped["Mevcut_Kopya"] - grouped["Silinen_Kopya"] < 1)]
    if risky_groups.empty:
        return pd.DataFrame(columns=["İşlem Tarihi", "Müşteri Adı", "Toplam Fatura", "Mevcut Kopya", "Silinen Kopya"])

    return risky_groups.rename(
        columns={
            "İşlem_Tarihi": "İşlem Tarihi",
            "Müşteri_Adı": "Müşteri Adı",
            "Toplam_Fatura": "Toplam Fatura",
            "Mevcut_Kopya": "Mevcut Kopya",
            "Silinen_Kopya": "Silinen Kopya",
        }
    )[["İşlem Tarihi", "Müşteri Adı", "Toplam Fatura", "Mevcut Kopya", "Silinen Kopya"]].reset_index(drop=True)


def load_transactions_by_ids(transaction_ids: list[int], db_path: Path = DATABASE_PATH) -> pd.DataFrame:
    if not transaction_ids:
        return pd.DataFrame(columns=["id", *EXPORT_KOLONLARI, "Arşiv Hafta Kodu", "Arşiv Etiketi"])

    placeholders = ",".join("?" for _ in transaction_ids)
    query = f"""
        SELECT
            id,
            islem_tarihi AS "İşlem Tarihi",
            musteri_adi AS "Müşteri Adı",
            musteri_tc AS "T.C. Kimlik No",
            vergisiz_bedel AS "Vergisiz Bedel",
            kdv AS "KDV",
            toplam_fatura AS "Toplam Fatura",
            gib_ettn AS "GİB ETTN",
            gib_belge_numarasi AS "GİB Belge No",
            gib_son_senkron AS "GİB Son Senkron",
            arsiv_hafta_kodu AS "Arşiv Hafta Kodu",
            arsiv_etiketi AS "Arşiv Etiketi"
        FROM transactions
        WHERE id IN ({placeholders})
    """
    with closing(get_db_connection(db_path)) as conn:
        return pd.read_sql_query(query, conn, params=transaction_ids)


def update_gib_status(
    transaction_id: int,
    gib_durumu: str,
    durum_mesaji: str,
    db_path: Path = DATABASE_PATH,
    *,
    gib_ettn: str | object = UNSET,
    gib_belge_numarasi: str | object = UNSET,
    gib_son_senkron: str | object = UNSET,
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
            f"""
            UPDATE transactions
            SET {", ".join(set_clauses)}
            WHERE id = ?
            """,
            params,
        )


def update_gib_status_bulk(
    transaction_ids: list[int], gib_durumu: str, durum_mesaji: str, db_path: Path = DATABASE_PATH
) -> None:
    if not transaction_ids:
        return

    placeholders = ",".join("?" for _ in transaction_ids)
    with closing(get_db_connection(db_path)) as conn, conn:
        conn.execute(
            f"""
            UPDATE transactions
            SET gib_durumu = ?, durum_mesaji = ?, guncellenme_zamani = ?
            WHERE id IN ({placeholders})
            """,
            (gib_durumu, durum_mesaji, now_iso(), *transaction_ids),
        )


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

    baslangic_tarihi = working_df["İşlem Tarihi"].min().strftime("%d/%m/%Y")
    bitis_tarihi = working_df["İşlem Tarihi"].max().strftime("%d/%m/%Y")

    portal = create_gib_portal_session(gib_kullanici, gib_sifre)
    try:
        drafts = portal.faturalari_getir(
            baslangic_tarihi=baslangic_tarihi,
            bitis_tarihi=bitis_tarihi,
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

        update_gib_status(
            transaction_id,
            local_status,
            build_gib_sync_message(draft_row),
            db_path,
            gib_ettn=str(draft_row.get("ettn") or "").strip() or None,
            gib_belge_numarasi=str(draft_row.get("belge_numarasi") or "").strip() or None,
            gib_son_senkron=sync_timestamp,
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
        message = str(exc)
        if "unable to infer type" in message or "ConfigError" in message:
            if not (sys.version_info >= (3, 11) and sys.version_info < (3, 13)):
                message = "eArsivPortal mevcut Python sürümüyle uyumlu görünmüyor. Python 3.11 veya 3.12 kullanın."
                return {"ok": False, "status": "Uyumluluk Hatası", "message": message}
        return {"ok": False, "status": "GİB Hatası", "message": message}

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


def delete_transactions(transaction_ids: list[int], db_path: Path = DATABASE_PATH) -> int:
    if not transaction_ids:
        return 0
    placeholders = ",".join("?" for _ in transaction_ids)
    with closing(get_db_connection(db_path)) as conn, conn:
        cursor = conn.execute(
            f"DELETE FROM transactions WHERE arsiv_hafta_kodu IS NULL AND id IN ({placeholders})",
            tuple(transaction_ids),
        )
        return max(cursor.rowcount, 0)


def delete_archived_transactions_by_ids(transaction_ids: list[int], db_path: Path = DATABASE_PATH) -> int:
    if not transaction_ids:
        return 0
    placeholders = ",".join("?" for _ in transaction_ids)
    with closing(get_db_connection(db_path)) as conn, conn:
        cursor = conn.execute(
            f"DELETE FROM transactions WHERE arsiv_hafta_kodu IS NOT NULL AND id IN ({placeholders})",
            tuple(transaction_ids),
        )
        return max(cursor.rowcount, 0)


def move_transactions_to_archive(
    transaction_ids: list[int], archive_key: str, archive_label: str, db_path: Path = DATABASE_PATH
) -> int:
    if not transaction_ids:
        return 0

    placeholders = ",".join("?" for _ in transaction_ids)
    with closing(get_db_connection(db_path)) as conn, conn:
        cursor = conn.execute(
            f"""
            UPDATE transactions
            SET arsiv_hafta_kodu = ?, arsiv_etiketi = ?, guncellenme_zamani = ?
            WHERE id IN ({placeholders})
            """,
            (archive_key, archive_label, now_iso(), *transaction_ids),
        )
        return max(cursor.rowcount, 0)


def delete_archive_transactions(archive_key: str, db_path: Path = DATABASE_PATH) -> int:
    with closing(get_db_connection(db_path)) as conn, conn:
        cursor = conn.execute(
            "DELETE FROM transactions WHERE arsiv_hafta_kodu = ?",
            (archive_key,),
        )
        return max(cursor.rowcount, 0)


def restore_archive_to_active(archive_key: str, db_path: Path = DATABASE_PATH) -> int:
    with closing(get_db_connection(db_path)) as conn, conn:
        cursor = conn.execute(
            """
            UPDATE transactions
            SET arsiv_hafta_kodu = NULL, arsiv_etiketi = NULL, guncellenme_zamani = ?
            WHERE arsiv_hafta_kodu = ?
            """,
            (now_iso(), archive_key),
        )
        return max(cursor.rowcount, 0)


def move_archive_to_archive(
    source_archive_key: str,
    target_archive_key: str,
    target_archive_label: str,
    db_path: Path = DATABASE_PATH,
) -> int:
    if not source_archive_key or not target_archive_key:
        return 0

    with closing(get_db_connection(db_path)) as conn, conn:
        cursor = conn.execute(
            """
            UPDATE transactions
            SET arsiv_hafta_kodu = ?, arsiv_etiketi = ?, guncellenme_zamani = ?
            WHERE arsiv_hafta_kodu = ?
            """,
            (target_archive_key, target_archive_label, now_iso(), source_archive_key),
        )
        return max(cursor.rowcount, 0)


def get_active_selection() -> set[int]:
    selected_ids = st.session_state.get(ACTIVE_SELECTION_STATE_KEY, [])
    return {int(record_id) for record_id in selected_ids}


def bump_active_selection_version() -> None:
    st.session_state[ACTIVE_SELECTION_VERSION_KEY] = st.session_state.get(ACTIVE_SELECTION_VERSION_KEY, 0) + 1


def set_active_selection(record_ids: list[int], selected: bool, *, refresh_widgets: bool = False) -> None:
    selected_ids = get_active_selection()
    for record_id in record_ids:
        record_id = int(record_id)
        if selected:
            selected_ids.add(record_id)
        else:
            selected_ids.discard(record_id)
    st.session_state[ACTIVE_SELECTION_STATE_KEY] = sorted(selected_ids)
    if refresh_widgets:
        bump_active_selection_version()


def sync_active_checkbox(record_id: int, checkbox_key: str) -> None:
    set_active_selection([int(record_id)], bool(st.session_state.get(checkbox_key, False)))


def prune_active_selection(valid_record_ids: list[int]) -> None:
    valid_ids = {int(record_id) for record_id in valid_record_ids}
    selected_ids = get_active_selection() & valid_ids
    st.session_state[ACTIVE_SELECTION_STATE_KEY] = sorted(selected_ids)


def build_export_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    export_df = df.copy()
    if "İşlem Tarihi" in export_df.columns:
        export_df["İşlem Tarihi"] = pd.to_datetime(export_df["İşlem Tarihi"], errors="coerce").dt.strftime("%Y-%m-%d")
        export_df["İşlem Tarihi"] = export_df["İşlem Tarihi"].fillna("")

    export_columns = [column for column in EXPORT_KOLONLARI if column in export_df.columns]
    export_df = export_df[export_columns].copy() if export_columns else pd.DataFrame(columns=EXPORT_KOLONLARI)

    total_row = {column: "" for column in export_df.columns}
    if "Müşteri Adı" in total_row:
        total_row["Müşteri Adı"] = "TOPLAM"
    for column in ["Vergisiz Bedel", "KDV", "Toplam Fatura"]:
        if column in export_df.columns:
            total_row[column] = round(pd.to_numeric(export_df[column], errors="coerce").fillna(0.0).sum(), 2)

    return pd.concat([export_df, pd.DataFrame([total_row])], ignore_index=True)


def build_display_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    visible_columns = [column for column in GORUNEN_KOLONLAR if column in df.columns]
    display_df = df[visible_columns].copy() if visible_columns else pd.DataFrame(columns=GORUNEN_KOLONLAR)
    if display_df.empty:
        return display_df

    if "İşlem Tarihi" in display_df.columns:
        display_df["İşlem Tarihi"] = pd.to_datetime(display_df["İşlem Tarihi"], errors="coerce").dt.strftime("%Y-%m-%d")
        display_df["İşlem Tarihi"] = display_df["İşlem Tarihi"].fillna("")

    total_row = {column: "" for column in display_df.columns}
    total_row["Müşteri Adı"] = "TOPLAM"
    for column in ["Vergisiz Bedel", "KDV", "Toplam Fatura"]:
        if column in display_df.columns:
            total_row[column] = round(pd.to_numeric(display_df[column], errors="coerce").fillna(0.0).sum(), 2)
    return pd.concat([display_df, pd.DataFrame([total_row])], ignore_index=True)


def export_dataframe_as_xlsx(df: pd.DataFrame, file_name: str, sheet_name: str) -> tuple[bytes, str]:
    xlsx_bytes = df_to_xlsx_bytes(build_export_dataframe(df), sheet_name=sheet_name)
    return xlsx_bytes, file_name


def export_dataframe_as_csv(df: pd.DataFrame, file_name: str) -> tuple[bytes, str]:
    csv_bytes = build_export_dataframe(df).to_csv(index=False).encode("utf-8-sig")
    return csv_bytes, file_name


def filter_transactions_for_statistics(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "GİB Durumu" not in df.columns:
        return df.copy()
    return df[~df["GİB Durumu"].isin(EXCLUDED_STATS_GIB_STATUSES)].copy()


def archive_active_transactions(
    db_path: Path = DATABASE_PATH,
    archive_dir: Path = ARCHIVE_DIR,
    *,
    target_archive_key: str | None = None,
    target_archive_label: str | None = None,
) -> tuple[bool, str]:
    active_df = load_transactions(db_path, archived=False)
    if active_df.empty:
        return False, "Arşive taşınacak aktif işlem bulunamadı."

    archive_conflicts = find_archive_conflicts(active_df, db_path)
    if not archive_conflicts.empty:
        return False, build_archive_conflict_message(archive_conflicts)

    archive_dir.mkdir(parents=True, exist_ok=True)
    hafta = get_hafta_bilgisi()
    archive_key = target_archive_key or hafta["hafta_kodu"]
    archive_label = target_archive_label or hafta["etiket"]
    file_path = archive_dir / hafta["dosya_adi"]
    if file_path.exists():
        file_path = archive_dir / f"{file_path.stem}_{datetime.now().strftime('%H%M%S')}{file_path.suffix}"

    backup_bytes, _ = export_dataframe_as_xlsx(active_df, file_path.name, sheet_name="Arsiv")
    file_path.write_bytes(backup_bytes)

    active_ids = active_df["id"].astype(int).tolist()
    placeholders = ",".join("?" for _ in active_ids)

    try:
        with closing(get_db_connection(db_path)) as conn:
            conn.execute("BEGIN")
            conn.execute(
                f"""
                UPDATE transactions
                SET arsiv_hafta_kodu = ?, arsiv_etiketi = ?, guncellenme_zamani = ?
                WHERE arsiv_hafta_kodu IS NULL AND id IN ({placeholders})
                """,
                (archive_key, archive_label, now_iso(), *active_ids),
            )
            conn.commit()
    except Exception:
        file_path.unlink(missing_ok=True)
        raise

    return True, f"Aktif işlemler güvenli biçimde `{archive_label}` arşivine taşındı. Yedek dosya: {file_path.name}"


def get_dashboard_metrics(df: pd.DataFrame) -> dict:
    df = filter_transactions_for_statistics(df)
    if df.empty:
        return {
            "bugun": 0.0,
            "hafta": 0.0,
            "ay": 0.0,
            "toplam_kdv": 0.0,
            "en_aktif_musteri": "-",
        }

    working_df = df.copy()
    working_df["İşlem Tarihi"] = pd.to_datetime(working_df["İşlem Tarihi"], errors="coerce")
    today = pd.Timestamp(date.today())
    week_start = today - pd.Timedelta(days=today.weekday())
    month_start = pd.Timestamp(date.today().replace(day=1))
    customer_counts = working_df["Müşteri Adı"].value_counts()

    return {
        "bugun": round(working_df.loc[working_df["İşlem Tarihi"] == today, "Toplam Fatura"].sum(), 2),
        "hafta": round(working_df.loc[working_df["İşlem Tarihi"] >= week_start, "Toplam Fatura"].sum(), 2),
        "ay": round(working_df.loc[working_df["İşlem Tarihi"] >= month_start, "Toplam Fatura"].sum(), 2),
        "toplam_kdv": round(working_df["KDV"].sum(), 2),
        "en_aktif_musteri": customer_counts.index[0] if not customer_counts.empty else "-",
    }


def get_finance_summary(df: pd.DataFrame) -> dict:
    filtered_df = filter_transactions_for_statistics(df)
    if filtered_df.empty:
        empty_columns = ["Tarih", "Toplam Fatura", "KDV"]
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
            "daily_summary": pd.DataFrame(columns=empty_columns),
            "monthly_summary": pd.DataFrame(
                columns=["Dönem", "Kayıt Adedi", "Vergisiz Bedel", "KDV", "Toplam Fatura"]
            ),
            "status_summary": pd.DataFrame(columns=["GİB Durumu", "Kayıt Adedi", "Toplam Fatura"]),
            "top_customers": pd.DataFrame(columns=["Müşteri Adı", "Kayıt Adedi", "Toplam Fatura", "Toplam KDV"]),
        }

    working_df = filtered_df.copy()
    working_df["İşlem Tarihi"] = pd.to_datetime(working_df["İşlem Tarihi"], errors="coerce")
    working_df = working_df.dropna(subset=["İşlem Tarihi"]).reset_index(drop=True)
    if working_df.empty:
        return get_finance_summary(pd.DataFrame(columns=df.columns))

    today_ts = pd.Timestamp(date.today())
    month_start = pd.Timestamp(date.today().replace(day=1))
    previous_month_end = month_start - pd.Timedelta(days=1)
    previous_month_start = pd.Timestamp(previous_month_end.date().replace(day=1))
    active_mask = working_df["Arşiv Hafta Kodu"].isna() | working_df["Arşiv Hafta Kodu"].astype(str).str.strip().eq("")
    active_df = working_df[active_mask]
    archive_df = working_df[~active_mask]
    current_month_df = working_df[working_df["İşlem Tarihi"] >= month_start]
    previous_month_df = working_df[
        (working_df["İşlem Tarihi"] >= previous_month_start)
        & (working_df["İşlem Tarihi"] <= previous_month_end)
    ]

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

    monthly_summary = (
        working_df.assign(Dönem=working_df["İşlem Tarihi"].dt.strftime("%Y-%m"))
        .groupby("Dönem")
        .agg(
            Kayıt_Adedi=("id", "count"),
            Vergisiz_Bedel=("Vergisiz Bedel", "sum"),
            KDV=("KDV", "sum"),
            Toplam_Fatura=("Toplam Fatura", "sum"),
        )
        .reset_index()
        .rename(
            columns={
                "Kayıt_Adedi": "Kayıt Adedi",
                "Vergisiz_Bedel": "Vergisiz Bedel",
                "Toplam_Fatura": "Toplam Fatura",
            }
        )
        .sort_values("Dönem", ascending=False)
        .head(6)
        .reset_index(drop=True)
    )

    status_summary = (
        working_df.groupby("GİB Durumu")
        .agg(Kayıt_Adedi=("id", "count"), Toplam_Fatura=("Toplam Fatura", "sum"))
        .reset_index()
        .rename(columns={"Kayıt_Adedi": "Kayıt Adedi", "Toplam_Fatura": "Toplam Fatura"})
        .sort_values(["Kayıt Adedi", "Toplam Fatura"], ascending=[False, False])
        .reset_index(drop=True)
    )

    top_customers = (
        working_df.groupby("Müşteri Adı")
        .agg(
            Kayıt_Adedi=("id", "count"),
            Toplam_Fatura=("Toplam Fatura", "sum"),
            Toplam_KDV=("KDV", "sum"),
        )
        .reset_index()
        .rename(
            columns={
                "Kayıt_Adedi": "Kayıt Adedi",
                "Toplam_Fatura": "Toplam Fatura",
                "Toplam_KDV": "Toplam KDV",
            }
        )
        .sort_values(["Toplam Fatura", "Kayıt Adedi"], ascending=[False, False])
        .head(8)
        .reset_index(drop=True)
    )

    toplam_kayit = int(len(working_df))
    toplam_fatura = round(float(working_df["Toplam Fatura"].sum()), 2)
    imzali_kayit = int((working_df["GİB Durumu"] == "İmzalandı").sum())
    taslak_kayit = int((working_df["GİB Durumu"] == "Taslak Oluşturuldu").sum())

    return {
        "toplam_kayit": toplam_kayit,
        "toplam_fatura": toplam_fatura,
        "vergisiz_bedel": round(float(working_df["Vergisiz Bedel"].sum()), 2),
        "toplam_kdv": round(float(working_df["KDV"].sum()), 2),
        "ortalama_fatura": round(toplam_fatura / toplam_kayit, 2) if toplam_kayit else 0.0,
        "aktif_toplam_fatura": round(float(active_df["Toplam Fatura"].sum()), 2),
        "arsiv_toplam_fatura": round(float(archive_df["Toplam Fatura"].sum()), 2),
        "bu_ay_toplam": bu_ay_toplam,
        "onceki_ay_toplam": onceki_ay_toplam,
        "ay_degisim_orani": ay_degisim_orani,
        "imzali_kayit": imzali_kayit,
        "taslak_kayit": taslak_kayit,
        "imza_orani": round((imzali_kayit / toplam_kayit) * 100, 2) if toplam_kayit else 0.0,
        "daily_summary": daily_summary,
        "monthly_summary": monthly_summary,
        "status_summary": status_summary,
        "top_customers": top_customers,
    }


def filter_transactions(
    df: pd.DataFrame,
    *,
    search_text: str,
    start_date: date,
    end_date: date,
    min_total: float,
    status_list: list[str],
) -> pd.DataFrame:
    filtered_df = df.copy()
    if filtered_df.empty:
        return filtered_df

    if search_text:
        filtered_df = filtered_df[
            filtered_df["Müşteri Adı"].astype(str).str.contains(search_text, case=False, na=False)
        ]
    filtered_df = filtered_df[
        (filtered_df["İşlem Tarihi"] >= pd.Timestamp(start_date))
        & (filtered_df["İşlem Tarihi"] <= pd.Timestamp(end_date))
    ]
    filtered_df = filtered_df[filtered_df["Toplam Fatura"] >= min_total]
    if status_list:
        filtered_df = filtered_df[filtered_df["GİB Durumu"].isin(status_list)]
    return filtered_df.reset_index(drop=True)


def render_global_dashboard(df: pd.DataFrame) -> None:
    metrics = get_dashboard_metrics(df)
    st.subheader("📈 Yönetim Özeti")
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Bugün", f"{metrics['bugun']:,.2f} TL")
    col2.metric("Bu Hafta", f"{metrics['hafta']:,.2f} TL")
    col3.metric("Bu Ay", f"{metrics['ay']:,.2f} TL")
    col4.metric("Toplam KDV", f"{metrics['toplam_kdv']:,.2f} TL")
    col5.metric("En Aktif Müşteri", metrics["en_aktif_musteri"])
    st.caption(f"Veriler SQLite veritabanında saklanır: {DATABASE_PATH.name}")
    st.markdown("---")


def render_statistics_tab(df: pd.DataFrame) -> None:
    st.subheader("📊 Tarih Aralığına Göre İstatistikler")

    df = filter_transactions_for_statistics(df)

    if df.empty:
        st.info("İstatistik gösterecek uygun kayıt bulunmuyor.")
        return

    working_df = df.copy()
    working_df["İşlem Tarihi"] = pd.to_datetime(working_df["İşlem Tarihi"], errors="coerce")
    working_df = working_df.dropna(subset=["İşlem Tarihi"]).reset_index(drop=True)

    if working_df.empty:
        st.info("Tarih bilgisi olan kayıt bulunmuyor.")
        return

    min_date = working_df["İşlem Tarihi"].min().date()
    max_date = working_df["İşlem Tarihi"].max().date()
    max_selectable_date = max(max_date, date.today())
    default_start = max(min_date, date.today().replace(day=1))
    default_end = date.today()

    filter_col1, filter_col2 = st.columns(2)
    start_date = filter_col1.date_input(
        "Başlangıç Tarihi",
        value=default_start,
        min_value=min_date,
        max_value=max_selectable_date,
    )
    end_date = filter_col2.date_input(
        "Bitiş Tarihi",
        value=default_end,
        min_value=min_date,
        max_value=max_selectable_date,
    )

    if end_date < start_date:
        st.error("Bitiş tarihi başlangıç tarihinden küçük olamaz.")
        return

    filtered_df = working_df[
        (working_df["İşlem Tarihi"] >= pd.Timestamp(start_date))
        & (working_df["İşlem Tarihi"] <= pd.Timestamp(end_date))
    ].copy()

    st.caption(f"Seçilen aralık: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}")

    if filtered_df.empty:
        st.warning("Bu tarih aralığında kayıt bulunmuyor.")
        return

    filtered_df["Arşiv Durumu"] = filtered_df["Arşiv Hafta Kodu"].apply(
        lambda value: "Arşivde" if pd.notna(value) and str(value).strip() else "Aktif"
    )

    customer_totals = filtered_df.groupby("Müşteri Adı")["Toplam Fatura"].sum().sort_values(ascending=False)
    top_customer = customer_totals.index[0] if not customer_totals.empty else "-"
    archived_count = int((filtered_df["Arşiv Durumu"] == "Arşivde").sum())
    active_count = int((filtered_df["Arşiv Durumu"] == "Aktif").sum())
    avg_invoice = round(float(filtered_df["Toplam Fatura"].mean()), 2)

    stat1, stat2, stat3, stat4, stat5, stat6 = st.columns(6)
    stat1.metric("Toplam Kayıt", f"{len(filtered_df)}")
    stat2.metric("Toplam Fatura", f"{filtered_df['Toplam Fatura'].sum():,.2f} TL")
    stat3.metric("Vergisiz Bedel", f"{filtered_df['Vergisiz Bedel'].sum():,.2f} TL")
    stat4.metric("Toplam KDV", f"{filtered_df['KDV'].sum():,.2f} TL")
    stat5.metric("Ortalama Fatura", f"{avg_invoice:,.2f} TL")
    stat6.metric("En Aktif Müşteri", top_customer)

    stat7, stat8, stat9 = st.columns(3)
    stat7.metric("Aktif Kayıt", f"{active_count}")
    stat8.metric("Arşiv Kayıt", f"{archived_count}")
    stat9.metric("Tekil Müşteri", f"{filtered_df['Müşteri Adı'].nunique()}")

    st.markdown("---")

    daily_summary = (
        filtered_df.assign(Tarih=filtered_df["İşlem Tarihi"].dt.strftime("%Y-%m-%d"))
        .groupby("Tarih")[["Toplam Fatura", "Vergisiz Bedel", "KDV"]]
        .sum()
        .sort_index()
    )
    st.write("**Günlük Tutar Trendi**")
    st.line_chart(daily_summary, height=320)

    detail_col1, detail_col2 = st.columns(2)
    with detail_col1:
        st.write("**GİB Durum Dağılımı**")
        gib_summary = filtered_df["GİB Durumu"].fillna("Bilinmiyor").value_counts().rename_axis("GİB Durumu").reset_index(name="Kayıt Adedi")
        st.dataframe(gib_summary, width="stretch", hide_index=True)

    with detail_col2:
        st.write("**En Çok Ciro Üreten Müşteriler**")
        customer_summary = (
            filtered_df.groupby("Müşteri Adı")
            .agg(
                Kayıt_Adedi=("id", "count"),
                Toplam_Fatura=("Toplam Fatura", "sum"),
                Toplam_KDV=("KDV", "sum"),
            )
            .sort_values(["Toplam_Fatura", "Kayıt_Adedi"], ascending=[False, False])
            .head(10)
            .reset_index()
            .rename(
                columns={
                    "Müşteri Adı": "Müşteri Adı",
                    "Kayıt_Adedi": "Kayıt Adedi",
                    "Toplam_Fatura": "Toplam Fatura",
                    "Toplam_KDV": "Toplam KDV",
                }
            )
        )
        st.dataframe(customer_summary, width="stretch", hide_index=True)

    st.write("**Aktif / Arşiv Dağılımı**")
    archive_status_summary = (
        filtered_df.groupby("Arşiv Durumu")
        .agg(Kayıt_Adedi=("id", "count"), Toplam_Fatura=("Toplam Fatura", "sum"))
        .reset_index()
        .rename(columns={"Arşiv Durumu": "Durum", "Kayıt_Adedi": "Kayıt Adedi", "Toplam_Fatura": "Toplam Fatura"})
    )
    st.dataframe(archive_status_summary, width="stretch", hide_index=True)


def render_finance_summary_tab(df: pd.DataFrame) -> None:
    st.subheader("💰 Finans Özeti")
    st.caption("Uyumluluk veya GİB hata durumundaki kayıtlar bu özete dahil edilmez.")

    summary = get_finance_summary(df)
    if not summary["toplam_kayit"]:
        st.info("Finans özeti oluşturmak için uygun kayıt bulunmuyor.")
        return

    top_row = st.columns(4)
    top_row[0].metric("Toplam Fatura", f"{summary['toplam_fatura']:,.2f} TL")
    top_row[1].metric("Vergisiz Bedel", f"{summary['vergisiz_bedel']:,.2f} TL")
    top_row[2].metric("Toplam KDV", f"{summary['toplam_kdv']:,.2f} TL")
    top_row[3].metric("Ortalama Fatura", f"{summary['ortalama_fatura']:,.2f} TL")

    bottom_row = st.columns(4)
    bottom_row[0].metric("Aktif Portföy", f"{summary['aktif_toplam_fatura']:,.2f} TL")
    bottom_row[1].metric("Arşiv Portföy", f"{summary['arsiv_toplam_fatura']:,.2f} TL")
    bottom_row[2].metric("İmzalanan Kayıt", f"{summary['imzali_kayit']}", delta=f"%{summary['imza_orani']:,.2f}")
    bottom_row[3].metric("Bu Ay", f"{summary['bu_ay_toplam']:,.2f} TL", delta=f"%{summary['ay_degisim_orani']:,.2f}")

    daily_summary = summary["daily_summary"]
    if not daily_summary.empty:
        st.write("**Son 30 Günlük Fatura ve KDV Akışı**")
        st.line_chart(
            daily_summary.set_index("Tarih")[["Toplam Fatura", "KDV"]],
            height=320,
        )

    detail_col1, detail_col2 = st.columns(2)
    with detail_col1:
        st.write("**Aylık Finans Özeti**")
        st.dataframe(summary["monthly_summary"], width="stretch", hide_index=True)

    with detail_col2:
        st.write("**GİB Durum Bazlı Tutarlar**")
        st.dataframe(summary["status_summary"], width="stretch", hide_index=True)

    st.write("**En Çok Ciro Üreten Müşteriler**")
    st.dataframe(summary["top_customers"], width="stretch", hide_index=True)


def render_expense_panel_tab() -> None:
    st.subheader("🧾 Gider Paneli")
    st.caption(
        "Tüm giderlerde vergi matrahı %100 hesaplanır. "
        "İndirilecek KDV her durumda %100 alınır. "
        "Vergi kalkanı tahmini %20 kurumlar vergisi etkisiyle hesaplanır."
    )
    st.info(f"Fatura dosyaları yerel olarak şu klasöre kaydedilir: {EXPENSE_INVOICE_DIR}")

    if st.session_state.get("expense_category") not in EXPENSE_CATEGORIES:
        st.session_state["expense_category"] = EXPENSE_CATEGORIES[0]
    if "expense_date" not in st.session_state:
        st.session_state["expense_date"] = date.today()
    if "expense_total" not in st.session_state:
        st.session_state["expense_total"] = 0.0
    if "expense_vat_rate" not in st.session_state:
        st.session_state["expense_vat_rate"] = 20
    if "expense_description" not in st.session_state:
        st.session_state["expense_description"] = ""
    if "expense_commercial_vehicle" not in st.session_state:
        st.session_state["expense_commercial_vehicle"] = False

    if st.session_state.get("expense_category") != "Araç":
        st.session_state["expense_commercial_vehicle"] = False

    all_expenses_df = load_expenses()
    month_options = get_expense_month_options(all_expenses_df)
    current_month_key = date.today().strftime("%Y-%m")
    selected_month = st.selectbox(
        "Rapor Ayı",
        options=month_options,
        index=month_options.index(current_month_key) if current_month_key in month_options else 0,
        key="expense_month_filter",
    )
    monthly_expenses_df = load_expenses(month_key=selected_month)
    monthly_summary = summarize_expenses(monthly_expenses_df)

    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
    metric_col1.metric("Toplam Gider", f"{monthly_summary['toplam_gider']:,.2f} TL")
    metric_col2.metric("Toplam KDV İadesi", f"{monthly_summary['toplam_kdv_iadesi']:,.2f} TL")
    metric_col3.metric("Vergi Matrahı", f"{monthly_summary['toplam_vergi_matrahi']:,.2f} TL")
    metric_col4.metric("Vergi Kalkanı", f"{monthly_summary['toplam_vergi_kalkani']:,.2f} TL")

    report_xlsx = build_expense_report_xlsx_bytes(monthly_expenses_df, selected_month)
    report_csv = build_expense_report_csv_bytes(monthly_expenses_df)
    report_col1, report_col2, report_col3 = st.columns([1.4, 1, 1])
    report_col1.download_button(
        "Ay Sonu Raporu Al",
        data=report_xlsx,
        file_name=f"gider_raporu_{selected_month}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
    report_col2.download_button(
        "CSV İndir",
        data=report_csv,
        file_name=f"gider_raporu_{selected_month}.csv",
        mime="text/csv",
        use_container_width=True,
    )
    report_col3.metric("Kayıt", str(monthly_summary["kayit_adedi"]))

    st.markdown("---")
    st.write("**Yeni Gider Girişi**")

    form_col1, form_col2 = st.columns(2)
    with form_col1:
        expense_date = st.date_input("Tarih", key="expense_date")
        expense_description = st.text_input("Açıklama", key="expense_description", placeholder="Örn: araç kiralama")
        expense_category = st.selectbox("Kategori", EXPENSE_CATEGORIES, key="expense_category")
    with form_col2:
        expense_total = st.number_input(
            "Toplam Tutar",
            min_value=0.0,
            step=100.0,
            format="%.2f",
            key="expense_total",
        )
        expense_vat_rate = st.number_input(
            "KDV Oranı (%)",
            min_value=10,
            max_value=20,
            step=1,
            key="expense_vat_rate",
        )
        expense_commercial_vehicle = st.checkbox(
            "Ticari Araç",
            key="expense_commercial_vehicle",
            disabled=expense_category != "Araç",
            help="İsteğe bağlı alan; mevcut kurala göre gider yazım oranı %100 uygulanır.",
        )

    uploaded_invoice = st.file_uploader(
        "Fatura Seç (PDF/JPG/PNG)",
        type=["pdf", "jpg", "jpeg", "png"],
        key="expense_invoice_upload",
        help="Dosya seçildiğinde gider kaydıyla birlikte giderler/faturalar klasörüne kopyalanır.",
    )

    preview = calculate_expense_breakdown(
        expense_total,
        float(expense_vat_rate),
        expense_category,
        bool(expense_commercial_vehicle),
    )
    preview_col1, preview_col2, preview_col3, preview_col4, preview_col5 = st.columns(5)
    preview_col1.metric("KDV Tutarı", f"{preview['kdv_tutari']:,.2f} TL")
    preview_col2.metric("Net Gider", f"{preview['net_gider']:,.2f} TL")
    preview_col3.metric("Vergi Matrahı", f"{preview['vergi_matrahi']:,.2f} TL")
    preview_col4.metric("İndirilecek KDV", f"{preview['indirilecek_kdv']:,.2f} TL")
    preview_col5.metric("Vergi Kalkanı", f"{preview['vergi_kalkani']:,.2f} TL")
    st.caption(f"Gider yazım oranı: %{preview['gider_yazim_orani'] * 100:.0f}")

    if st.button("Gideri Kaydet", type="primary", use_container_width=True):
        invoice_name = uploaded_invoice.name if uploaded_invoice is not None else ""
        errors = validate_expense_input(
            expense_date,
            expense_description,
            expense_category,
            float(expense_total),
            float(expense_vat_rate),
            invoice_name,
        )
        if errors:
            for error in errors:
                st.error(error)
        else:
            expense_record = {
                "İşlem Tarihi": expense_date.isoformat(),
                "Açıklama": expense_description.strip(),
                "Kategori": expense_category,
                "Toplam Tutar": round(float(expense_total), 2),
                "KDV Oranı": round(float(expense_vat_rate), 2),
                "KDV Tutarı": preview["kdv_tutari"],
                "Net Gider": preview["net_gider"],
                "Gider Yazım Oranı": preview["gider_yazim_orani"],
                "Vergi Matrahı": preview["vergi_matrahi"],
                "İndirilecek KDV": preview["indirilecek_kdv"],
                "Vergi Kalkanı": preview["vergi_kalkani"],
                "Ticari Araç": bool(expense_commercial_vehicle),
                "Fatura Dosya Yolu": None,
                "Fatura Orijinal Adı": invoice_name,
            }
            expense_id = save_expense(expense_record)
            try:
                attach_expense_invoice(
                    expense_id,
                    invoice_name,
                    uploaded_invoice.getvalue() if uploaded_invoice is not None else b"",
                    islem_tarihi=expense_date,
                    aciklama=expense_description,
                )
            except Exception as exc:
                delete_expense(expense_id)
                st.error(f"Fatura dosyası kaydedilemedi: {exc}")
            else:
                st.success(f"Gider kaydedildi. Dosya ilişkilendirildi: {invoice_name}")
                st.session_state["expense_date"] = date.today()
                st.session_state["expense_description"] = ""
                st.session_state["expense_category"] = EXPENSE_CATEGORIES[0]
                st.session_state["expense_total"] = 0.0
                st.session_state["expense_vat_rate"] = 20
                st.session_state["expense_commercial_vehicle"] = False
                if "expense_invoice_upload" in st.session_state:
                    del st.session_state["expense_invoice_upload"]
                st.rerun()

    st.markdown("---")
    st.write(f"**{selected_month} Dönemi Giderleri**")
    if monthly_expenses_df.empty:
        st.info("Seçilen ayda kayıtlı gider bulunmuyor.")
        return

    display_df = monthly_expenses_df.copy()
    display_df["Tarih"] = pd.to_datetime(display_df["Tarih"], errors="coerce").dt.strftime("%Y-%m-%d")
    display_df["Gider Yazım Oranı"] = (display_df["Gider Yazım Oranı"] * 100).round(0).astype(int).astype(str) + "%"
    display_df["Ticari Araç"] = display_df["Ticari Araç"].map({True: "Evet", False: "Hayır"})
    st.dataframe(
        display_df[
            [
                "Tarih", "Açıklama", "Kategori", "Toplam Tutar", "KDV Oranı", "KDV Tutarı",
                "Vergi Matrahı", "İndirilecek KDV", "Vergi Kalkanı", "Gider Yazım Oranı", "Ticari Araç",
            ]
        ],
        width="stretch",
    )

    st.write("**Fatura Aksiyonları**")
    for _, row in monthly_expenses_df.iterrows():
        expense_path = str(row.get("Fatura Dosya Yolu") or "").strip()
        expense_file = Path(expense_path) if expense_path else None
        row_col1, row_col2, row_col3, row_col4, row_col5 = st.columns([1.1, 2.4, 1.2, 1.1, 1.1])
        expense_date_label = pd.to_datetime(row["Tarih"], errors="coerce")
        row_col1.markdown(f"**{expense_date_label.strftime('%Y-%m-%d') if pd.notna(expense_date_label) else row['Tarih']}**")
        row_col2.write(f"{row['Açıklama']} | {row['Kategori']}")
        row_col3.write(f"Toplam: {float(row['Toplam Tutar']):,.2f} TL")
        row_col4.write(f"KDV: {float(row['İndirilecek KDV']):,.2f} TL")
        if expense_file is not None and expense_file.exists():
            if row_col5.button("Faturayı Aç", key=f"open_expense_{int(row['id'])}"):
                try:
                    open_file_with_default_app(expense_file)
                    st.success(f"Fatura açıldı: {expense_file.name}")
                except Exception as exc:
                    st.error(f"Fatura açılamadı: {exc}")
        else:
            row_col5.caption("Dosya yok")


def render_sidebar() -> tuple[str, str, str]:
    st.sidebar.title("⚙️ Fatura Ayarları")
    fatura_motoru = st.sidebar.radio("Fatura Kesim Altyapısı:", ("GİB e-Arşiv (Taslak)", "KolayBi API"))
    st.sidebar.markdown("---")

    if find_spec("eArsivPortal") is None:
        st.sidebar.warning("eArsivPortal kurulu değil. GİB taslakları için kurulum gerekir.")
    else:
        st.sidebar.success("eArsivPortal kütüphanesi hazır.")

    if fatura_motoru == "GİB e-Arşiv (Taslak)":
        st.sidebar.info("GİB Portalına bağlanıp taslak fatura oluşturur.")
        st.session_state.setdefault("gib_kullanici_kodu", DEFAULT_GIB_KULLANICI_KODU)
        st.session_state.setdefault("gib_sifre", "")
        gib_kullanici = st.sidebar.text_input("GİB Kullanıcı Kodu", key="gib_kullanici_kodu").strip()
        gib_sifre = st.sidebar.text_input("GİB Şifresi", type="password", key="gib_sifre").strip()
        if gib_sifre:
            st.sidebar.caption("GİB şifresi alındı.")
        else:
            st.sidebar.caption("GİB şifresi bekleniyor.")
        return fatura_motoru, gib_kullanici, gib_sifre

    st.sidebar.info("KolayBi onay maili geldiğinde burayı kullanacağız.")
    st.sidebar.text_input("KolayBi API Key", type="password")
    st.sidebar.text_input("KolayBi Channel Kodu", type="password")
    return fatura_motoru, "", ""


def render_new_transaction_tab(fatura_motoru: str, gib_kullanici: str, gib_sifre: str) -> None:
    st.subheader("📝 İşlem ve Müşteri Girişi")

    popular_usdt_values = load_popular_usdt_values()

    kayit_modu = st.radio(
        "Kayıt Modu",
        ("Otomatik Hesapla", "Manuel Fatura Ekle"),
        horizontal=True,
    )

    col_mus1, col_mus2 = st.columns(2)
    with col_mus1:
        musteri_ad_soyad = st.text_input("Müşteri Adı Soyadı", placeholder="Örn: Ahmet Yılmaz")
    with col_mus2:
        musteri_tc = st.text_input(
            "Müşteri T.C. Kimlik No",
            placeholder="Yoksa 11111111111 girin",
            value=DEFAULT_TC,
        )

    islem_tarihi = st.date_input("İşlem Tarihi", date.today())

    if kayit_modu == "Otomatik Hesapla":
        st.session_state.setdefault("otomatik_satilan_usdt", 100.0)

        quick_usdt_options: list[str | float] = ["Özel değer", *popular_usdt_values]
        st.session_state.setdefault("otomatik_satilan_usdt_hizli", quick_usdt_options[0])
        previous_quick_selection = st.session_state.get("otomatik_satilan_usdt_hizli_prev")

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            quick_usdt_selection = st.selectbox(
                "Hızlı USDT Seç",
                quick_usdt_options,
                key="otomatik_satilan_usdt_hizli",
                format_func=lambda option: (
                    "Özel değer gir"
                    if option == "Özel değer"
                    else f"{float(option):,.2f} USDT"
                ),
            )
            if quick_usdt_selection != "Özel değer" and quick_usdt_selection != previous_quick_selection:
                st.session_state["otomatik_satilan_usdt"] = float(quick_usdt_selection)
            st.session_state["otomatik_satilan_usdt_hizli_prev"] = quick_usdt_selection
        with col2:
            satilan_usdt = st.number_input(
                "Satılan USDT",
                min_value=0.0,
                step=10.0,
                key="otomatik_satilan_usdt",
            )
        with col3:
            alis_kuru = st.number_input("USDT Alış Kuru", min_value=0.0, step=0.10, value=44.30, format="%.2f")
        with col4:
            satis_kuru = st.number_input("USDT Satış Kuru", min_value=0.0, step=0.10, value=48.00, format="%.2f")

        if popular_usdt_values:
            st.caption(
                "Hızlı seçim listesi geçmiş kayıtlarda en sık kullanılan USDT tutarlarından otomatik oluşturulur."
            )
        else:
            st.caption("Hızlı seçim listesi için henüz geçmiş USDT kaydı bulunmuyor.")

        hesap = calculate_invoice(satilan_usdt, alis_kuru, satis_kuru)
        preview1, preview2, preview3 = st.columns(3)
        preview1.metric("Vergisiz Bedel", f"{hesap['vergisiz_bedel']:,.2f} TL")
        preview2.metric("KDV", f"{hesap['kdv']:,.2f} TL")
        preview3.metric("Toplam Fatura", f"{hesap['toplam_fatura']:,.2f} TL")

        if st.button("HESAPLA, KAYDET VE GİB'E GÖNDER", type="primary"):
            errors, tc_no = validate_transaction_input(
                musteri_ad_soyad,
                musteri_tc,
                islem_tarihi,
                satilan_usdt,
                alis_kuru,
                satis_kuru,
            )
            if errors:
                for error in errors:
                    st.error(error)
                return

            kayit = {
                "İşlem Tarihi": islem_tarihi.strftime("%Y-%m-%d"),
                "Müşteri Adı": musteri_ad_soyad.strip(),
                "T.C. Kimlik No": tc_no,
                "Satılan USDT": round(float(satilan_usdt), 2),
                "Alış Kuru": round(float(alis_kuru), 4),
                "Satış Kuru": round(float(satis_kuru), 4),
                "Vergisiz Bedel": hesap["vergisiz_bedel"],
                "KDV": hesap["kdv"],
                "Toplam Fatura": hesap["toplam_fatura"],
                "GİB Durumu": "Kaydedildi",
                "Durum Mesajı": "İşlem veritabanına kaydedildi.",
                "Kaynak": "otomatik",
            }

            transaction_id = save_transaction(kayit)
            status, message, gib_ettn = ("Kaydedildi", "İşlem veritabanına kaydedildi.", None)
            if fatura_motoru == "GİB e-Arşiv (Taslak)":
                with st.spinner("GİB taslak durumu kontrol ediliyor..."):
                    status, message, gib_ettn = try_create_gib_draft(
                        gib_kullanici=gib_kullanici,
                        gib_sifre=gib_sifre,
                        musteri_adi=musteri_ad_soyad,
                        musteri_tc=tc_no,
                        islem_tarihi=islem_tarihi,
                        toplam_fatura=hesap["toplam_fatura"],
                    )
                    update_gib_status(
                        transaction_id,
                        status,
                        message,
                        gib_ettn=gib_ettn,
                        gib_son_senkron=now_iso() if gib_ettn else UNSET,
                    )
            else:
                update_gib_status(transaction_id, "API Bekleniyor", "KolayBi entegrasyonu henüz devrede değil.")
                status, message = "API Bekleniyor", "KolayBi entegrasyonu henüz devrede değil."

            st.success(
                f"Kayıt oluşturuldu. Vergisiz: {hesap['vergisiz_bedel']:,.2f} TL | "
                f"KDV: {hesap['kdv']:,.2f} TL | Toplam: {hesap['toplam_fatura']:,.2f} TL"
            )
            if status == "Taslak Oluşturuldu":
                st.success(message)
            elif status in {"Kimlik Bekleniyor", "Kütüphane Eksik", "API Bekleniyor"}:
                st.warning(message)
            elif status != "Kaydedildi":
                st.error(message)
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        manuel_vergisiz = st.number_input("Vergisiz Bedel", min_value=0.0, step=10.0, value=100.0)
    with col2:
        manuel_kdv = st.number_input("KDV", min_value=0.0, step=1.0, value=20.0)
    with col3:
        manuel_toplam = st.number_input("Toplam Fatura", min_value=0.0, step=10.0, value=120.0)

    beklenen_toplam = round(float(manuel_vergisiz) + float(manuel_kdv), 2)
    st.caption(f"Manuel girişte beklenen toplam: {beklenen_toplam:,.2f} TL")
    if round(float(manuel_toplam), 2) != beklenen_toplam:
        st.warning("Toplam fatura, vergisiz bedel + KDV ile eşleşmiyor.")

    if st.button("MANUEL FATURA EKLE", type="primary"):
        errors, tc_no = validate_manual_invoice_input(
            musteri_ad_soyad,
            musteri_tc,
            islem_tarihi,
            manuel_vergisiz,
            manuel_kdv,
            manuel_toplam,
        )
        if errors:
            for error in errors:
                st.error(error)
            return

        kayit = {
            "İşlem Tarihi": islem_tarihi.strftime("%Y-%m-%d"),
            "Müşteri Adı": musteri_ad_soyad.strip(),
            "T.C. Kimlik No": tc_no,
            "Satılan USDT": 0.0,
            "Alış Kuru": 0.0,
            "Satış Kuru": 0.0,
            "Vergisiz Bedel": round(float(manuel_vergisiz), 2),
            "KDV": round(float(manuel_kdv), 2),
            "Toplam Fatura": round(float(manuel_toplam), 2),
            "GİB Durumu": "Kaydedildi",
            "Durum Mesajı": "Manuel fatura veritabanına kaydedildi.",
            "Kaynak": "manuel",
        }

        transaction_id = save_transaction(kayit)
        status, message, gib_ettn = ("Kaydedildi", "Manuel fatura veritabanına kaydedildi.", None)
        if fatura_motoru == "GİB e-Arşiv (Taslak)":
            with st.spinner("GİB taslak durumu kontrol ediliyor..."):
                status, message, gib_ettn = try_create_gib_draft(
                    gib_kullanici=gib_kullanici,
                    gib_sifre=gib_sifre,
                    musteri_adi=musteri_ad_soyad,
                    musteri_tc=tc_no,
                    islem_tarihi=islem_tarihi,
                    toplam_fatura=round(float(manuel_toplam), 2),
                )
                update_gib_status(
                    transaction_id,
                    status,
                    message,
                    gib_ettn=gib_ettn,
                    gib_son_senkron=now_iso() if gib_ettn else UNSET,
                )
        else:
            update_gib_status(transaction_id, "API Bekleniyor", "KolayBi entegrasyonu henüz devrede değil.")
            status, message = "API Bekleniyor", "KolayBi entegrasyonu henüz devrede değil."

        st.success(
            f"Manuel fatura kaydedildi. Vergisiz: {float(manuel_vergisiz):,.2f} TL | "
            f"KDV: {float(manuel_kdv):,.2f} TL | Toplam: {float(manuel_toplam):,.2f} TL"
        )
        if status == "Taslak Oluşturuldu":
            st.success(message)
        elif status in {"Kimlik Bekleniyor", "Kütüphane Eksik", "API Bekleniyor"}:
            st.warning(message)
        elif status != "Kaydedildi":
            st.error(message)


def render_active_list_tab(gib_kullanici: str, gib_sifre: str) -> None:
    st.subheader("📂 Aktif İşlem Listesi")
    active_df = load_transactions(archived=False)

    if active_df.empty:
        st.info("Aktif listede kayıt bulunmuyor.")
        return

    default_start = date.today().replace(day=1)
    status_options = sorted(active_df["GİB Durumu"].dropna().unique().tolist())

    filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)
    search_text = filter_col1.text_input("Müşteri ara")
    start_date = filter_col2.date_input("Başlangıç", default_start, key="aktif_baslangic")
    end_date = filter_col3.date_input("Bitiş", date.today(), key="aktif_bitis")
    min_total = filter_col4.number_input("Minimum Fatura", min_value=0.0, value=0.0, step=100.0)
    selected_statuses = st.multiselect(
        "GİB Durum Filtreleri",
        status_options,
        default=status_options,
    )

    if end_date < start_date:
        st.error("Bitiş tarihi başlangıç tarihinden küçük olamaz.")
        return

    filtered_df = filter_transactions(
        active_df,
        search_text=search_text,
        start_date=start_date,
        end_date=end_date,
        min_total=min_total,
        status_list=selected_statuses,
    )

    summary1, summary2, summary3, summary4 = st.columns(4)
    summary1.metric("Filtrelenen Kayıt", f"{len(filtered_df)}")
    summary2.metric("Toplam Fatura", f"{filtered_df['Toplam Fatura'].sum():,.2f} TL")
    summary3.metric("Toplam KDV", f"{filtered_df['KDV'].sum():,.2f} TL")
    summary4.metric("İmza Bekleyen", f"{(filtered_df['GİB Durumu'] == 'Taslak Oluşturuldu').sum()}")

    filtered_record_ids = filtered_df["id"].astype(int).tolist()
    active_record_ids = active_df["id"].astype(int).tolist()

    sync_col1, sync_col2 = st.columns([1.2, 3])
    with sync_col1:
        if st.button("🔄 Filtreyi GİB ile Senkronize Et", disabled=not filtered_record_ids):
            with st.spinner("GİB taslak durumları okunuyor..."):
                sync_result = try_synchronize_gib_statuses(
                    gib_kullanici=gib_kullanici,
                    gib_sifre=gib_sifre,
                    transaction_ids=filtered_record_ids,
                )
            if sync_result.get("ok"):
                st.success(sync_result["message"])
                st.rerun()
            elif sync_result.get("status") in {"Kimlik Bekleniyor", "Kütüphane Eksik"}:
                st.warning(sync_result["message"])
            else:
                st.error(sync_result["message"])
    with sync_col2:
        st.caption("Filtredeki aktif kayıtların GİB taslak durumları portalden okunur. İsterseniz manuel `İmzalandı` işaretleme akışı yine kullanılabilir.")

    st.dataframe(build_display_dataframe(filtered_df), width="stretch")
    st.markdown("---")
    prune_active_selection(active_record_ids)
    selection_version = st.session_state.get(ACTIVE_SELECTION_VERSION_KEY, 0)

    col_sec1, col_sec2, col_sec3 = st.columns(3)
    with col_sec1:
        if st.button("✅ Filtredekileri Seç"):
            set_active_selection(filtered_record_ids, True, refresh_widgets=True)
            st.rerun()
    with col_sec2:
        if st.button("❌ Filtre Seçimini Kaldır"):
            set_active_selection(filtered_record_ids, False, refresh_widgets=True)
            st.rerun()
    with col_sec3:
        st.write("")

    st.write("**Faturaları İşaretleyin:**")
    row_col1, row_col2, row_col3, row_col4, row_col5 = st.columns([0.5, 2, 1.1, 1.1, 1.4])
    row_col1.write("Seç")
    row_col2.write("Müşteri")
    row_col3.write("Toplam")
    row_col4.write("KDV")
    row_col5.write("GİB")

    for _, row in filtered_df.iterrows():
        record_id = int(row["id"])
        checkbox_key = f"fatura_{selection_version}_{record_id}"
        cols = st.columns([0.5, 2, 1.1, 1.1, 1.4])
        with cols[0]:
            st.checkbox(
                "Seç",
                value=record_id in get_active_selection(),
                key=checkbox_key,
                label_visibility="collapsed",
                on_change=sync_active_checkbox,
                args=(record_id, checkbox_key),
            )
        with cols[1]:
            st.write(f"**{row['Müşteri Adı']}**")
        with cols[2]:
            st.write(f"{row['Toplam Fatura']:,.2f} TL")
        with cols[3]:
            st.write(f"{row['KDV']:,.2f} TL")
        with cols[4]:
            st.write(str(row["GİB Durumu"]))

    selected_ids = [
        int(record_id)
        for record_id in filtered_record_ids
        if int(record_id) in get_active_selection()
    ]

    st.markdown("---")
    if selected_ids:
        st.success(f"{len(selected_ids)} kayıt seçildi.")
        selected_df = filtered_df[filtered_df["id"].isin(selected_ids)].copy()
        selected_xlsx, selected_name = export_dataframe_as_xlsx(
            selected_df,
            file_name=f"Secili_Faturalar_{date.today()}.xlsx",
            sheet_name="SeciliFaturalar",
        )

        action_col1, action_col2, action_col3 = st.columns(3)
        action_col1.download_button(
            label="📥 Seçili Faturaları Excel (XLSX) Olarak İndir",
            data=selected_xlsx,
            file_name=selected_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        if action_col2.button("✍️ GİB'de İmzalandı Olarak İşaretle"):
            update_gib_status_bulk(
                selected_ids,
                "İmzalandı",
                "Kayıt, kullanıcı tarafından GİB portalında imzalandı olarak işaretlendi.",
            )
            st.success(f"{len(selected_ids)} kayıt `İmzalandı` durumuna alındı.")
            st.rerun()
        confirm_active_delete = action_col3.checkbox("Silmeyi onayla", key="aktif_sil_onay")
        if action_col3.button("🗑️ Seçili Faturaları Sil", type="secondary", disabled=not confirm_active_delete):
            deleted_count = delete_transactions(selected_ids)
            set_active_selection(selected_ids, False, refresh_widgets=True)
            if deleted_count:
                st.success(f"{deleted_count} kayıt silindi.")
            else:
                st.warning("Seçili kayıtlar silinemedi veya zaten kaldırılmıştı.")
            st.rerun()

        st.markdown("---")
        st.write("**Seçili Kayıtları Arşive Taşı**")

        archive_groups = load_archive_groups()
        hafta = get_hafta_bilgisi()
        aktif_hedef_tipi = st.radio(
            "Arşiv hedefi",
            ("Mevcut arşive ekle", "Yeni arşiv oluştur"),
            horizontal=True,
            key="aktif_arsiv_hedef_tipi",
        )

        aktif_archive_key = None
        aktif_archive_label = None
        if aktif_hedef_tipi == "Mevcut arşive ekle" and not archive_groups.empty:
            active_archive_options = {
                f"{row['Arşiv Etiketi']} | {int(row['Kayıt Adedi'])} kayıt": (
                    row["Arşiv Hafta Kodu"],
                    row["Arşiv Etiketi"],
                )
                for _, row in archive_groups.iterrows()
            }
            chosen_active_archive = st.selectbox(
                "Hedef arşiv",
                list(active_archive_options.keys()),
                key="aktif_mevcut_arsiv_sec",
            )
            aktif_archive_key, aktif_archive_label = active_archive_options[chosen_active_archive]
        else:
            aktif_archive_label = st.text_input(
                "Yeni arşiv etiketi",
                value=hafta["etiket"],
                key="aktif_yeni_arsiv_etiketi",
            )

        if st.button("📦 Seçili Faturaları Arşive Taşı", type="primary"):
            move_errors: list[str] = []
            if aktif_hedef_tipi == "Mevcut arşive ekle" and archive_groups.empty:
                move_errors.append("Hedef olarak seçilecek mevcut arşiv bulunmuyor. Yeni arşiv oluşturun.")
            if aktif_hedef_tipi == "Yeni arşiv oluştur" and not str(aktif_archive_label).strip():
                move_errors.append("Yeni arşiv etiketi boş bırakılamaz.")

            if move_errors:
                for error in move_errors:
                    st.error(error)
            else:
                target_label = str(aktif_archive_label).strip()
                target_key = aktif_archive_key or make_archive_key(target_label, date.today())
                selected_transactions_df = load_transactions_by_ids(selected_ids)
                archive_conflicts = find_archive_conflicts(selected_transactions_df)
                if not archive_conflicts.empty:
                    st.error(build_archive_conflict_message(archive_conflicts))
                else:
                    moved_count = move_transactions_to_archive(selected_ids, target_key, target_label)
                    set_active_selection(selected_ids, False, refresh_widgets=True)
                    if moved_count:
                        st.success(f"{moved_count} kayıt `{target_label}` arşivine taşındı.")
                        st.rerun()
                    st.warning("Seçili kayıtlar arşive taşınamadı.")

    all_xlsx, all_name = export_dataframe_as_xlsx(
        filtered_df,
        file_name=f"Furkan_P2P_Aktif_{date.today()}.xlsx",
        sheet_name="AktifKayitlar",
    )
    st.download_button(
        label="📥 Filtrelenmiş Listeyi Excel (XLSX) Olarak İndir",
        data=all_xlsx,
        file_name=all_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def render_archive_tab(fatura_motoru: str, gib_kullanici: str, gib_sifre: str) -> None:
    st.subheader("🗄️ Haftalık Arşiv ve Dönem Kapatma")
    hafta = get_hafta_bilgisi()
    archive_groups = load_archive_groups()
    archive_duplicates_df = find_cross_archive_duplicate_invoices()
    all_transactions_df = load_transactions(archived=None)

    st.info(f"📅 Bu Hafta: {hafta['etiket']}")
    st.warning(
        "Hafta bittiğinde butona bastığınızda aktif kayıtlar önce XLSX yedeğine alınır, sonra tek işlemde arşivlenir."
    )

    if not archive_duplicates_df.empty:
        duplicate_invoice_count = len(
            archive_duplicates_df[ARCHIVE_MATCH_KOLONLARI].drop_duplicates()
        )
        st.warning(
            f"Arşivler arasında {duplicate_invoice_count} adet birebir çakışan fatura bulundu. Aşağıdan inceleyip silebilirsiniz."
        )

        with st.expander("⚠️ Arşivler Arası Çakışan Faturalar", expanded=True):
            duplicate_display_df = archive_duplicates_df[
                [
                    "id",
                    "İşlem Tarihi",
                    "Müşteri Adı",
                    "T.C. Kimlik No",
                    "Toplam Fatura",
                    "Arşiv Etiketi",
                    "Çakışan Arşiv Sayısı",
                    "Çakışan Kayıt Sayısı",
                ]
            ].copy()
            st.dataframe(duplicate_display_df, width="stretch", hide_index=True)

            # Her çakışma grubunda en düşük ID kalır, diğerleri silinecek olarak işaretlenir
            dup_norm = normalize_invoice_identity(archive_duplicates_df)
            dup_norm["id"] = archive_duplicates_df["id"].astype(int)
            safe_to_delete_ids: list[int] = []
            for _, group_df in dup_norm.groupby(ARCHIVE_MATCH_KOLONLARI):
                sorted_ids = sorted(group_df["id"].tolist())
                safe_to_delete_ids.extend(sorted_ids[1:])  # en küçük ID'yi koru, geri kalanları sil

            st.info(
                f"Her çakışma grubundan **en eski kayıt (en küçük ID) korunur**, "
                f"fazla {len(safe_to_delete_ids)} kopya otomatik seçilebilir."
            )

            confirm_auto_delete = st.checkbox(
                f"Her gruptan 1'er kopya bırakıp fazla {len(safe_to_delete_ids)} kaydı silmeyi onayla",
                key="arsiv_cakisma_otomatik_onay",
            )
            if st.button(
                "🧹 Her Gruptan 1 Kopya Bırak, Fazlaları Sil",
                type="primary",
                disabled=not confirm_auto_delete,
            ):
                deleted_count = delete_archived_transactions_by_ids(safe_to_delete_ids)
                if deleted_count:
                    st.success(f"{deleted_count} fazla kopya silindi. Her faturadan 1 kayıt kaldı.")
                    st.rerun()
                st.warning("Silinecek fazla kopya bulunamadı.")

            st.markdown("---")
            duplicate_delete_options = {
                (
                    f"ID {int(row['id'])} | {row['Müşteri Adı']} | {row['İşlem Tarihi']} | "
                    f"{float(row['Toplam Fatura']):,.2f} TL | {row['Arşiv Etiketi']}"
                ): int(row["id"])
                for _, row in archive_duplicates_df.iterrows()
            }
            selected_duplicate_labels = st.multiselect(
                "Ya da silinecekleri manuel seç",
                list(duplicate_delete_options.keys()),
                key="arsiv_cakisma_silinecekler",
            )
            confirm_duplicate_delete = st.checkbox(
                "Seçili çakışan kayıtları silmeyi onayla",
                key="arsiv_cakisma_sil_onay",
            )
            if st.button(
                "🗑️ Seçili Çakışan Kayıtları Sil",
                type="secondary",
                disabled=not confirm_duplicate_delete or not selected_duplicate_labels,
            ):
                selected_duplicate_ids = [duplicate_delete_options[label] for label in selected_duplicate_labels]
                deletion_risks = find_full_duplicate_deletion_risks(selected_duplicate_ids)
                if not deletion_risks.empty:
                    st.error("Her çakışma grubunda en az bir kopya kalmalı. Seçiminiz bazı faturaların tüm kopyalarını siliyor.")
                    st.dataframe(deletion_risks, width="stretch", hide_index=True)
                else:
                    deleted_count = delete_archived_transactions_by_ids(selected_duplicate_ids)
                    if deleted_count:
                        st.success(f"{deleted_count} çakışan arşiv kaydı silindi.")
                        st.rerun()
                    st.warning("Seçilen çakışan kayıtlar silinemedi veya zaten kaldırılmıştı.")
    else:
        st.success("Arşivler arasında birebir çakışan fatura bulunmuyor.")

    haftalik_hedef_tipi = st.radio(
        "Bu haftaki kayıtlar hangi arşive eklensin?",
        ("Yeni arşiv oluştur", "Mevcut arşive ekle"),
        horizontal=True,
        key="haftalik_arsiv_hedef_tipi",
    )

    haftalik_archive_key = None
    haftalik_archive_label = None
    if haftalik_hedef_tipi == "Mevcut arşive ekle" and not archive_groups.empty:
        haftalik_options = {
            f"{row['Arşiv Etiketi']} | {int(row['Kayıt Adedi'])} kayıt": (
                row["Arşiv Hafta Kodu"],
                row["Arşiv Etiketi"],
            )
            for _, row in archive_groups.iterrows()
        }
        chosen_haftalik_archive = st.selectbox(
            "Hedef arşiv",
            list(haftalik_options.keys()),
            key="haftalik_mevcut_arsiv_sec",
        )
        haftalik_archive_key, haftalik_archive_label = haftalik_options[chosen_haftalik_archive]
    else:
        haftalik_archive_label = st.text_input(
            "Yeni arşiv etiketi",
            value=hafta["etiket"],
            key="haftalik_yeni_arsiv_etiketi",
        )

    if st.button("📦 BU HAFTA İŞLEMLERİNİ GÜVENLİ ARŞİVE KALDIR", type="primary"):
        weekly_errors: list[str] = []
        if haftalik_hedef_tipi == "Mevcut arşive ekle" and archive_groups.empty:
            weekly_errors.append("Eklenecek mevcut arşiv bulunmuyor. Yeni arşiv oluştur seçeneğini kullanın.")
        if haftalik_hedef_tipi == "Yeni arşiv oluştur" and not str(haftalik_archive_label).strip():
            weekly_errors.append("Yeni arşiv etiketi boş bırakılamaz.")

        if weekly_errors:
            for error in weekly_errors:
                st.error(error)
        else:
            resolved_weekly_label = str(haftalik_archive_label).strip()
            resolved_weekly_key = haftalik_archive_key
            if haftalik_hedef_tipi == "Yeni arşiv oluştur":
                resolved_weekly_key = (
                    hafta["hafta_kodu"]
                    if resolved_weekly_label == hafta["etiket"]
                    else make_archive_key(resolved_weekly_label, hafta["pazartesi"])
                )

            try:
                success, message = archive_active_transactions(
                    target_archive_key=resolved_weekly_key,
                    target_archive_label=resolved_weekly_label,
                )
            except Exception as exc:
                st.error(f"Arşivleme başarısız oldu: {exc}")
            else:
                if success:
                    st.success(message)
                    st.rerun()
                else:
                    st.warning(message)

    st.markdown("---")
    st.subheader("📚 Geçmiş Haftaların Arşivi")

    with st.expander("➕ Arşive Fatura Ekle"):
        hedef_tipi = st.radio(
            "Arşiv hedefi",
            ("Mevcut arşive ekle", "Yeni arşiv oluştur"),
            horizontal=True,
            key="arsiv_hedef_tipi",
        )

        selected_archive_key = None
        selected_archive_label = None
        if hedef_tipi == "Mevcut arşive ekle" and not archive_groups.empty:
            existing_options = {
                f"{row['Arşiv Etiketi']} | {int(row['Kayıt Adedi'])} kayıt": (
                    row["Arşiv Hafta Kodu"],
                    row["Arşiv Etiketi"],
                )
                for _, row in archive_groups.iterrows()
            }
            chosen_existing = st.selectbox("Mevcut arşiv", list(existing_options.keys()), key="mevcut_arsiv_sec")
            selected_archive_key, selected_archive_label = existing_options[chosen_existing]
        else:
            selected_archive_label = st.text_input(
                "Yeni arşiv etiketi",
                value=hafta["etiket"],
                key="yeni_arsiv_etiketi",
            )

        ar_col1, ar_col2 = st.columns(2)
        with ar_col1:
            arsiv_musteri = st.text_input("Müşteri Adı Soyadı", key="arsiv_musteri")
        with ar_col2:
            arsiv_tc = st.text_input(
                "Müşteri T.C. Kimlik No",
                value=DEFAULT_TC,
                key="arsiv_tc",
            )

        ar_col3, ar_col4, ar_col5, ar_col6 = st.columns(4)
        with ar_col3:
            arsiv_tarih = st.date_input("İşlem Tarihi", date.today(), key="arsiv_tarih")
        with ar_col4:
            arsiv_vergisiz = st.number_input("Vergisiz Bedel", min_value=0.0, step=10.0, value=100.0, key="arsiv_vergisiz")
        with ar_col5:
            arsiv_kdv = st.number_input("KDV", min_value=0.0, step=1.0, value=20.0, key="arsiv_kdv")
        with ar_col6:
            arsiv_toplam = st.number_input("Toplam Fatura", min_value=0.0, step=10.0, value=120.0, key="arsiv_toplam")

        st.caption(f"Arşiv girişi beklenen toplam: {arsiv_vergisiz + arsiv_kdv:,.2f} TL")

        if st.button("📥 ARŞİVE FATURA EKLE", type="primary"):
            errors, tc_no = validate_manual_invoice_input(
                arsiv_musteri,
                arsiv_tc,
                arsiv_tarih,
                arsiv_vergisiz,
                arsiv_kdv,
                arsiv_toplam,
            )

            if hedef_tipi == "Mevcut arşive ekle" and archive_groups.empty:
                errors.append("Eklemek için mevcut arşiv yok. Yeni arşiv oluştur seçeneğini kullanın.")
            if hedef_tipi == "Yeni arşiv oluştur" and not str(selected_archive_label).strip():
                errors.append("Yeni arşiv etiketi boş bırakılamaz.")

            if errors:
                for error in errors:
                    st.error(error)
                return

            archive_key = selected_archive_key or make_archive_key(str(selected_archive_label), arsiv_tarih)
            archive_label = str(selected_archive_label).strip()
            archive_conflicts = find_archive_conflicts(
                pd.DataFrame(
                    [
                        {
                            "İşlem Tarihi": arsiv_tarih.strftime("%Y-%m-%d"),
                            "Müşteri Adı": arsiv_musteri.strip(),
                            "T.C. Kimlik No": tc_no,
                            "Vergisiz Bedel": round(float(arsiv_vergisiz), 2),
                            "KDV": round(float(arsiv_kdv), 2),
                            "Toplam Fatura": round(float(arsiv_toplam), 2),
                        }
                    ]
                )
            )
            if not archive_conflicts.empty:
                st.error(build_archive_conflict_message(archive_conflicts))
                return

            kayit = {
                "İşlem Tarihi": arsiv_tarih.strftime("%Y-%m-%d"),
                "Müşteri Adı": arsiv_musteri.strip(),
                "T.C. Kimlik No": tc_no,
                "Satılan USDT": 0.0,
                "Alış Kuru": 0.0,
                "Satış Kuru": 0.0,
                "Vergisiz Bedel": round(float(arsiv_vergisiz), 2),
                "KDV": round(float(arsiv_kdv), 2),
                "Toplam Fatura": round(float(arsiv_toplam), 2),
                "GİB Durumu": "Kaydedildi",
                "Durum Mesajı": "Fatura doğrudan arşive kaydedildi.",
                "Kaynak": "manuel_arsiv",
                "Arşiv Hafta Kodu": archive_key,
                "Arşiv Etiketi": archive_label,
            }

            transaction_id = save_transaction(kayit)
            status, message, gib_ettn = ("Kaydedildi", "Fatura doğrudan arşive kaydedildi.", None)
            if fatura_motoru == "GİB e-Arşiv (Taslak)":
                with st.spinner("GİB taslak durumu kontrol ediliyor..."):
                    status, message, gib_ettn = try_create_gib_draft(
                        gib_kullanici=gib_kullanici,
                        gib_sifre=gib_sifre,
                        musteri_adi=arsiv_musteri,
                        musteri_tc=tc_no,
                        islem_tarihi=arsiv_tarih,
                        toplam_fatura=round(float(arsiv_toplam), 2),
                    )
                    update_gib_status(
                        transaction_id,
                        status,
                        message,
                        gib_ettn=gib_ettn,
                        gib_son_senkron=now_iso() if gib_ettn else UNSET,
                    )
            else:
                update_gib_status(transaction_id, "API Bekleniyor", "KolayBi entegrasyonu henüz devrede değil.")

            st.success(f"Fatura arşive eklendi: {archive_label}")
            st.rerun()

    with st.expander("🧾 Tarih Aralığından CSV Arşiv Oluştur"):
        if all_transactions_df.empty:
            st.info("CSV arşivi oluşturmak için kayıt bulunmuyor.")
        else:
            csv_working_df = all_transactions_df.copy()
            csv_working_df["İşlem Tarihi"] = pd.to_datetime(csv_working_df["İşlem Tarihi"], errors="coerce")
            csv_working_df = csv_working_df.dropna(subset=["İşlem Tarihi"]).reset_index(drop=True)

            if csv_working_df.empty:
                st.info("CSV arşivi için tarih bilgisi bulunan kayıt yok.")
            else:
                min_csv_date = csv_working_df["İşlem Tarihi"].min().date()
                max_csv_date = csv_working_df["İşlem Tarihi"].max().date()
                max_csv_selectable_date = max(max_csv_date, date.today())
                default_csv_start = max(min_csv_date, date.today().replace(day=1))
                default_csv_end = date.today()

                csv_col1, csv_col2 = st.columns(2)
                csv_start_date = csv_col1.date_input(
                    "CSV Başlangıç Tarihi",
                    value=default_csv_start,
                    min_value=min_csv_date,
                    max_value=max_csv_selectable_date,
                    key="csv_arsiv_baslangic",
                )
                csv_end_date = csv_col2.date_input(
                    "CSV Bitiş Tarihi",
                    value=default_csv_end,
                    min_value=min_csv_date,
                    max_value=max_csv_selectable_date,
                    key="csv_arsiv_bitis",
                )

                if csv_end_date < csv_start_date:
                    st.error("CSV bitiş tarihi başlangıç tarihinden küçük olamaz.")
                else:
                    csv_filtered_df = csv_working_df[
                        (csv_working_df["İşlem Tarihi"] >= pd.Timestamp(csv_start_date))
                        & (csv_working_df["İşlem Tarihi"] <= pd.Timestamp(csv_end_date))
                    ].copy()

                    st.caption(
                        "Seçilen tarih aralığındaki tüm faturalar arşiv dışa aktarma formatında CSV olarak hazırlanır ve son satıra toplam eklenir."
                    )

                    if csv_filtered_df.empty:
                        st.warning("Seçilen tarih aralığında CSV oluşturacak fatura bulunmuyor.")
                    else:
                        csv_export_name = (
                            f"arsiv_{csv_start_date.strftime('%Y%m%d')}_{csv_end_date.strftime('%Y%m%d')}.csv"
                        )
                        csv_bytes, csv_name = export_dataframe_as_csv(csv_filtered_df, csv_export_name)

                        summary_col1, summary_col2, summary_col3 = st.columns(3)
                        summary_col1.metric("Kayıt Sayısı", f"{len(csv_filtered_df)}")
                        summary_col2.metric("Toplam Fatura", f"{csv_filtered_df['Toplam Fatura'].sum():,.2f} TL")
                        summary_col3.metric("Toplam KDV", f"{csv_filtered_df['KDV'].sum():,.2f} TL")

                        st.download_button(
                            label="📥 Tarih Aralığını CSV Olarak İndir",
                            data=csv_bytes,
                            file_name=csv_name,
                            mime="text/csv",
                        )

    if archive_groups.empty:
        st.info("Henüz arşivlenmiş hafta bulunmuyor.")
        return

    options = {
        (
            f"{row['Arşiv Etiketi']} | {int(row['Kayıt Adedi'])} kayıt | "
            f"{row['Toplam Fatura']:,.2f} TL"
        ): row["Arşiv Hafta Kodu"]
        for _, row in archive_groups.iterrows()
    }
    selected_label = st.selectbox("Arşiv seçin:", list(options.keys()))
    selected_key = options[selected_label]

    archived_df = load_transactions(archived=True)
    archived_df = archived_df[archived_df["Arşiv Hafta Kodu"] == selected_key].reset_index(drop=True)

    st.dataframe(build_display_dataframe(archived_df), width="stretch")

    metric1, metric2, metric3 = st.columns(3)
    metric1.metric("Toplam Fatura", f"{archived_df['Toplam Fatura'].sum():,.2f} TL")
    metric2.metric("Toplam KDV", f"{archived_df['KDV'].sum():,.2f} TL")
    metric3.metric("Vergisiz Bedel", f"{archived_df['Vergisiz Bedel'].sum():,.2f} TL")

    st.markdown("---")
    st.write("**Bu Arşivi Başka Arşive Taşı**")

    selected_archive_row = archive_groups.loc[archive_groups["Arşiv Hafta Kodu"] == selected_key].iloc[0]
    current_archive_label = str(selected_archive_row["Arşiv Etiketi"])
    other_archive_groups = archive_groups[archive_groups["Arşiv Hafta Kodu"] != selected_key].copy()
    source_archive_date = pd.to_datetime(archived_df["İşlem Tarihi"], errors="coerce").dropna().min()
    source_reference_date = source_archive_date.date() if pd.notna(source_archive_date) else date.today()

    archive_move_target_type = st.radio(
        "Taşıma hedefi",
        ("Mevcut arşive taşı", "Yeni arşive taşı"),
        horizontal=True,
        key="arsiv_tasima_hedef_tipi",
    )

    target_archive_key = None
    target_archive_label = None
    if archive_move_target_type == "Mevcut arşive taşı" and not other_archive_groups.empty:
        move_archive_options = {
            f"{row['Arşiv Etiketi']} | {int(row['Kayıt Adedi'])} kayıt": (
                row["Arşiv Hafta Kodu"],
                row["Arşiv Etiketi"],
            )
            for _, row in other_archive_groups.iterrows()
        }
        chosen_target_archive = st.selectbox(
            "Hedef arşiv",
            list(move_archive_options.keys()),
            key="arsiv_tasima_mevcut_hedef",
        )
        target_archive_key, target_archive_label = move_archive_options[chosen_target_archive]
    else:
        target_archive_label = st.text_input(
            "Yeni arşiv etiketi",
            value=f"{current_archive_label} - Taşınan",
            key="arsiv_tasima_yeni_etiket",
        )

    if st.button("📦 Bu Arşivi Başka Arşive Taşı", type="primary"):
        move_archive_errors: list[str] = []
        if archive_move_target_type == "Mevcut arşive taşı" and other_archive_groups.empty:
            move_archive_errors.append("Taşınacak başka mevcut arşiv yok. Yeni arşiv oluştur seçeneğini kullanın.")
        if archive_move_target_type == "Yeni arşive taşı" and not str(target_archive_label).strip():
            move_archive_errors.append("Yeni arşiv etiketi boş bırakılamaz.")

        resolved_target_label = str(target_archive_label).strip()
        resolved_target_key = target_archive_key or make_archive_key(resolved_target_label, source_reference_date)
        if resolved_target_key == selected_key:
            move_archive_errors.append("Kaynak ve hedef arşiv aynı olamaz.")

        if move_archive_errors:
            for error in move_archive_errors:
                st.error(error)
        else:
            moved_count = move_archive_to_archive(selected_key, resolved_target_key, resolved_target_label)
            if moved_count:
                st.success(f"{moved_count} kayıt `{current_archive_label}` arşivinden `{resolved_target_label}` arşivine taşındı.")
                st.rerun()
            st.warning("Seçilen arşiv taşınamadı veya taşınacak kayıt bulunamadı.")

    st.caption("Arşivi sildiğinizde kayıtlar aktif listeye geri döner; klasördeki XLSX yedek dosyaları korunur.")
    st.caption("Arşivler arası taşıma yalnızca veritabanındaki arşiv etiketini değiştirir; mevcut XLSX yedekleri korunur.")

    archive_xlsx, archive_name = export_dataframe_as_xlsx(
        archived_df,
        file_name=f"{selected_key}.xlsx",
        sheet_name="Arsiv",
    )
    archive_action_col1, archive_action_col2 = st.columns(2)
    archive_action_col1.download_button(
        label="📥 Bu Arşivi Excel (XLSX) Olarak İndir",
        data=archive_xlsx,
        file_name=archive_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    confirm_archive_delete = archive_action_col2.checkbox("Arşivi silmeyi onayla", key="arsiv_sil_onay")
    if archive_action_col2.button("🗑️ Bu Arşivi Sil", type="secondary", disabled=not confirm_archive_delete):
        restored_count = restore_archive_to_active(selected_key)
        if restored_count:
            st.success(f"{restored_count} kayıt aktif listeye geri alındı ve arşiv kaldırıldı.")
            st.rerun()
        st.warning("Seçilen arşivde geri alınacak kayıt bulunamadı.")


def inject_premium_css() -> None:
    st.markdown("""
<style>
/* ═══════════════════════════════════════════════════
   DARK PREMIUM THEME — Streamlit overrides
   ═══════════════════════════════════════════════════ */

/* ── Base ───────────────────────────────────────── */
html, body, [data-testid="stApp"], .stApp,
[data-testid="stAppViewContainer"], .main {
    font-family: "Segoe UI Variable","Segoe UI","Inter","Helvetica Neue",sans-serif !important;
    background: linear-gradient(160deg,#08131a 0%,#0e1b25 100%) !important;
    color: #edf4f8 !important;
}
[data-testid="stMain"], [data-testid="stMainBlockContainer"],
.block-container {
    background: transparent !important;
    color: #edf4f8 !important;
    padding: 2rem 2.5rem 3rem !important;
    max-width: 1400px !important;
}

/* ── Hide chrome ────────────────────────────────── */
#MainMenu, footer, header { display: none !important; }

/* ── Sidebar ────────────────────────────────────── */
[data-testid="stSidebar"], [data-testid="stSidebarContent"] {
    background: rgba(9,20,29,.92) !important;
    border-right: 1px solid rgba(148,163,184,.12) !important;
    backdrop-filter: blur(18px) !important;
}
[data-testid="stSidebar"] * { color: #bac5cf !important; }
[data-testid="stSidebar"] h1,[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3,[data-testid="stSidebar"] strong {
    color: #edf4f8 !important;
}
[data-testid="stSidebar"] .stRadio label { font-size: 13px !important; font-weight: 600 !important; }

/* ── Headings ───────────────────────────────────── */
h1 { font-size: 28px !important; font-weight: 700 !important; letter-spacing: -.04em !important; color: #edf4f8 !important; }
h2 { font-size: 22px !important; font-weight: 700 !important; letter-spacing: -.03em !important; color: #edf4f8 !important; }
h3 { font-size: 16px !important; font-weight: 700 !important; color: #edf4f8 !important; }
p, li, span, label { color: #bac5cf !important; }

/* ── Tabs ───────────────────────────────────────── */
[data-testid="stTabs"] [role="tablist"] {
    background: rgba(13,27,36,.80) !important;
    border-radius: 18px !important;
    border: 1px solid rgba(148,163,184,.14) !important;
    padding: 5px !important;
    gap: 4px !important;
    box-shadow: 0 10px 28px rgba(2,8,23,.32) !important;
}
[data-testid="stTabs"] button[role="tab"] {
    border-radius: 13px !important;
    font-size: 13px !important;
    font-weight: 700 !important;
    color: #7e8b97 !important;
    padding: 10px 18px !important;
    border: 1px solid transparent !important;
    background: transparent !important;
    transition: all .15s !important;
}
[data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
    background: linear-gradient(135deg,#0f766e 0%,#1aa598 100%) !important;
    color: #fff !important;
    box-shadow: 0 10px 24px rgba(15,118,110,.28) !important;
    border-color: transparent !important;
}
[data-testid="stTabs"] button[role="tab"]:hover:not([aria-selected="true"]) {
    background: rgba(13,27,36,.90) !important;
    border-color: rgba(148,163,184,.18) !important;
    color: #edf4f8 !important;
}
/* tab panel bg */
[data-testid="stTabs"] [role="tabpanel"] {
    background: transparent !important;
}

/* ── Metric cards ───────────────────────────────── */
[data-testid="metric-container"] {
    background: linear-gradient(180deg,#11232d 0%,rgba(13,27,36,.78) 100%) !important;
    border: 1px solid rgba(148,163,184,.14) !important;
    border-radius: 20px !important;
    padding: 18px 18px 16px !important;
    box-shadow: 0 12px 28px rgba(2,8,23,.24) !important;
}
[data-testid="metric-container"] [data-testid="stMetricLabel"] {
    font-size: 11px !important; text-transform: uppercase !important;
    letter-spacing: .12em !important; color: #7e8b97 !important; font-weight: 700 !important;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-size: 26px !important; font-weight: 700 !important;
    letter-spacing: -.04em !important; color: #edf4f8 !important;
}
[data-testid="metric-container"] [data-testid="stMetricDelta"] { font-size: 12px !important; font-weight: 700 !important; }

/* ── Inputs ─────────────────────────────────────── */
.stTextInput input, .stNumberInput input, .stDateInput input,
.stTextArea textarea {
    border: 1px solid rgba(148,163,184,.18) !important;
    border-radius: 14px !important;
    background: rgba(9,20,29,.86) !important;
    color: #edf4f8 !important;
    font-size: 13px !important;
    transition: border .15s, box-shadow .15s !important;
}
.stTextInput input:focus, .stNumberInput input:focus, .stTextArea textarea:focus {
    border-color: #33b6aa !important;
    box-shadow: 0 0 0 4px rgba(51,182,170,.2) !important;
}
/* selectbox */
[data-baseweb="select"] > div {
    background: rgba(9,20,29,.86) !important;
    border: 1px solid rgba(148,163,184,.18) !important;
    border-radius: 14px !important;
    color: #edf4f8 !important;
}
[data-baseweb="popover"] [role="option"] {
    background: #0e1b25 !important; color: #edf4f8 !important;
}
[data-baseweb="popover"] [role="option"]:hover {
    background: rgba(51,182,170,.15) !important;
}

/* ── Buttons ────────────────────────────────────── */
.stButton > button {
    border-radius: 14px !important;
    font-size: 13px !important;
    font-weight: 700 !important;
    padding: 10px 20px !important;
    border: 1px solid rgba(148,163,184,.18) !important;
    background: rgba(13,27,36,.78) !important;
    color: #bac5cf !important;
    box-shadow: 0 10px 24px rgba(2,8,23,.24) !important;
    transition: all .15s !important;
}
.stButton > button:hover {
    border-color: #33b6aa !important;
    color: #33b6aa !important;
    transform: translateY(-1px) !important;
}
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg,#0f766e 0%,#1aa598 100%) !important;
    color: #fff !important;
    border-color: transparent !important;
    box-shadow: 0 14px 28px rgba(15,118,110,.28) !important;
}
.stButton > button[kind="primary"]:hover {
    background: linear-gradient(135deg,#115e59 0%,#0f766e 100%) !important;
}

/* ── Checkbox / radio ───────────────────────────── */
[data-testid="stCheckbox"] label, [data-testid="stRadio"] label { color: #bac5cf !important; }

/* ── Date picker ────────────────────────────────── */
[data-baseweb="calendar"] { background: #0e1b25 !important; }

/* ── Alerts ─────────────────────────────────────── */
[data-testid="stSuccess"] { background: #0f2a1a !important; border: 1px solid #166534 !important; border-radius: 14px !important; color: #86efac !important; }
[data-testid="stError"]   { background: #2a1010 !important; border: 1px solid #7f1d1d !important; border-radius: 14px !important; color: #fca5a5 !important; }
[data-testid="stWarning"] { background: #2a1f00 !important; border: 1px solid #92400e !important; border-radius: 14px !important; color: #fde68a !important; }
[data-testid="stInfo"]    { background: #10233d !important; border: 1px solid #1d4ed8 !important; border-radius: 14px !important; color: #93c5fd !important; }
[data-testid="stSuccess"] *, [data-testid="stError"] *,
[data-testid="stWarning"] *, [data-testid="stInfo"] * { color: inherit !important; }

/* ── DataFrames / Tables ────────────────────────── */
[data-testid="stDataFrame"] {
    border-radius: 20px !important;
    overflow: hidden !important;
    border: 1px solid rgba(148,163,184,.14) !important;
    box-shadow: 0 12px 28px rgba(2,8,23,.24) !important;
}
[data-testid="stDataFrame"] th {
    background: rgba(16,33,43,.94) !important;
    color: #7e8b97 !important;
    font-size: 10px !important;
    text-transform: uppercase !important;
    letter-spacing: .14em !important;
    font-weight: 700 !important;
}
[data-testid="stDataFrame"] td { color: #edf4f8 !important; }

/* ── Expander ───────────────────────────────────── */
[data-testid="stExpander"] {
    border: 1px solid rgba(148,163,184,.14) !important;
    border-radius: 18px !important;
    background: rgba(13,27,36,.78) !important;
    box-shadow: 0 12px 28px rgba(2,8,23,.24) !important;
}
[data-testid="stExpander"] summary { color: #bac5cf !important; font-weight: 700 !important; }

/* ── Divider ────────────────────────────────────── */
hr { border-color: rgba(148,163,184,.14) !important; }

/* ── Scrollbar ──────────────────────────────────── */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: rgba(51,182,170,.28); border-radius: 999px; }
::-webkit-scrollbar-thumb:hover { background: rgba(51,182,170,.55); }

/* ── Caption / markdown ─────────────────────────── */
[data-testid="stCaptionContainer"] p,
.stMarkdown p, .stMarkdown li { color: #7e8b97 !important; }
</style>
""", unsafe_allow_html=True)


def render_app() -> None:
    ensure_storage()
    ensure_database()
    migrate_legacy_files()

    st.set_page_config(page_title="P2P Ticaret Paneli", layout="wide", page_icon="📊")
    inject_premium_css()
    st.markdown('<p style="font-size:11px;font-weight:700;letter-spacing:.16em;text-transform:uppercase;color:#0f766e;margin-bottom:6px">P2P TİCARET PANELİ</p>', unsafe_allow_html=True)
    st.title("Furkan — P2P Ticaret Paneli")
    st.markdown('<p style="font-size:14px;color:#52606d;margin-top:-12px;margin-bottom:8px">GİB e-Arşiv entegrasyonlu işlem yönetimi</p>', unsafe_allow_html=True)

    fatura_motoru, gib_kullanici, gib_sifre = render_sidebar()
    all_transactions_df = load_transactions(archived=None)
    render_global_dashboard(all_transactions_df)

    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "🚀 Yeni İşlem Gir",
        "📂 Aktif Liste",
        "🗄️ Fatura Arşivi",
        "📊 İstatistikler",
        "💰 Finans Özeti",
        "🧾 Gider Paneli",
        "🌐 P2P Fatura Paneli",
    ])
    with tab1:
        render_new_transaction_tab(fatura_motoru, gib_kullanici, gib_sifre)
    with tab2:
        render_active_list_tab(gib_kullanici, gib_sifre)
    with tab3:
        render_archive_tab(fatura_motoru, gib_kullanici, gib_sifre)
    with tab4:
        render_statistics_tab(all_transactions_df)
    with tab5:
        render_finance_summary_tab(all_transactions_df)
    with tab6:
        render_expense_panel_tab()
    with tab7:
        _html_path = BASE_DIR / "P2P Fatura" / "p2p_html.html"
        _css_path = BASE_DIR / "styles" / "p2p-premium.css"
        if _html_path.exists():
            _html_content = _html_path.read_text(encoding="utf-8")
            if _css_path.exists():
                _css_content = _css_path.read_text(encoding="utf-8")
                _html_content = _html_content.replace(
                    '<link rel="stylesheet" href="../styles/p2p-premium.css">',
                    f'<style>\n{_css_content}\n</style>'
                )
            components.html(_html_content, height=900, scrolling=True)
        else:
            st.error("p2p_html.html bulunamadı.")


def main() -> None:
    render_app()


if __name__ == "__main__":
    main()