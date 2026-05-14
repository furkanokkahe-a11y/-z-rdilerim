import tempfile
import unittest
import zipfile
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import gib_fatura_api as api


class GibFaturaApiTests(unittest.TestCase):
    def test_ensure_panel_auth_config_and_verify_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "panel_auth.json"

            config = api.ensure_panel_auth_config(config_path)

            self.assertTrue(config_path.exists())
            self.assertEqual(config["username"], "admin")
            self.assertTrue(api.verify_panel_credentials("admin", config["password"], config_path))
            self.assertFalse(api.verify_panel_credentials("admin", "yanlis", config_path))

    def test_build_expense_audit_flags_missing_and_invalid_records(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            missing_file_path = Path(temp_dir) / "olmayan.pdf"
            df = pd.DataFrame(
                [
                    {
                        "id": 1,
                        "tarih": "2026-01-15",
                        "aciklama": "",
                        "kategori": "Bilinmeyen",
                        "kdv_orani": 8,
                        "fatura_dosya_yolu": str(missing_file_path),
                    },
                    {
                        "id": 2,
                        "tarih": "2026-01-16",
                        "aciklama": "Market alışverişi",
                        "kategori": "Market",
                        "kdv_orani": 20,
                        "fatura_dosya_yolu": "",
                    },
                ]
            )

            audit = api.build_expense_audit(df)

            self.assertEqual(audit["summary"]["kayit_adedi"], 2)
            self.assertEqual(audit["summary"]["sorunlu_kayit"], 2)
            self.assertEqual(audit["summary"]["diskte_olmayan"], 1)
            self.assertEqual(audit["summary"]["dosyasi_olmayan"], 1)
            self.assertEqual(audit["summary"]["gecersiz_kategori"], 1)
            self.assertEqual(audit["summary"]["gecersiz_kdv"], 1)
            self.assertEqual(audit["summary"]["eksik_aciklama"], 1)
            self.assertEqual(len(audit["records"]), 2)

    def test_create_backup_archive_includes_expected_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            db_path = root / "gib_fatura.db"
            archive_dir = root / "arsivler"
            expense_dir = root / "giderler" / "faturalar"
            backup_dir = root / "yedekler"
            auth_path = root / "panel_auth.json"

            db_path.write_text("db", encoding="utf-8")
            archive_dir.mkdir(parents=True, exist_ok=True)
            expense_dir.mkdir(parents=True, exist_ok=True)
            (archive_dir / "hafta.xlsx").write_text("arsiv", encoding="utf-8")
            (expense_dir / "fatura.pdf").write_text("fatura", encoding="utf-8")
            auth_path.write_text('{"username":"admin","password":"123"}', encoding="utf-8")

            backup_path = api.create_backup_archive(
                db_path=db_path,
                archive_dir=archive_dir,
                expense_invoice_dir=expense_dir,
                backup_dir=backup_dir,
                auth_config_path=auth_path,
                prefix="test_yedek",
                reference_time=datetime(2026, 1, 31, 10, 15, 0),
            )

            self.assertTrue(backup_path.exists())
            with zipfile.ZipFile(backup_path) as archive:
                names = set(archive.namelist())

            self.assertIn("veritabani/gib_fatura.db", names)
            self.assertIn("arsivler/hafta.xlsx", names)
            self.assertIn("giderler/faturalar/fatura.pdf", names)
            self.assertIn("guvenlik/panel_auth.json", names)
            self.assertIn("manifest.json", names)

    def test_create_manual_transaction_only_saves_to_panel(self) -> None:
        payload = api.ManualTransactionIn(
            musteri_adi="Test Musteri",
            musteri_tc="12345678901",
            islem_tarihi="2026-01-15",
            vergisiz_bedel=100.0,
            kdv=20.0,
            toplam_fatura=120.0,
            gib_kullanici="kullanici",
            gib_sifre="sifre",
        )

        with patch("gib_fatura_api.try_create_gib_draft", side_effect=AssertionError("manual endpoint should not call GIB")):
            with patch("gib_fatura_api.save_transaction", return_value=42) as save_mock:
                result = api.create_manual_transaction(payload)

        self.assertEqual(result["id"], 42)
        self.assertEqual(result["gib_durumu"], "Kaydedildi")
        self.assertEqual(result["Durum Mesajı"], "Manuel fatura panele kaydedildi. GIB'e gonderilmedi.")
        self.assertIsNone(result["GİB ETTN"])
        save_mock.assert_called_once()

    def test_analyze_gib_date_discrepancies_reports_both_sides(self) -> None:
        transactions_df = pd.DataFrame(
            [
                {
                    "id": 1,
                    "İşlem Tarihi": "2026-01-15",
                    "Müşteri Adı": "Ali Kaya",
                    "T.C. Kimlik No": "11111111111",
                    "Toplam Fatura": 120.0,
                    "GİB ETTN": "",
                },
                {
                    "id": 2,
                    "İşlem Tarihi": "2026-01-16",
                    "Müşteri Adı": "Veli Demir",
                    "T.C. Kimlik No": "22222222222",
                    "Toplam Fatura": 240.0,
                    "GİB ETTN": "",
                },
            ]
        )
        gib_drafts_df = pd.DataFrame(
            [
                {
                    "ettn": "",
                    "belge_numarasi": "A-1",
                    "musteri_tc": "11111111111",
                    "musteri_adi": "Ali Kaya",
                    "islem_tarihi": "2026-01-15",
                    "onay_durumu": "Onaylanmadı",
                },
                {
                    "ettn": "",
                    "belge_numarasi": "A-2",
                    "musteri_tc": "33333333333",
                    "musteri_adi": "Ayse Yilmaz",
                    "islem_tarihi": "2026-01-17",
                    "onay_durumu": "Onaylanmadı",
                },
            ]
        )

        result = api.analyze_gib_date_discrepancies(transactions_df, gib_drafts_df)

        self.assertEqual(result["matched_records"], 1)
        self.assertEqual(result["panel_only_count"], 1)
        self.assertEqual(result["gib_only_count"], 1)
        self.assertEqual(result["panel_only_dates"][0]["tarih"], "2026-01-16")
        self.assertEqual(result["gib_only_dates"][0]["tarih"], "2026-01-17")
        self.assertEqual(result["panel_only_dates"][0]["kayit_adedi"], 1)
        self.assertEqual(result["gib_only_dates"][0]["kayit_adedi"], 1)


if __name__ == "__main__":
    unittest.main()