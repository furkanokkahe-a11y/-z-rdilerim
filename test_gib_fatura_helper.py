import tempfile
import unittest
from pathlib import Path

import pandas as pd

import gib_fatura_helper as app


class GibFaturaHelperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_gib_fatura.db"
        self.archive_dir = Path(self.temp_dir.name) / "arsivler"
        app.ensure_database(self.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_calculate_invoice(self) -> None:
        result = app.calculate_invoice(100, 44.3, 48.0)
        self.assertEqual(result["vergisiz_bedel"], 308.33)
        self.assertEqual(result["kdv"], 61.67)
        self.assertEqual(result["toplam_fatura"], 370.0)

    def test_validate_transaction_input(self) -> None:
        errors, _ = app.validate_transaction_input("", "123", app.date.today(), 0, 10, 9)
        self.assertGreaterEqual(len(errors), 4)

    def test_validate_manual_invoice_input(self) -> None:
        errors, _ = app.validate_manual_invoice_input("Ali", "11111111111", app.date.today(), 100.0, 20.0, 119.0)
        self.assertEqual(len(errors), 1)
        self.assertIn("Beklenen", errors[0])

    def test_build_export_dataframe_adds_total_row(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "İşlem Tarihi": "2026-03-31",
                    "Müşteri Adı": "Ahmet",
                    "T.C. Kimlik No": "11111111111",
                    "Vergisiz Bedel": 100.0,
                    "KDV": 20.0,
                    "Toplam Fatura": 120.0,
                    "GİB Durumu": "Taslak Oluşturuldu",
                }
            ]
        )
        exported = app.build_export_dataframe(df)
        self.assertEqual(exported.iloc[-1]["Müşteri Adı"], "TOPLAM")
        self.assertEqual(exported.iloc[-1]["Toplam Fatura"], 120.0)

    def test_normalize_legacy_dataframe_skips_total_row(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "İşlem Tarihi": "2026-03-31",
                    "Müşteri Adı": "Ahmet",
                    "Vergisiz Bedel": 100.0,
                    "KDV": 20.0,
                    "Toplam Fatura": 120.0,
                },
                {
                    "İşlem Tarihi": "",
                    "Müşteri Adı": "TOPLAM",
                    "Vergisiz Bedel": 100.0,
                    "KDV": 20.0,
                    "Toplam Fatura": 120.0,
                },
            ]
        )
        normalized = app.normalize_legacy_dataframe(df)
        self.assertEqual(len(normalized), 1)
        self.assertEqual(normalized.iloc[0]["Müşteri Adı"], "Ahmet")

    def test_archive_active_transactions(self) -> None:
        record = {
            "İşlem Tarihi": "2026-03-31",
            "Müşteri Adı": "Ayşe",
            "T.C. Kimlik No": "11111111111",
            "Satılan USDT": 100.0,
            "Alış Kuru": 44.3,
            "Satış Kuru": 48.0,
            "Vergisiz Bedel": 370.0,
            "KDV": 74.0,
            "Toplam Fatura": 444.0,
            "GİB Durumu": "Kaydedildi",
            "Durum Mesajı": "Test kaydı",
        }
        app.save_transaction(record, self.db_path)

        success, _ = app.archive_active_transactions(self.db_path, self.archive_dir)

        active_df = app.load_transactions(self.db_path, archived=False)
        archived_df = app.load_transactions(self.db_path, archived=True)

        self.assertTrue(success)
        self.assertTrue(active_df.empty)
        self.assertEqual(len(archived_df), 1)
        self.assertEqual(len(list(self.archive_dir.glob("*.xlsx"))), 1)

    def test_save_transaction_directly_to_archive(self) -> None:
        record = {
            "İşlem Tarihi": "2026-03-31",
            "Müşteri Adı": "Mehmet",
            "T.C. Kimlik No": "11111111111",
            "Satılan USDT": 0.0,
            "Alış Kuru": 0.0,
            "Satış Kuru": 0.0,
            "Vergisiz Bedel": 500.0,
            "KDV": 100.0,
            "Toplam Fatura": 600.0,
            "GİB Durumu": "Kaydedildi",
            "Durum Mesajı": "Fatura doğrudan arşive kaydedildi.",
            "Kaynak": "manuel_arsiv",
            "Arşiv Hafta Kodu": "manuel_20260331_mart_arsivi",
            "Arşiv Etiketi": "Mart Arsivi",
        }
        app.save_transaction(record, self.db_path)

        active_df = app.load_transactions(self.db_path, archived=False)
        archived_df = app.load_transactions(self.db_path, archived=True)

        self.assertTrue(active_df.empty)
        self.assertEqual(len(archived_df), 1)
        self.assertEqual(archived_df.iloc[0]["Arşiv Etiketi"], "Mart Arsivi")

    def test_update_gib_status_bulk(self) -> None:
        first = {
            "İşlem Tarihi": "2026-03-31",
            "Müşteri Adı": "Ali",
            "T.C. Kimlik No": "11111111111",
            "Satılan USDT": 0.0,
            "Alış Kuru": 0.0,
            "Satış Kuru": 0.0,
            "Vergisiz Bedel": 100.0,
            "KDV": 20.0,
            "Toplam Fatura": 120.0,
            "GİB Durumu": "Taslak Oluşturuldu",
            "Durum Mesajı": "Taslak oluşturuldu.",
        }
        second = dict(first)
        second["Müşteri Adı"] = "Veli"

        first_id = app.save_transaction(first, self.db_path)
        second_id = app.save_transaction(second, self.db_path)

        app.update_gib_status_bulk(
            [first_id, second_id],
            "İmzalandı",
            "Kayıt, kullanıcı tarafından GİB portalında imzalandı olarak işaretlendi.",
            self.db_path,
        )

        active_df = app.load_transactions(self.db_path, archived=False)
        self.assertTrue((active_df["GİB Durumu"] == "İmzalandı").all())

    def test_filter_transactions_for_statistics_excludes_error_statuses(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "İşlem Tarihi": "2026-03-31",
                    "Müşteri Adı": "Ali",
                    "Toplam Fatura": 120.0,
                    "KDV": 20.0,
                    "GİB Durumu": "Kaydedildi",
                },
                {
                    "İşlem Tarihi": "2026-03-31",
                    "Müşteri Adı": "Veli",
                    "Toplam Fatura": 240.0,
                    "KDV": 40.0,
                    "GİB Durumu": "Uyumluluk Hatası",
                },
            ]
        )

        filtered = app.filter_transactions_for_statistics(df)

        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered.iloc[0]["Müşteri Adı"], "Ali")

    def test_get_dashboard_metrics_ignores_error_statuses(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "İşlem Tarihi": pd.Timestamp(app.date.today()),
                    "Müşteri Adı": "Ali",
                    "Toplam Fatura": 120.0,
                    "KDV": 20.0,
                    "GİB Durumu": "Kaydedildi",
                },
                {
                    "İşlem Tarihi": pd.Timestamp(app.date.today()),
                    "Müşteri Adı": "Veli",
                    "Toplam Fatura": 999.0,
                    "KDV": 166.5,
                    "GİB Durumu": "Uyumluluk Hatası",
                },
            ]
        )

        metrics = app.get_dashboard_metrics(df)

        self.assertEqual(metrics["bugun"], 120.0)
        self.assertEqual(metrics["hafta"], 120.0)
        self.assertEqual(metrics["ay"], 120.0)
        self.assertEqual(metrics["toplam_kdv"], 20.0)
        self.assertEqual(metrics["en_aktif_musteri"], "Ali")

    def test_reconcile_automatic_invoice_totals_corrects_old_formula(self) -> None:
        old_formula_record = {
            "İşlem Tarihi": "2026-03-31",
            "Müşteri Adı": "Kazim",
            "T.C. Kimlik No": "11111111111",
            "Satılan USDT": 100.0,
            "Alış Kuru": 44.3,
            "Satış Kuru": 48.0,
            "Vergisiz Bedel": 370.0,
            "KDV": 74.0,
            "Toplam Fatura": 444.0,
            "GİB Durumu": "Taslak Oluşturuldu",
            "Durum Mesajı": "Eski formulle kaydedildi.",
            "Kaynak": "otomatik",
        }
        record_id = app.save_transaction(old_formula_record, self.db_path)

        updated_count = app.reconcile_automatic_invoice_totals(self.db_path)
        corrected_df = app.load_transactions_by_ids([record_id], self.db_path)

        self.assertEqual(updated_count, 1)
        self.assertEqual(float(corrected_df.iloc[0]["Vergisiz Bedel"]), 308.33)
        self.assertEqual(float(corrected_df.iloc[0]["KDV"]), 61.67)
        self.assertEqual(float(corrected_df.iloc[0]["Toplam Fatura"]), 370.0)

    def test_match_gib_drafts_to_transactions_uses_ettn(self) -> None:
        transactions_df = pd.DataFrame(
            [
                {
                    "id": 1,
                    "İşlem Tarihi": "2026-03-31",
                    "Müşteri Adı": "Ali Veli",
                    "T.C. Kimlik No": "11111111111",
                    "GİB ETTN": "ettn-123",
                }
            ]
        )
        gib_drafts_df = pd.DataFrame(
            [
                {
                    "ettn": "ettn-123",
                    "belge_numarasi": "GIB20260001",
                    "musteri_tc": "11111111111",
                    "musteri_adi": "Ali Veli",
                    "islem_tarihi": "2026-03-31",
                    "onay_durumu": "Onaylandı",
                }
            ]
        )

        matches = app.match_gib_drafts_to_transactions(transactions_df, gib_drafts_df)

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0][0], 1)
        self.assertEqual(matches[0][1]["ettn"], "ettn-123")

    def test_match_gib_drafts_to_transactions_uses_unique_identity_fallback(self) -> None:
        transactions_df = pd.DataFrame(
            [
                {
                    "id": 7,
                    "İşlem Tarihi": "2026-04-02",
                    "Müşteri Adı": "Ayşe Demir",
                    "T.C. Kimlik No": "11111111111",
                    "GİB ETTN": None,
                }
            ]
        )
        gib_drafts_df = pd.DataFrame(
            [
                {
                    "ettn": "",
                    "belge_numarasi": "",
                    "musteri_tc": "11111111111",
                    "musteri_adi": "Ayşe Demir",
                    "islem_tarihi": "2026-04-02",
                    "onay_durumu": "Onaylanmadı",
                }
            ]
        )

        matches = app.match_gib_drafts_to_transactions(transactions_df, gib_drafts_df)

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0][0], 7)
        self.assertEqual(matches[0][1]["onay_durumu"], "Onaylanmadı")

    def test_get_finance_summary_excludes_error_statuses(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "id": 1,
                    "İşlem Tarihi": "2026-04-01",
                    "Müşteri Adı": "Ali",
                    "Vergisiz Bedel": 100.0,
                    "KDV": 20.0,
                    "Toplam Fatura": 120.0,
                    "GİB Durumu": "İmzalandı",
                    "Arşiv Hafta Kodu": None,
                },
                {
                    "id": 2,
                    "İşlem Tarihi": "2026-04-02",
                    "Müşteri Adı": "Veli",
                    "Vergisiz Bedel": 200.0,
                    "KDV": 40.0,
                    "Toplam Fatura": 240.0,
                    "GİB Durumu": "Taslak Oluşturuldu",
                    "Arşiv Hafta Kodu": "2026_14",
                },
                {
                    "id": 3,
                    "İşlem Tarihi": "2026-04-03",
                    "Müşteri Adı": "Hata",
                    "Vergisiz Bedel": 500.0,
                    "KDV": 100.0,
                    "Toplam Fatura": 600.0,
                    "GİB Durumu": "Uyumluluk Hatası",
                    "Arşiv Hafta Kodu": None,
                },
            ]
        )

        summary = app.get_finance_summary(df)

        self.assertEqual(summary["toplam_kayit"], 2)
        self.assertEqual(summary["toplam_fatura"], 360.0)
        self.assertEqual(summary["aktif_toplam_fatura"], 120.0)
        self.assertEqual(summary["arsiv_toplam_fatura"], 240.0)
        self.assertEqual(summary["imzali_kayit"], 1)
        self.assertEqual(summary["taslak_kayit"], 1)

    def test_calculate_expense_breakdown_uses_full_rate(self) -> None:
        result = app.calculate_expense_breakdown(1200.0, 20.0, "Araç", False)

        self.assertEqual(result["kdv_tutari"], 200.0)
        self.assertEqual(result["net_gider"], 1000.0)
        self.assertEqual(result["vergi_matrahi"], 1000.0)
        self.assertEqual(result["indirilecek_kdv"], 200.0)
        self.assertEqual(result["vergi_kalkani"], 200.0)

    def test_calculate_expense_breakdown_honors_commercial_vehicle_override(self) -> None:
        result = app.calculate_expense_breakdown(1200.0, 20.0, "Araç", True)

        self.assertEqual(result["net_gider"], 1000.0)
        self.assertEqual(result["vergi_matrahi"], 1000.0)
        self.assertEqual(result["vergi_kalkani"], 200.0)

    def test_save_expense_and_attach_invoice(self) -> None:
        expense_record = {
            "İşlem Tarihi": "2026-04-15",
            "Açıklama": "Araç Kiralama",
            "Kategori": "Araç",
            "Toplam Tutar": 1200.0,
            "KDV Oranı": 20.0,
            "KDV Tutarı": 200.0,
            "Net Gider": 1000.0,
            "Gider Yazım Oranı": 1.0,
            "Vergi Matrahı": 1000.0,
            "İndirilecek KDV": 200.0,
            "Vergi Kalkanı": 200.0,
            "Ticari Araç": False,
            "Fatura Dosya Yolu": None,
            "Fatura Orijinal Adı": "arac-kiralama.pdf",
        }

        expense_id = app.save_expense(expense_record, self.db_path)
        invoice_dir = Path(self.temp_dir.name) / "giderler" / "faturalar"
        invoice_path = app.attach_expense_invoice(
            expense_id,
            "arac-kiralama.pdf",
            b"%PDF-1.4 test invoice",
            islem_tarihi=app.date(2026, 4, 15),
            aciklama="Araç Kiralama",
            db_path=self.db_path,
            invoice_dir=invoice_dir,
        )

        expenses_df = app.load_expenses(self.db_path)

        self.assertEqual(len(expenses_df), 1)
        self.assertTrue(invoice_path.exists())
        self.assertEqual(expenses_df.iloc[0]["Fatura Orijinal Adı"], "arac-kiralama.pdf")
        self.assertEqual(Path(expenses_df.iloc[0]["Fatura Dosya Yolu"]), invoice_path)


if __name__ == "__main__":
    unittest.main()