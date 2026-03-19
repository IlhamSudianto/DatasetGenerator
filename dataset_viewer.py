import pandas as pd
import logging
import json
from typing import List, Dict

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatasetStorage:
    @staticmethod
    def save(data: List[Dict], filename_prefix: str, formats: List[str] = ['csv', 'jsonl', 'parquet']) -> None:
        """
        Menyimpan dataset ke berbagai format Big Data secara dinamis.
        Format yang didukung: csv, jsonl (JSON Lines), parquet.
        """
        if not data:
            logger.warning("Data kosong. Tidak ada yang disimpan.")
            return

        df = pd.DataFrame(data)
        logger.info(f"Mulai menyimpan {len(data)} baris data...")

        for fmt in formats:
            try:
                fmt = fmt.lower()
                if fmt == 'csv':
                    fname = f"{filename_prefix}.csv"
                    df.to_csv(fname, index=False)
                    logger.info(f" -> Disimpan ke: {fname}")
                elif fmt == 'jsonl':
                    fname = f"{filename_prefix}.jsonl"
                    with open(fname, 'w', encoding='utf-8') as f:
                        for row in data:
                            f.write(json.dumps(row, ensure_ascii=False) + '\n')
                    logger.info(f" -> Disimpan ke: {fname}")
                elif fmt == 'parquet':
                    fname = f"{filename_prefix}.parquet"
                    # Menggunakan engine pyarrow
                    df.to_parquet(fname, engine='pyarrow', index=False)
                    logger.info(f" -> Disimpan ke: {fname}")
                else:
                    logger.warning(f"Format {fmt} tidak dikenali.")
            except Exception as e:
                logger.error(f"Gagal menyimpan format {fmt}: {e}")

    @staticmethod
    def view_terminal(filename: str, num_rows: int = 5) -> None:
        """
        Membaca dan menampilkan isi dataset di terminal.
        Otomatis mendeteksi CSV, JSONL, atau Parquet berdasarkan ekstensi.
        """
        try:
            if filename.endswith('.csv'):
                df = pd.read_csv(filename)
            elif filename.endswith('.jsonl'):
                # Baca jsonl dengan pandas
                df = pd.read_json(filename, lines=True)
            elif filename.endswith('.parquet'):
                df = pd.read_parquet(filename, engine='pyarrow')
            else:
                logger.error(f"Ekstensi file {filename} tidak didukung.")
                return

            logger.info(f"--- Menampilkan isi {filename} ---")

            # Set opsi pandas agar tidak memotong teks
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_colwidth', 80)
            pd.set_option('display.width', 1000)

            print("\n" + df.head(num_rows).to_string())
            print("-" * 50 + "\n")

            pd.reset_option('display.max_columns')
            pd.reset_option('display.max_colwidth')
            pd.reset_option('display.width')

        except FileNotFoundError:
            logger.error(f"File {filename} tidak ditemukan.")
        except Exception as e:
            logger.error(f"Terjadi kesalahan saat membaca {filename}: {e}")

if __name__ == "__main__":
    # Test utilitas
    dummy_data = [
        {"id": 1, "text": "Ini adalah baris pertama untuk tes Parquet dan JSONL", "label": "positif"},
        {"id": 2, "text": "Baris kedua dengan teks yang sedikit lebih panjang untuk pengujian Big Data format", "label": "netral"}
    ]
    DatasetStorage.save(dummy_data, "test_storage", formats=['csv', 'jsonl', 'parquet'])
    DatasetStorage.view_terminal("test_storage.parquet")
