import pandas as pd
import logging
from typing import List, Dict

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def save_to_csv(data: List[Dict], filename: str) -> None:
    """
    Menyimpan list of dictionaries ke dalam file CSV.
    """
    try:
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)
        logger.info(f"Berhasil menyimpan {len(data)} baris data ke {filename}")
    except Exception as e:
        logger.error(f"Gagal menyimpan ke CSV {filename}: {e}")

def view_csv(filename: str, num_rows: int = 5) -> None:
    """
    Membaca dan menampilkan beberapa baris pertama dari file CSV ke terminal
    dengan format yang rapi dan mudah dibaca (wrap text panjang).
    """
    try:
        df = pd.read_csv(filename)
        logger.info(f"--- Menampilkan {min(num_rows, len(df))} baris dari {filename} ---")

        # Set opsi pandas agar tidak memotong teks terlalu pendek
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_colwidth', 80)
        pd.set_option('display.width', 1000)

        print("\n" + df.head(num_rows).to_string())
        print("-" * 50 + "\n")

        # Reset opsi setelah digunakan (opsional)
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
        {"id": 1, "text": "Ini adalah contoh teks panjang yang akan ditampilkan di terminal agar kita tahu kalau semuanya berfungsi dengan baik.", "label": "positif"},
        {"id": 2, "text": "Teks lain untuk tes.", "label": "netral"}
    ]
    save_to_csv(dummy_data, "test_view.csv")
    view_csv("test_view.csv")
