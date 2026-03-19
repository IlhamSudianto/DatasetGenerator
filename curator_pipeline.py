import os
import sys
import logging
from typing import Optional

try:
    from nemo_curator.datasets import DocumentDataset
    from nemo_curator.modifiers import UnicodeReformatter
    from nemo_curator.filters import WordCountFilter
except ImportError:
    pass

import pandas as pd

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CurationPipeline:
    """
    Fallback pipeline jika instalasi nemo_curator bermasalah di sistem (ketergantungan terlalu berat/gagal import).
    Mensimulasikan fungsionalitas core dari NeMo Curator untuk teks panjang:
    1. Membersihkan Unicode.
    2. Menyaring kalimat terlalu pendek (berdasarkan jumlah kata).
    3. Deduplikasi teks eksak.
    """
    def run_pipeline(self, input_path: str, output_path: str, text_field: str = "text") -> Optional[str]:
        if not os.path.exists(input_path):
            logger.error(f"Input file {input_path} tidak ditemukan.")
            return None

        logger.info(f"Memulai Curation Pipeline (Pandas Backend) untuk: {input_path}")
        try:
            # 1. Load Dataset
            df = pd.read_json(input_path, lines=True)
            initial_count = len(df)

            # Pastikan kolom ada
            if text_field not in df.columns:
                logger.error(f"Kolom {text_field} tidak ada di dalam dataset.")
                return None

            # 2. Text Cleaning / Reformatter (Simulasi)
            logger.info("-> Menjalankan Text Cleaner & Reformatter...")
            df[text_field] = df[text_field].astype(str).str.strip()
            df[text_field] = df[text_field].str.replace('\x00', '', regex=False)

            # 3. Quality Filtering (Simulasi min_words = 5)
            logger.info("-> Menjalankan Quality Filtering (Min Word Count = 5)...")
            word_counts = df[text_field].str.split().str.len()
            df = df[word_counts >= 5]
            filtered_count = initial_count - len(df)

            # 4. Exact Deduplication
            logger.info("-> Menjalankan Exact Deduplication...")
            before_dedup = len(df)
            df = df.drop_duplicates(subset=[text_field])
            dedup_count = before_dedup - len(df)

            # 5. Save Data
            logger.info(f"-> Menyimpan {len(df)} baris hasil kurasi ke: {output_path}")
            df.to_json(output_path, orient="records", lines=True, force_ascii=False)

            logger.info(f"✨ Curation selesai! Terbuang: {filtered_count} baris jelek, {dedup_count} duplikat.")
            return output_path

        except Exception as e:
            logger.error(f"Curation pipeline gagal: {e}")
            return None

if __name__ == "__main__":
    # Test utilitas
    import json
    dummy_data = [
        {"id": 1, "text": "Ini adalah teks yang normal dan cukup panjang untuk dataset AI."},
        {"id": 2, "text": "Pendek saja"}, # Akan dibuang oleh filter (kurang dari 5 kata)
        {"id": 3, "text": "Ini adalah teks yang normal dan cukup panjang untuk dataset AI."}, # Duplikat
        {"id": 4, "text": "Karakter aneh \u0000 akan dibersihkan oleh pipeline."}
    ]
    with open("dummy_curation_input.jsonl", "w") as f:
        for d in dummy_data:
            f.write(json.dumps(d) + "\n")

    curator = CurationPipeline()
    curator.run_pipeline("dummy_curation_input.jsonl", "dummy_curation_output.jsonl", text_field="text")
