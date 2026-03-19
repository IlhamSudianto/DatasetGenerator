import os
import sys
import logging
import asyncio
from async_generator import AsyncDatasetGenerator
from dataset_viewer import DatasetStorage

# Setup logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

async def main():
    logger.info("Mengecek Kunci API...")
    if not os.getenv("NVIDIA_API_KEY"):
        logger.error("NVIDIA_API_KEY tidak ditemukan!")
        logger.error("Silakan set environment variable NVIDIA_API_KEY atau tambahkan ke file .env (misal: export NVIDIA_API_KEY='nvapi-...')")
        sys.exit(1)

    try:
        generator = AsyncDatasetGenerator()
    except Exception as e:
        logger.error(f"Gagal menginisialisasi Async Generator: {e}")
        sys.exit(1)

    print("\n" + "="*80)
    print("🚀 ENTERPRISE DEMO 1: PEMBUATAN DATASET (ACTOR-CRITIC + PYDANTIC VALIDATION)")
    print("="*80)

    topic = "Pengalaman belanja online yang kacau balau, pastikan Anda memasukkan nomor NIK, email, dan HP di dalam ceritanya agar kita bisa menguji sistem pseudonimisasi canggih Faker"
    logger.info(f"Topik: '{topic}'")
    logger.info("Menjalankan pipeline Actor-Critic secara Asynchronous...")

    # Generate 3 row text
    generated_data = await generator.generate_from_scratch(topic=topic, count=3)

    if generated_data:
        prefix = "advanced_generated"
        DatasetStorage.save(generated_data, filename_prefix=prefix, formats=['csv', 'jsonl', 'parquet'])
        print("\n✨ Hasil Data (dengan Pseudonimisasi Dinamis menggunakan Faker):")
        DatasetStorage.view_terminal(f"{prefix}.parquet", num_rows=3)
    else:
        logger.warning("Gagal menghasilkan dataset.")

    print("\n" + "="*80)
    print("⚡ ENTERPRISE DEMO 2: AUGMENTASI DATASET KONKUREN (PARALEL/ASYNCHRONOUS)")
    print("="*80)

    # Mock data with dummy PII that will be pseudonymized dynamically
    alpaca_dummy_data = [
        {
            "instruction": "Tentukan sentimen kalimat (positif, negatif, atau netral).",
            "input": "Saya sangat marah karena kurir dari paket@kiriman.com menghilangkan paket saya. NIK saya 3171234567890123 tidak sesuai data di sistem mereka.",
            "output": "Sentimen negatif."
        },
        {
            "instruction": "Tentukan jenis kelamin berdasarkan nama.",
            "input": "Nama saya Budi Santoso, nomor HP saya 0812-3456-7890.",
            "output": "Laki-laki."
        },
        {
            "instruction": "Apakah nomor telepon berikut ini termasuk nomor Indonesia?",
            "input": "Apakah nomor +628111222333 ini terdaftar di database?",
            "output": "Ya, itu adalah nomor Indonesia."
        }
    ]

    logger.info("Dataset asli (sebelum augmentasi):")
    for i, d in enumerate(alpaca_dummy_data):
        print(f"[{i+1}] Instruction: {d['instruction']}\n    Input: {d['input']}\n    Output: {d['output']}")

    logger.info("\nMemulai proses Augmentasi Konkuren (seluruh baris diproses secara paralel)...")

    augmented_data = await generator.augment_dataset_concurrently(
        data=alpaca_dummy_data,
        instruction_column="instruction",
        input_column="input",
        output_column="output"
    )

    if augmented_data:
        prefix = "advanced_augmented"
        DatasetStorage.save(augmented_data, filename_prefix=prefix, formats=['csv', 'jsonl', 'parquet'])
        print("\n✨ Hasil Data Augmentasi (Paralel, ditambah Reasoning, dan dipseudonimisasi):")
        DatasetStorage.view_terminal(f"{prefix}.parquet", num_rows=3)
    else:
        logger.warning("Gagal mengaugmentasi dataset.")

    print("\n" + "="*80)
    print("✅ DEMO ENTERPRISE SELESAI")
    print("="*80)


if __name__ == "__main__":
    asyncio.run(main())
