import os
import sys
import logging
from generator import DatasetGenerator
from dataset_viewer import save_to_csv, view_csv

# Setup logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Mengecek Kunci API...")
    if not os.getenv("NVIDIA_API_KEY"):
        logger.error("NVIDIA_API_KEY tidak ditemukan!")
        logger.error("Silakan set environment variable NVIDIA_API_KEY atau tambahkan ke file .env (misal: export NVIDIA_API_KEY='nvapi-...')")
        sys.exit(1)

    try:
        generator = DatasetGenerator()
    except Exception as e:
        logger.error(f"Gagal menginisialisasi Generator: {e}")
        sys.exit(1)

    print("\n" + "="*60)
    print("DEMO 1: PEMBUATAN DATASET SINTETIS (GENERATE FROM SCRATCH)")
    print("="*60)

    # Meminta generator membuat dataset teks panjang baru tentang teknologi (termasuk dummy PII yang akan disensor)
    topic = "Pengalaman pelanggan saat berbelanja online, sebutkan juga informasi dummy seperti email, NIK, dan nomor HP secara alami dalam cerita"
    logger.info(f"Topik: '{topic}'")

    generated_data = generator.generate_from_scratch(topic=topic, count=3)

    if generated_data:
        csv_file_1 = "generated_dataset.csv"
        save_to_csv(generated_data, csv_file_1)
        print("\nHasil Data (yang telah melewati sensor privasi):")
        view_csv(csv_file_1, num_rows=3)
    else:
        logger.warning("Gagal menghasilkan dataset dari awal.")

    print("\n" + "="*60)
    print("DEMO 2: AUGMENTASI DATA (MENAMBAHKAN KOLOM 'REASONING')")
    print("="*60)

    # Dummy dataset (Mirip dengan dataset instruksi Alpaca)
    # Terdapat dummy PII untuk membuktikan PII disensor juga dari data asli
    alpaca_dummy_data = [
        {
            "instruction": "Evaluasi kalimat berikut dan tentukan sentimennya (positif, negatif, atau netral).",
            "input": "Layanan dari bank ini sangat mengecewakan. Email CS-nya cs@bank.com dan NIK CS-nya 3171234567890123 tidak membantu.",
            "output": "Sentimen negatif."
        },
        {
            "instruction": "Terjemahkan kalimat ini ke bahasa Inggris.",
            "input": "Nama saya Budi, hubungi saya di 0812-3456-7890.",
            "output": "My name is Budi, contact me at 0812-3456-7890."
        }
    ]

    logger.info("Dataset asli (sebelum augmentasi):")
    for d in alpaca_dummy_data:
        print(f"Instruction: {d['instruction']}")
        print(f"Input: {d['input']}")
        print(f"Output: {d['output']}\n")

    augmented_data = generator.augment_dataset(
        data=alpaca_dummy_data,
        instruction_column="instruction",
        input_column="input",
        output_column="output"
    )

    if augmented_data:
        csv_file_2 = "augmented_dataset.csv"
        save_to_csv(augmented_data, csv_file_2)
        print("\nHasil Data Augmentasi (dengan tambahan 'reasoning' dan tersensor):")
        view_csv(csv_file_2, num_rows=2)
    else:
        logger.warning("Gagal mengaugmentasi dataset.")

    print("\n" + "="*60)
    print("DEMO SELESAI")
    print("="*60)


if __name__ == "__main__":
    main()
