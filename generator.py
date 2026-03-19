import os
import json
import logging
from typing import List, Dict, Any, Optional
from openai import OpenAI
from dotenv import load_dotenv
from privacy_utils import mask_pii

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class DatasetGenerator:
    def __init__(self, api_key: Optional[str] = None):
        """
        Inisialisasi generator dataset.
        Menggunakan OpenAI client yang diarahkan ke endpoint NVIDIA.
        """
        self.api_key = api_key or os.getenv("NVIDIA_API_KEY")
        if not self.api_key:
            raise ValueError("NVIDIA_API_KEY tidak ditemukan. Harap setel di environment atau file .env")

        # Endpoint NVIDIA NIM API menggunakan OpenAI SDK format
        self.client = OpenAI(
            base_url="https://integrate.api.nvidia.com/v1",
            api_key=self.api_key
        )

        # Menggunakan model Llama-3.1 70B dari NVIDIA NIM (bisa diganti sesuai kebutuhan)
        self.model = "meta/llama-3.1-70b-instruct"

    def generate_from_scratch(self, topic: str, count: int = 1) -> List[Dict[str, str]]:
        """
        Menghasilkan dataset teks panjang dari awal berdasarkan topik.
        """
        logger.info(f"Mulai generate {count} baris data tentang: '{topic}'")
        dataset = []

        prompt = f"""Buatlah {count} paragraf panjang yang sangat detail mengenai '{topic}'.
Setiap paragraf harus menceritakan aspek atau contoh yang berbeda.
Berikan hasil secara eksklusif dalam format JSON List of Objects (Array).
Format yang diinginkan (hanya ini tanpa markdown text lainnya):
[
  {{"id": 1, "topic": "...", "text": "..."}},
  ...
]"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Anda adalah AI pembuat dataset yang hanya membalas dengan format JSON valid."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=2048
            )

            content = response.choices[0].message.content
            # Bersihkan markdown if model returned it despite instructions
            content = content.strip()
            if content.startswith("```json"):
                content = content[7:]
            if content.endswith("```"):
                content = content[:-3]

            raw_data = json.loads(content.strip())

            # Mask PII pada data sebelum dikembalikan
            for item in raw_data:
                for key, value in item.items():
                    if isinstance(value, str):
                        item[key] = mask_pii(value)
                dataset.append(item)

            logger.info(f"Berhasil meng-generate {len(dataset)} baris data murni.")
            return dataset

        except Exception as e:
            logger.error(f"Gagal saat generate data: {e}")
            return []

    def augment_dataset(self, data: List[Dict[str, str]], instruction_column: str, input_column: str, output_column: str) -> List[Dict[str, str]]:
        """
        Menambahkan kolom 'reasoning' (penalaran) ke dataset instruksi yang ada (misal: Alpaca).
        """
        logger.info(f"Memulai data augmentation untuk {len(data)} baris.")
        augmented_data = []

        for idx, row in enumerate(data):
            instruction = row.get(instruction_column, "")
            input_text = row.get(input_column, "")
            output_text = row.get(output_column, "")

            prompt = f"""Diberikan sebuah task AI:
Instruksi: {instruction}
Input Tambahan: {input_text}
Output AI: {output_text}

Tugas Anda: Berikan 'reasoning' (penalaran) langkah demi langkah, detail, dan logis yang menjelaskan mengapa AI menghasilkan output tersebut berdasarkan instruksi.
Jawablah HANYA dengan teks penalarannya saja, tanpa basa-basi. Panjang penalaran harus cukup komprehensif."""

            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "Anda adalah sistem analitik yang ahli dalam menjelaskan proses berpikir (reasoning) AI secara logis."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.4,
                    max_tokens=1024
                )

                reasoning_text = response.choices[0].message.content.strip()

                # Copy row dan tambahkan reasoning
                new_row = row.copy()
                new_row['reasoning'] = mask_pii(reasoning_text) # Pastikan penalaran juga disensor

                # Mask juga data asli jika sebelumnya tidak disensor
                for k, v in new_row.items():
                    if k != 'reasoning' and isinstance(v, str):
                        new_row[k] = mask_pii(v)

                augmented_data.append(new_row)
                logger.info(f"Berhasil augmentasi baris {idx+1}/{len(data)}")

            except Exception as e:
                logger.error(f"Gagal saat augmentasi baris {idx+1}: {e}")
                # Tetap simpan baris asli dengan reasoning kosong jika gagal
                new_row = row.copy()
                new_row['reasoning'] = "ERROR_GENERATING_REASONING"
                augmented_data.append(new_row)

        return augmented_data

if __name__ == "__main__":
    # Ini hanya untuk uji coba manual (hanya akan jalan jika NVIDIA_API_KEY diset di .env atau env var)
    try:
        gen = DatasetGenerator()
        print("Generator inisialisasi berhasil.")
    except ValueError as e:
        print(f"Error: {e}")
