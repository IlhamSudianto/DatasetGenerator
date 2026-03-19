import os
import json
import asyncio
import logging
from typing import List, Dict, Any, Optional
from openai import AsyncOpenAI
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError
from advanced_privacy import anonymize_text

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Pydantic Schemas for validation
class DatasetItem(BaseModel):
    id: int
    topic: str
    text: str = Field(..., description="The long form text generated")

class DatasetList(BaseModel):
    items: List[DatasetItem]

class AsyncDatasetGenerator:
    def __init__(self, api_key: Optional[str] = None):
        """
        Enterprise-grade Async Generator.
        Menggunakan AsyncOpenAI untuk konkurensi.
        """
        self.api_key = api_key or os.getenv("NVIDIA_API_KEY")
        if not self.api_key:
            raise ValueError("NVIDIA_API_KEY tidak ditemukan. Harap setel di environment atau file .env")

        self.client = AsyncOpenAI(
            base_url="https://integrate.api.nvidia.com/v1",
            api_key=self.api_key
        )
        self.model = "meta/llama-3.1-70b-instruct"

    async def _actor_generate(self, topic: str, count: int) -> str:
        """Actor: Menghasilkan draft awal teks."""
        prompt = f"""Buatlah {count} paragraf panjang dan sangat detail mengenai '{topic}'.
Setiap paragraf harus menceritakan aspek atau contoh yang berbeda secara naratif.
Jawab HANYA dalam format JSON valid seperti ini tanpa markdown tambahan:
{{
  "items": [
    {{"id": 1, "topic": "...", "text": "..."}},
    ...
  ]
}}"""
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Anda adalah AI pembuat dataset yang merespons secara eksklusif dalam format JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.8,
                max_tokens=2048
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Error pada Actor: {e}")
            return ""

    async def _critic_evaluate_and_fix(self, raw_content: str) -> Optional[List[Dict]]:
        """Critic: Mengevaluasi, memperbaiki format, dan memvalidasi struktur via Pydantic."""
        content = raw_content.strip()
        if content.startswith("```json"):
            content = content[7:]
        if content.endswith("```"):
            content = content[:-3]

        try:
            data = json.loads(content.strip())
            # Validasi Pydantic ketat
            validated_data = DatasetList(**data)
            return [item.model_dump() for item in validated_data.items]
        except (json.JSONDecodeError, ValidationError) as e:
            logger.warning(f"Critic menemukan kesalahan format/struktur: {e}. Mencoba memperbaiki...")
            fix_prompt = f"""Perbaiki output JSON berikut agar menjadi valid dan memenuhi struktur: {{"items": [{{"id":int, "topic":str, "text":str}}]}}\n\nData Salah:\n{raw_content}"""
            try:
                response = await self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "Anda adalah JSON repair bot. Hanya keluarkan JSON valid."},
                        {"role": "user", "content": fix_prompt}
                    ],
                    temperature=0.1
                )
                fixed_content = response.choices[0].message.content.strip()
                if fixed_content.startswith("```json"): fixed_content = fixed_content[7:]
                if fixed_content.endswith("```"): fixed_content = fixed_content[:-3]

                fixed_data = json.loads(fixed_content.strip())
                validated_data = DatasetList(**fixed_data)
                return [item.model_dump() for item in validated_data.items]
            except Exception as fix_e:
                logger.error(f"Critic gagal memperbaiki: {fix_e}")
                return None

    async def generate_from_scratch(self, topic: str, count: int = 1) -> List[Dict[str, str]]:
        """Pipeline utuh Actor-Critic untuk generate data."""
        logger.info(f"Actor mulai menyusun {count} draft tentang: '{topic}'")
        raw_text = await self._actor_generate(topic, count)

        if not raw_text:
            return []

        logger.info("Critic sedang mengevaluasi draft...")
        validated_items = await self._critic_evaluate_and_fix(raw_text)

        if not validated_items:
            logger.error("Gagal mendapatkan data tervalidasi dari pipeline Actor-Critic.")
            return []

        # Pseudonimisasi PII pada hasil akhir
        dataset = []
        for item in validated_items:
            for key, value in item.items():
                if isinstance(value, str):
                    item[key] = anonymize_text(value)
            dataset.append(item)

        logger.info(f"Berhasil meng-generate & memvalidasi {len(dataset)} baris data murni.")
        return dataset

    async def _augment_single_row(self, row: Dict[str, str], instruction_col: str, input_col: str, output_col: str) -> Dict[str, str]:
        """Proses satu baris secara mandiri untuk konkurensi."""
        instruction = row.get(instruction_col, "")
        input_text = row.get(input_col, "")
        output_text = row.get(output_col, "")

        prompt = f"""Diberikan sebuah task AI:
Instruksi: {instruction}
Input: {input_text}
Output AI: {output_text}

Tugas: Berikan 'reasoning' logis mengapa AI menghasilkan output tersebut.
Jawablah HANYA dengan teks penalarannya saja, langsung ke intinya."""

        new_row = row.copy()
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Anda adalah sistem analitik ahli reasoning."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.4,
                max_tokens=1024
            )
            reasoning_text = response.choices[0].message.content.strip()
            new_row['reasoning'] = anonymize_text(reasoning_text)

            # Pseudonimisasi kolom asli juga
            for k, v in new_row.items():
                if k != 'reasoning' and isinstance(v, str):
                    new_row[k] = anonymize_text(v)
        except Exception as e:
            logger.error(f"Gagal augmentasi baris: {e}")
            new_row['reasoning'] = "ERROR_GENERATING_REASONING"

        return new_row

    async def augment_dataset_concurrently(self, data: List[Dict[str, str]], instruction_column: str, input_column: str, output_column: str) -> List[Dict[str, str]]:
        """
        Memproses seluruh dataset secara PARALEL dengan asynchronous tasks.
        Sangat cepat dibandingkan memproses baris demi baris.
        """
        logger.info(f"Memulai concurrent data augmentation untuk {len(data)} baris.")

        # Buat list of coroutines
        tasks = [
            self._augment_single_row(row, instruction_column, input_column, output_column)
            for row in data
        ]

        # Eksekusi secara paralel
        augmented_data = await asyncio.gather(*tasks)
        logger.info("Augmentasi dataset selesai.")
        return list(augmented_data)
