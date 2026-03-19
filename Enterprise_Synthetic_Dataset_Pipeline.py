#!/usr/bin/env python
# coding: utf-8

# # 🚀 Enterprise Synthetic Dataset Pipeline (Generasi & Kurasi)
#
# Notebook ini menunjukkan eksekusi End-to-End dari sistem dataset *synthetic* tingkat lanjut.
#
# Sistem ini mencakup:
# 1. **Async AI Generation (Actor-Critic):** Pembuatan data massal dengan validasi ketat (Pydantic) menggunakan LLM NVIDIA.
# 2. **Dynamic Pseudonymization:** Perlindungan privasi pintar menggunakan Faker untuk mengganti NIK, Email, dan Telepon asli dengan versi palsu yang realistis.
# 3. **Data Augmentation:** Menambahkan kolom `reasoning` ke dataset secara paralel (Concurrent Processing).
# 4. **Data Curation (NeMo-style):** Pembersihan karakter aneh (Unicode Reformatter), filter kualitas (Word Count Filter), dan deduplikasi presisi.
# 5. **Big Data Export:** Penyimpanan format JSONL/Parquet/CSV via Pandas & PyArrow.
#

# In[ ]:


import os
import asyncio
import nest_asyncio

# nest_asyncio diperlukan agar asyncio bisa berjalan di dalam Jupyter Notebook
nest_asyncio.apply()

from async_generator import AsyncDatasetGenerator
from dataset_viewer import DatasetStorage
from curator_pipeline import CurationPipeline

print("Modul berhasil di-load!")


# ### 1. Setup API Key
# Pastikan Anda telah mengisi file `.env` dengan kredensial NVIDIA API. Sel tertutup ini akan memeriksa ketersediaannya.

# In[ ]:


from dotenv import load_dotenv
load_dotenv()

api_key = os.getenv("NVIDIA_API_KEY")
if api_key:
    print("✅ NVIDIA_API_KEY Terdeteksi!")
else:
    print("❌ NVIDIA_API_KEY belum disetel. Tambahkan ke file .env.")


# ### 2. Inisialisasi Generator

# In[ ]:


# Inisialisasi sistem Async Actor-Critic yang terhubung dengan NVIDIA NIM
if api_key:
    generator = AsyncDatasetGenerator()
else:
    print("Harap setel API Key terlebih dahulu sebelum menginisialisasi generator.")


# ### 3. Generate dari Awal (From Scratch)
# Kita akan membuat 3 baris cerita panjang. Sistem privasi Faker akan secara otomatis mengganti detail PII (NIK, Email, Telepon) yang muncul di hasil dengan data *dummy* secara real-time.

# In[ ]:


topic = "Kisah pelanggan di restoran yang marah karena pesanan salah, di mana ia juga menyebut NIK dan Nomor HP."

async def run_generation():
    if 'generator' in locals() or 'generator' in globals():
        return await generator.generate_from_scratch(topic=topic, count=3)
    return []

raw_data = asyncio.run(run_generation())
print("\n\nData berhasil dibuat:", len(raw_data), "baris.")
raw_data


# ### 4. Menyimpan Draft Awal ke JSONL
# Simpan hasil dari tahap sebelumnya ke dalam file `JSONL` untuk persiapan kurasi.

# In[ ]:


raw_file = "notebook_generated.jsonl"
DatasetStorage.save(raw_data, "notebook_generated", formats=["jsonl"])
DatasetStorage.view_terminal(raw_file)


# ### 5. Kurasi Kualitas (Data Curation)
# Di sini, kita membersihkan `JSONL` tersebut (misal: membuang karakter Unicode aneh, menyaring kalimat yang terlalu pendek, dan membuang duplikat) sehingga dataset akhir benar-benar berkualitas super tinggi.

# In[ ]:


curator = CurationPipeline()
curated_file = "notebook_curated.jsonl"
curator.run_pipeline(input_path=raw_file, output_path=curated_file, text_field="text")

print("\n--- Hasil Setelah Kurasi ---")
DatasetStorage.view_terminal(curated_file)


# --- Selesai! Anda dapat mengubah input dan parameter sesuka Anda untuk mengeksplorasi pembuatan dataset raksasa secara paralel. ---
