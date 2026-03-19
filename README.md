# Sistem Dataset Sintetis Kualitas Tinggi & Aman Privasi

Sistem ini digunakan untuk menghasilkan dataset teks panjang berkualitas tinggi dan aman dari kebocoran data pribadi (PII), serta mampu memperkaya dataset yang ada dengan kolom baru (misalnya, kolom "reasoning" pada dataset tipe instruksi). Sistem ini terintegrasi dengan NVIDIA NIM API (kompatibel dengan format OpenAI).

## Fitur Utama
1.  **Pembuatan Dataset Murni (Generate from Scratch):** Membuat teks panjang dari instruksi awal.
2.  **Penambahan Label pada Dataset (Data Augmentation):** Menambahkan kolom baru ke dataset yang sudah ada (misalnya: menambahkan "reasoning" ke dalam dataset instruksi seperti Alpaca).
3.  **Perlindungan Privasi:** Mendeteksi dan menyensor NIK (16 digit), Email, dan Nomor Telepon secara otomatis dari teks yang dihasilkan, menjaga keamanan data.
4.  **Export & View CSV:** Menyimpan dataset ke dalam format CSV dan melihat isinya dengan mudah langsung di terminal.

## Arsitektur "Enterprise-Grade"
- **Asynchronous & Concurrent:** Menggunakan `AsyncOpenAI` dan `asyncio` untuk mengirim banyak request secara paralel, mempercepat proses augmentasi secara masif.
- **Actor-Critic AI Generation:** AI tidak sekadar menghasilkan teks, melainkan menggunakan pola dua model: satu bertindak sebagai pembuat konten (Actor), dan satu bertindak sebagai validator/pengoreksi format (Critic).
- **Strict Validation:** Menggunakan `Pydantic` untuk memastikan skema JSON yang dikembalikan oleh LLM 100% valid dan sesuai struktur.
- **Dynamic Pseudonymization:** Privasi menggunakan `Faker` id_ID untuk mengganti entitas NIK, Email, dan Nomor HP asli dengan data sintetik yang masuk akal namun palsu, bukan sekadar memberikan teks sensor kaku, sehingga model AI tetap bisa mempelajari pola kalimat secara natural.
- **Big Data Storage:** Mendukung format file modern untuk data besar seperti `Parquet` dan `JSONL`, selain format `CSV` tradisional.

## Cara Instalasi
1. Pastikan Anda sudah menginstal Python (direkomendasikan versi 3.8 ke atas).
2. Install library yang dibutuhkan:
   ```bash
   pip install -r requirements.txt
   ```

## Pengaturan API Key (NVIDIA NIM)
Sistem ini menggunakan NVIDIA API. Anda memerlukan API Key dari [https://build.nvidia.com/](https://build.nvidia.com/).

1. Buat file bernama `.env` di folder proyek ini.
2. Tambahkan baris berikut di dalam file `.env`:
   ```env
   NVIDIA_API_KEY=nvapi-kunci_rahasia_anda_disini
   ```

## Cara Penggunaan
Jalankan file utama untuk melihat demonstrasi bagaimana sistem membuat dataset murni dan memperkaya dataset yang ada:
```bash
python main.py
```
Ini akan menghasilkan file CSV (`generated_dataset.csv` dan `augmented_dataset.csv`) dan menampilkannya di layar.
