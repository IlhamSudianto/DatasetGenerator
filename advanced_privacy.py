import re
from faker import Faker

# Inisialisasi Faker dengan locale Indonesia
fake = Faker('id_ID')

# Dictionary untuk menyimpan mapping (entity asli -> entity palsu)
# agar konsisten di seluruh dataset jika muncul PII yang sama berkali-kali.
_pseudonym_map = {
    "nik": {},
    "email": {},
    "phone": {}
}

def get_fake_nik(real_nik: str) -> str:
    """Mengembalikan NIK palsu yang konsisten untuk NIK asli yang sama."""
    if real_nik not in _pseudonym_map["nik"]:
        # Generate NIK format 16 digit: (Provinsi 2)(Kota 2)(Kecamatan 2)(TglLahir 6)(Urut 4)
        # Jika id_ID tidak memiliki nik(), kita gunakan random 16 angka
        _pseudonym_map["nik"][real_nik] = fake.numerify(text="################")
    return _pseudonym_map["nik"][real_nik]

def get_fake_email(real_email: str) -> str:
    """Mengembalikan email palsu yang konsisten."""
    if real_email not in _pseudonym_map["email"]:
        _pseudonym_map["email"][real_email] = fake.email()
    return _pseudonym_map["email"][real_email]

def get_fake_phone(real_phone: str) -> str:
    """Mengembalikan nomor telepon palsu yang konsisten."""
    if real_phone not in _pseudonym_map["phone"]:
        _pseudonym_map["phone"][real_phone] = fake.phone_number()
    return _pseudonym_map["phone"][real_phone]

def anonymize_text(text: str) -> str:
    """
    Fungsi ini melakukan pseudonimisasi tingkat lanjut.
    Alih-alih hanya menyensor [DISENSOR], fungsi ini mengganti PII asli
    dengan PII palsu yang terlihat realistis secara dinamis agar
    model AI tetap mempelajari pola kalimat yang natural.
    """
    if not isinstance(text, str):
        return text

    # Regex untuk mendeteksi Nomor Telepon
    # (jalankan ini dulu agar NIK tidak overlap dengan no telepon panjang)
    phone_pattern = r'(?:\+62|62|0)[-\s]?8[0-9]{1,2}[-\s]?[0-9]{3,4}[-\s]?[0-9]{3,4}\b'
    text = re.sub(phone_pattern, lambda m: get_fake_phone(m.group(0)), text)

    # Regex untuk mendeteksi NIK (tepat 16 digit yang mungkin dipisahkan oleh spasi/dash)
    nik_pattern = r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b'
    # Ganti setiap match dengan fungsi yang memanggil Faker
    text = re.sub(nik_pattern, lambda m: get_fake_nik(m.group(0)), text)

    # Regex untuk mendeteksi Email
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    text = re.sub(email_pattern, lambda m: get_fake_email(m.group(0)), text)

    return text

if __name__ == "__main__":
    # Test kasus
    sample_text = "Halo, nama saya Budi. NIK saya 3171234567890123. Email budi@example.com dan nomor HP 0812-3456-7890 atau +628111222333. Jangan lupa NIK istri saya 3171234567890123 juga."
    print("--- ORIGINAL TEXT ---")
    print(sample_text)
    print("\n--- PSEUDONYMIZED TEXT ---")
    print(anonymize_text(sample_text))
