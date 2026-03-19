import re

def mask_pii(text: str) -> str:
    """
    Fungsi ini menyensor PII (Personally Identifiable Information) dari teks.
    Informasi yang disensor meliputi:
    - NIK (16 digit angka)
    - Email
    - Nomor Telepon (format internasional, lokal Indonesia, dan variasi umum)

    Args:
        text (str): Teks asli yang mungkin mengandung PII.

    Returns:
        str: Teks dengan PII yang telah disensor.
    """
    if not isinstance(text, str):
        return text

    # Regex untuk mendeteksi NIK (tepat 16 digit yang mungkin dipisahkan oleh spasi/dash)
    nik_pattern = r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b'
    text = re.sub(nik_pattern, '[NIK_DISENSOR]', text)

    # Regex untuk mendeteksi Email
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    text = re.sub(email_pattern, '[EMAIL_DISENSOR]', text)

    # Regex untuk mendeteksi Nomor Telepon
    # Mencakup format: +628..., 08..., 628..., mungkin dengan spasi atau dash
    phone_pattern = r'(?:\+62|62|0)[-\s]?8[0-9]{1,2}[-\s]?[0-9]{3,4}[-\s]?[0-9]{3,4}\b'
    text = re.sub(phone_pattern, '[TELEPON_DISENSOR]', text)

    return text

if __name__ == "__main__":
    # Test kasus
    sample_text = "Halo, nama saya Budi. NIK saya 3171234567890123. Email budi@example.com dan nomor HP 0812-3456-7890 atau +628111222333."
    print("Original:", sample_text)
    print("Masked:", mask_pii(sample_text))
