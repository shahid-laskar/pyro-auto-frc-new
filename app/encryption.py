import base64
import hashlib

from Crypto.Cipher import DES3
from Crypto.Util.Padding import pad, unpad


def _derive_key(secret_key: str) -> bytes:
    
    sha1 = hashlib.sha1(secret_key.encode("utf-8")).digest()  # 20 bytes
    return sha1 + b"\x00" * 4                                  # pad to 24 bytes


def encrypt(message: str, secret_key: str) -> str:
   
    key = _derive_key(secret_key)
    cipher = DES3.new(key, DES3.MODE_ECB)
    padded = pad(message.encode("utf-8"), 8)   # PKCS5: 8-byte DES block
    return base64.b64encode(cipher.encrypt(padded)).decode("utf-8").rstrip("=")


def decrypt(encrypted_text: str, secret_key: str) -> str:
    
    key = _derive_key(secret_key)
    padded_b64 = encrypted_text + "=" * (-len(encrypted_text) % 4)
    encrypted_bytes = base64.b64decode(padded_b64)
    cipher = DES3.new(key, DES3.MODE_ECB)
    return unpad(cipher.decrypt(encrypted_bytes), 8).decode("utf-8")