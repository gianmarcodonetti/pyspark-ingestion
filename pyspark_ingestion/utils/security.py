import hashlib
import json

from pyDes import triple_des


def pydes_decrypt(cipher_text, password):
    hashed_pw = hash_password(password)
    plain_text = triple_des(hashed_pw).decrypt(cipher_text, padmode=2)
    return plain_text


def pydes_encrypt(plain_text, password):
    hashed_pw = hash_password(password)
    cipher_text = triple_des(hashed_pw).encrypt(plain_text, padmode=2)
    return cipher_text


def hash_password(password):
    return hashlib.md5(password).digest()


def decrypt_json(filename, password):
    with open(filename, 'rb') as fin:
        decrypted = pydes_decrypt(fin.read(), password.encode('utf-8'))
    settings_dict = json.loads(decrypted)
    return settings_dict


def encrypt_json(filename, password):
    with open(filename, 'r', encoding='utf-8') as fin:
        encrypted = pydes_encrypt(fin.read(), password.encode('utf-8'))

    with open(filename + '.pydes', 'wb') as fout:
        fout.write(encrypted)
    return
