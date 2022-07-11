from Crypto.Cipher import AES 
from secrets import token_bytes
key = token_bytes(16)
def encrypt(msg):
    ciper = AES.new(key,AES.MODE_EAX)
    nonce = ciper.nonce
    ciper_text,tag = ciper.encrypt_and_digit(msg.encode('ascii'))
    return nonce,ciper_text,tag
# Create your views here.
def decrypt(nonce,ciper_text,tag):
    ciper = AES.new(key,AES.MODE_EAX,nonce=nonce)
    plainText = ciper.decrypt(ciper_text) 
    try :
        ciper.verify(tag)
        return plainText.decode('ascii')
    except:
        return False