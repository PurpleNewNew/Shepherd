package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"

	"golang.org/x/crypto/hkdf"
)

const (
	keyDerivationSalt = "shepherd/aes/v1"
	keyDerivationInfo = "shepherd/aes-gcm"
)

// KeyPadding 使用 HKDF 派生 32 字节 AES 密钥，输入为空时返回 nil。
func KeyPadding(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}
	kdf := hkdf.New(sha256.New, key, []byte(keyDerivationSalt), []byte(keyDerivationInfo))
	derived := make([]byte, 32)
	if _, err := io.ReadFull(kdf, derived); err != nil {
		panic(fmt.Sprintf("aes: hkdf derive failed: %v", err))
	}
	return derived
}

func genNonce(nonceSize int) ([]byte, error) {
	nonce := make([]byte, nonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return nonce, nil
}

func AESDecrypt(cryptedData, key []byte) ([]byte, error) {
	if key == nil {
		return cryptedData, nil
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("aes: new cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("aes: new gcm: %w", err)
	}
	nonceSize := gcm.NonceSize()
	if len(cryptedData) < nonceSize {
		return nil, fmt.Errorf("aes: ciphertext too short (%d < %d)", len(cryptedData), nonceSize)
	}
	nonce, ciphertext := cryptedData[:nonceSize], cryptedData[nonceSize:]
	origData, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("aes: decrypt failed: %w", err)
	}
	return origData, nil
}

func AESEncrypt(origData, key []byte) ([]byte, error) {
	if key == nil {
		return origData, nil
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("aes: new cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("aes: new gcm: %w", err)
	}
	nonce, err := genNonce(gcm.NonceSize())
	if err != nil {
		return nil, fmt.Errorf("aes: nonce: %w", err)
	}
	return gcm.Seal(nonce, nonce, origData, nil), nil
}
