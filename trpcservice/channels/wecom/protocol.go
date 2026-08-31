package wecom

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
)

const wecomPaddingBlockSize = 32

var (
	errInvalidWeComSignature = errors.New("invalid wecom signature")
	errWeComDecryptFailed    = errors.New("wecom message decryption failed")
)

type callbackCodec struct {
	token string
	key   []byte
}

func newCallbackCodec(token, encodingAESKey string) (callbackCodec, error) {
	if token == "" {
		return callbackCodec{}, errors.New("wecom token is required")
	}
	key, err := decodeEncodingAESKey(encodingAESKey)
	if err != nil {
		return callbackCodec{}, err
	}
	return callbackCodec{token: token, key: key}, nil
}

func decodeEncodingAESKey(value string) ([]byte, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, errors.New("wecom encoding aes key is required")
	}
	encodings := []*base64.Encoding{
		base64.RawStdEncoding,
		base64.StdEncoding,
	}
	for _, encoding := range encodings {
		decoded, err := encoding.DecodeString(value)
		if err == nil && len(decoded) == 32 {
			return decoded, nil
		}
	}
	return nil, errors.New("wecom encoding aes key must decode to 32 bytes")
}

func (c callbackCodec) verifySignature(timestamp, nonce, encrypted, signature string) error {
	if timestamp == "" || nonce == "" || encrypted == "" || signature == "" {
		return errInvalidWeComSignature
	}
	expected := c.signature(timestamp, nonce, encrypted)
	if len(signature) != len(expected) ||
		subtle.ConstantTimeCompare([]byte(expected), []byte(signature)) != 1 {
		return errInvalidWeComSignature
	}
	return nil
}

func (c callbackCodec) signature(timestamp, nonce, encrypted string) string {
	values := []string{c.token, timestamp, nonce, encrypted}
	sort.Strings(values)
	digest := sha1.Sum([]byte(strings.Join(values, "")))
	return hex.EncodeToString(digest[:])
}

func (c callbackCodec) decrypt(encrypted string) ([]byte, error) {
	ciphertext, err := decodeWeComBase64(encrypted)
	if err != nil {
		return nil, fmt.Errorf("%w: ciphertext encoding", errWeComDecryptFailed)
	}
	if len(c.key) != 32 || len(ciphertext) == 0 || len(ciphertext)%aes.BlockSize != 0 {
		return nil, errWeComDecryptFailed
	}
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return nil, fmt.Errorf("%w: cipher: %v", errWeComDecryptFailed, err)
	}
	plaintext := make([]byte, len(ciphertext))
	decrypter := cipher.NewCBCDecrypter(block, c.key[:aes.BlockSize])
	decrypter.CryptBlocks(plaintext, ciphertext)
	plaintext, err = unpadWeCom(plaintext)
	if err != nil || len(plaintext) < aes.BlockSize+4 {
		return nil, errWeComDecryptFailed
	}
	messageLength := binary.BigEndian.Uint32(plaintext[aes.BlockSize : aes.BlockSize+4])
	messageStart := aes.BlockSize + 4
	if messageLength > uint32(len(plaintext)-messageStart) {
		return nil, errWeComDecryptFailed
	}
	messageEnd := messageStart + int(messageLength)
	if messageEnd != len(plaintext) {
		return nil, errWeComDecryptFailed
	}
	return append([]byte(nil), plaintext[messageStart:messageEnd]...), nil
}

func (c callbackCodec) encrypt(message []byte) (string, error) {
	if len(c.key) != 32 {
		return "", errors.New("wecom encoding aes key is invalid")
	}
	randomPrefix := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, randomPrefix); err != nil {
		return "", fmt.Errorf("generate wecom message prefix: %w", err)
	}
	plaintext := make([]byte, aes.BlockSize+4+len(message))
	copy(plaintext, randomPrefix)
	binary.BigEndian.PutUint32(plaintext[aes.BlockSize:aes.BlockSize+4], uint32(len(message)))
	copy(plaintext[aes.BlockSize+4:], message)
	plaintext = padWeCom(plaintext)
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return "", fmt.Errorf("create wecom cipher: %w", err)
	}
	ciphertext := make([]byte, len(plaintext))
	encrypter := cipher.NewCBCEncrypter(block, c.key[:aes.BlockSize])
	encrypter.CryptBlocks(ciphertext, plaintext)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

func decodeWeComBase64(value string) ([]byte, error) {
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding} {
		decoded, err := encoding.DecodeString(value)
		if err == nil {
			return decoded, nil
		}
	}
	return nil, errors.New("invalid base64")
}

func padWeCom(value []byte) []byte {
	padding := wecomPaddingBlockSize - len(value)%wecomPaddingBlockSize
	return append(value, bytesOf(byte(padding), padding)...)
}

func unpadWeCom(value []byte) ([]byte, error) {
	if len(value) == 0 || len(value)%wecomPaddingBlockSize != 0 {
		return nil, errors.New("invalid padding")
	}
	padding := int(value[len(value)-1])
	if padding == 0 || padding > wecomPaddingBlockSize || padding > len(value) {
		return nil, errors.New("invalid padding")
	}
	for _, current := range value[len(value)-padding:] {
		if int(current) != padding {
			return nil, errors.New("invalid padding")
		}
	}
	return value[:len(value)-padding], nil
}

func bytesOf(value byte, count int) []byte {
	result := make([]byte, count)
	for index := range result {
		result[index] = value
	}
	return result
}
