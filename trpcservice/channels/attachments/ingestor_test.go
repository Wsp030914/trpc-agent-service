package attachments

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"net"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

func TestFilenameFromContentDisposition(t *testing.T) {
	for _, test := range []struct {
		header string
		want   string
	}{
		{header: `attachment; filename="report.docx"`, want: "report.docx"},
		{header: `attachment; filename="C:\\temp\\report.xlsx"`, want: "report.xlsx"},
		{header: `attachment; filename*=UTF-8''%E6%8A%A5%E5%91%8A.pdf`, want: "报告.pdf"},
	} {
		if got := filenameFromContentDisposition(test.header); got != test.want {
			t.Errorf("filename for %q = %q, want %q", test.header, got, test.want)
		}
	}
}

func TestDecryptWeComMediaUsesOfficialAESKeyAndPadding(t *testing.T) {
	key := []byte("01234567890123456789012345678901")
	plaintext := []byte("official ai bot media")
	padded := append([]byte(nil), plaintext...)
	padding := 32 - len(padded)%32
	for range padding {
		padded = append(padded, byte(padding))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	encrypted := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, key[:aes.BlockSize]).CryptBlocks(encrypted, padded)
	got, err := decryptWeComMedia(encrypted, base64.StdEncoding.EncodeToString(key))
	if err != nil {
		t.Fatalf("decrypt media: %v", err)
	}
	if string(got) != string(plaintext) {
		t.Fatalf("decrypted media = %q, want %q", got, plaintext)
	}
}

func TestProviderMediaRefKeepsWeComDecryptionMetadataInMemory(t *testing.T) {
	ref := channels.ProviderMediaRef{
		Kind:          channels.MessageTypeImage,
		Reference:     "https://wework.qpic.cn/attachment/1",
		DecryptionKey: "YWJj",
	}
	if err := ref.Validate(); err != nil {
		t.Fatalf("provider media ref: %v", err)
	}
}

func TestIsAllowedWeComMediaHost(t *testing.T) {
	for _, test := range []struct {
		host string
		want bool
	}{
		{host: "qyapi.weixin.qq.com", want: true},
		{host: "WEWORK.QPIC.CN", want: true},
		{host: "example.test", want: false},
		{host: "qyapi.weixin.qq.com.evil.test", want: false},
	} {
		if got := isAllowedWeComMediaHost(test.host); got != test.want {
			t.Fatalf("host %q allowed = %t, want %t", test.host, got, test.want)
		}
	}
}

func TestIsPublicInternetIPRejectsNonPublicTargets(t *testing.T) {
	for _, test := range []struct {
		name string
		ip   string
		want bool
	}{
		{name: "loopback", ip: "127.0.0.1", want: false},
		{name: "metadata", ip: "169.254.169.254", want: false},
		{name: "private", ip: "10.0.0.1", want: false},
		{name: "cgnat", ip: "100.64.0.1", want: false},
		{name: "ula", ip: "fd00::1", want: false},
		{name: "public", ip: "1.1.1.1", want: true},
	} {
		if got := isPublicInternetIP(net.ParseIP(test.ip)); got != test.want {
			t.Errorf("%s public = %t, want %t", test.name, got, test.want)
		}
	}
}
