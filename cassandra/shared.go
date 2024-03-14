package cassandra

import (
	"crypto/sha256"
	"encoding/hex"
	"hash/crc32"
)

// taken from here - http://techblog.d2-si.eu/2018/02/23/my-first-terraform-provider.html
func hash(s string) string {
	sha := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sha[:])
}

func stringHashcode(s string) int {
	v := int(crc32.ChecksumIEEE([]byte(s)))
	if v >= 0 {
		return v
	}
	if -v >= 0 {
		return -v
	}
	// v == MinInt
	return 0
}
