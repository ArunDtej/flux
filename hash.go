package flux

// Hash calculates a high-performance, zero-allocation 64-bit FNV-1a hash value
// for the given string.
//
// This is commonly used to convert arbitrary string IDs (e.g. usernames, UUIDs)
// into uint64 keys for Flux buffering, providing high-performance map keys and
// uniform distribution across shards.
func Hash(s string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	var h uint64 = offset64
	for i := 0; i < len(s); i++ {
		h *= prime64
		h ^= uint64(s[i])
	}
	return h
}
