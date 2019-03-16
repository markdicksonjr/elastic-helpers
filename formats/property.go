package formats

import "strings"

func EncodePropertyName(potentiallyUnsafeName string) string {
	return strings.Replace(strings.Replace(strings.Replace(
		potentiallyUnsafeName,
		"(", "_", -1),
		")", "_", -1),
		".", "_", -1)
}
