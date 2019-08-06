package formats

import (
	"regexp"
	"strings"
)

func EncodePropertyName(potentiallyUnsafeName string) string {
	var trailingPeriods = regexp.MustCompile(`[.]*$`)
	return strings.TrimSpace(strings.Replace(strings.Replace(
		trailingPeriods.ReplaceAllString(potentiallyUnsafeName, ""),
		"(", "_", -1),
		")", "_", -1))
}
