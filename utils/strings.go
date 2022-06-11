package utils

import "strings"

func StringSlice(str, sep string) []string {
	var sl []string

	for _, p := range strings.Split(str, sep) {
		if str := strings.TrimSpace(p); len(str) > 0 {
			sl = append(sl, strings.TrimSpace(p))
		}
	}

	return sl
}
