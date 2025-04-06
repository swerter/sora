package helpers

import "unicode/utf8"

func SanitizeUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	buf := make([]rune, 0, len(s))
	for i, r := range s {
		if r == utf8.RuneError {
			_, size := utf8.DecodeRuneInString(s[i:])
			if size == 1 {
				continue // skip invalid byte
			}
		}
		buf = append(buf, r)
	}
	return string(buf)
}
