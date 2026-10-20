// Package safeerr redacts secrets from error strings before logging or HTTP diagnostics.
package safeerr

import (
	"net/url"
	"regexp"
	"strings"
)

var (
	// user:pass@ in URLs (amqp, http, …)
	userinfoRE = regexp.MustCompile(`(?i)(://)([^/@:\s]+):([^/@\s]+)@`)
	// password=… / passwd=… query-style
	passwordKVRE = regexp.MustCompile(`(?i)(password|passwd|pwd|secret|token)=([^\s&"']+)`)
	// Basic **** base64 blobs sometimes appear in bodies
	basicAuthRE = regexp.MustCompile(`(?i)(authorization:\s*basic\s+)(\S+)`)
)

// String redacts common credential patterns from an error or diagnostic string.
func String(s string) string {
	if s == "" {
		return s
	}
	s = userinfoRE.ReplaceAllString(s, `${1}${2}:***@`)
	s = passwordKVRE.ReplaceAllString(s, `${1}=***`)
	s = basicAuthRE.ReplaceAllString(s, `${1}***`)
	return s
}

// Err redacts err.Error(); returns empty string for nil.
func Err(err error) string {
	if err == nil {
		return ""
	}
	return String(err.Error())
}

// HTTPStatus formats a management/HTTP failure without response body contents.
func HTTPStatus(path string, status int) string {
	return path + ": HTTP " + itoa(status)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [12]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// RedactURLUserinfo strips password from a parsed URL for logging.
func RedactURLUserinfo(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return String(raw)
	}
	if u.User != nil {
		if _, has := u.User.Password(); has {
			u.User = url.UserPassword(u.User.Username(), "***")
		}
	}
	return u.String()
}

// SanitizeSourceError returns a short, body-free error suitable for /targets.
func SanitizeSourceError(s string) string {
	s = String(s)
	// Drop anything that looks like an HTML/JSON body after status.
	if idx := strings.Index(s, ": HTTP "); idx >= 0 {
		rest := s[idx+2:] // "HTTP …"
		if sp := strings.IndexByte(rest, ':'); sp >= 0 {
			// Keep "path: HTTP 401" only.
			prefix := s[:idx]
			codePart := rest[:sp]
			return strings.TrimSpace(prefix + ": " + codePart)
		}
	}
	if len(s) > 200 {
		return s[:200] + "…"
	}
	return s
}
