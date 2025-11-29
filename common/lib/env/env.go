package envutil

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

func init() {
	// Load .env only if present (local)
	_ = godotenv.Load()
}

// Get returns env value or a fallback
func Get(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// Must returns env value or kills the app
func Must(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("missing required environment variable: %s", key)
	}
	return v
}

// GetInt returns int(env) or fallback
func GetInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}

	n, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("invalid int for %s: %v", key, err)
	}
	return n
}

// GetBool returns bool(env) or fallback
// true values: "1", "true", "yes"
func GetBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}

	switch v {
	case "1", "true", "TRUE", "yes", "YES":
		return true
	case "0", "false", "FALSE", "no", "NO":
		return false
	default:
		log.Fatalf("invalid bool for %s: %s", key, v)
		return false
	}
}
