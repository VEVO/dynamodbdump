package storage

import (
	"regexp"
	"testing"
)

func TestGenNewFileName(t *testing.T) {
	name := genNewFileName()
	matched, err := regexp.MatchString("^[A-Z0-9a-z]{8}-[A-Z0-9a-z]{4}-[A-Z0-9a-z]{4}-[A-Z0-9a-z]{4}-[A-Z0-9a-z]{12}$", name)
	if err != nil {
		t.Errorf("unable to check the string returned by genNewFileName(): %s\n the string was: %s", err.Error(), name)
	}
	if !matched {
		t.Errorf("genNewFileName() returned an unexpected string: %s", name)
	}
}
