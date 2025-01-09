package bus

import (
	"errors"
	"unicode"
)

// MatchSubject checks if the given subject matches the pattern.
// it has been optimized for performance and zero allocations.
func MatchSubject(subject, pattern string) bool {
	i, j := 0, 0
	subLen, patLen := len(subject), len(pattern)

	for i < subLen && j < patLen {
		subStart, patStart := i, j

		// Advance index to next dot in subject
		for i < subLen && subject[i] != '.' {
			i++
		}
		// Advance index to next dot in pattern
		for j < patLen && pattern[j] != '.' {
			j++
		}

		subPart := subject[subStart:i]
		patPart := pattern[patStart:j]

		// Handle ">" (catch-all) in pattern
		if patPart == ">" {
			return true // matches anything that follows
		}

		// Handle "*" (single element wildcard)
		if patPart != "*" && subPart != patPart {
			return false
		}

		// Skip the dots
		i++
		j++
	}

	// If pattern contains ">", it's valid to have remaining parts in subject
	if j < patLen && pattern[j:] == ">" {
		return true
	}

	// Ensure both subject and pattern are fully processed
	return i >= subLen && j >= patLen
}

func ValidateSubject(subject string) error {
	// subject must be in a form of "a.b.c", "a.b.*", "a.b.>"
	// - subject should not have > at the middle of the string
	// - subject should not have spaces or any other characters other than alphanumerics, dots, *, and >
	// - * should have a dot or nothing before and after it
	// - > should have a dot or nothing before it and nothing after it
	// - subject should not starts with .
	// - subject should not ends with .
	// - subject should not have .. in the middle of the string

	if subject == "" {
		return errors.New("subject is empty")
	}

	for i, c := range subject {
		if i == 0 {
			if c == '.' {
				return errors.New("subject should not starts with .")
			} else if c == '*' {
				return errors.New("subject should not starts with *")
			} else if c == '>' {
				return errors.New("subject should not starts with >")
			}
		}

		if i == len(subject)-1 && c == '.' {
			return errors.New("subject should not ends with .")
		}

		if c == ' ' {
			return errors.New("subject should not have spaces")
		}

		if i > 0 && c == '.' && subject[i-1] == '.' {
			return errors.New("subject should not have series of dots one after another")
		}

		if c == '>' && i != len(subject)-1 {
			return errors.New("subject should not have anything after >")
		}

		if c == '>' && i > 0 && subject[i-1] != '.' {
			return errors.New("subject should have a dot before >")
		}

		if c == '*' {
			if i > 0 && subject[i-1] != '.' {
				return errors.New("subject should have a dot before *")
			}

			if i != len(subject)-1 && subject[i+1] != '.' {
				return errors.New("subject should have a dot after *")
			}
		}

		if c != '.' && c != '*' && c != '>' && !unicode.IsDigit(c) && !unicode.IsLetter(c) && c != '_' {
			return errors.New("subject should have only consists of alphanumerics, dots, *, > and _")
		}
	}

	return nil
}
