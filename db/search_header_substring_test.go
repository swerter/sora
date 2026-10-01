package db

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/emersion/go-imap/v2"
	"github.com/migadu/sora/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Header search (SEARCH FROM/TO/CC/SUBJECT/HEADER References, TEXT, and the User API
// mailbox search) must match headers with strpos(), never LIKE '%term%'.
//
// LIKE on the subject and *_sort columns is served by the corpus-wide trigram GINs from
// migration 000034, and the planner's cost model for them is wrong by three orders of
// magnitude: a production SEARCH FROM on a 777-message mailbox was planned through the
// trigram indexes at an estimated cost of 1106 and read 3.5 GB of GIN pages in 70 s,
// where filtering the mailbox's own 777 rows took 744 pages and 0.26 s. The trigram
// cost scales with cluster-wide matches times trigrams in the pattern and does not
// depend on the mailbox, so no mailbox-scoped search may be routed through it. strpos is
// not indexable, which pins the plan to a mailbox index. See substringCond.

// headerSearchSourceFiles are the files that build header-matching SQL.
var headerSearchSourceFiles = []string{"search.go", "user_operations.go"}

// TestHeaderSearchSQLNeverUsesLike scans the source of the header-search builders for a
// LIKE token outside comments. Any new header predicate written as LIKE would silently
// re-enable the trigram plan, so the guard is on the source, not on one generated query.
func TestHeaderSearchSQLNeverUsesLike(t *testing.T) {
	likeToken := regexp.MustCompile(`\bLIKE\b`)
	for _, name := range headerSearchSourceFiles {
		src, err := os.ReadFile(name)
		require.NoError(t, err)
		for i, line := range strings.Split(string(src), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") || strings.HasPrefix(trimmed, "--") {
				continue
			}
			if likeToken.MatchString(line) {
				t.Errorf("%s:%d uses LIKE; header matching must use substringCond/strpos so the planner cannot pick the corpus-wide trigram GINs: %s", name, i+1, trimmed)
			}
		}
	}
}

// TestHeaderSearchCriteriaBindLiteralTerms verifies, for every header criterion, that
// the generated SQL is a strpos() test and the bound value is the bare lowercased term:
// no LIKE, and no wildcard wrapping, which LIKE needed and which also turned a % or _ in
// the user's term into a wildcard.
func TestHeaderSearchCriteriaBindLiteralTerms(t *testing.T) {
	var db *Database // the builder never dereferences its receiver

	cases := []struct {
		name     string
		criteria *imap.SearchCriteria
		wantSQL  []string
	}{
		{"FROM", &imap.SearchCriteria{Header: []imap.SearchCriteriaHeaderField{{Key: "From", Value: "Support@Equishark.com"}}},
			[]string{"strpos(m.from_email_sort, @", "strpos(m.from_name_sort, @"}},
		{"TO", &imap.SearchCriteria{Header: []imap.SearchCriteriaHeaderField{{Key: "To", Value: "Support@Equishark.com"}}},
			[]string{"strpos(m.to_email_sort, @", "strpos(m.to_name_sort, @"}},
		{"CC", &imap.SearchCriteria{Header: []imap.SearchCriteriaHeaderField{{Key: "Cc", Value: "Support@Equishark.com"}}},
			[]string{"strpos(m.cc_email_sort, @"}},
		{"SUBJECT", &imap.SearchCriteria{Header: []imap.SearchCriteriaHeaderField{{Key: "Subject", Value: "Support@Equishark.com"}}},
			[]string{"strpos(LOWER(m.subject), @"}},
		{"HEADER References", &imap.SearchCriteria{Header: []imap.SearchCriteriaHeaderField{{Key: "References", Value: "Support@Equishark.com"}}},
			[]string{`strpos(LOWER(m."references"), @`}},
		{"TEXT", &imap.SearchCriteria{Text: []string{"Support@Equishark.com"}},
			[]string{"strpos(LOWER(m.subject), @", "strpos(m.from_email_sort, @", "strpos(m.cc_email_sort, @"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			paramCounter := 0
			query, args, err := db.buildSearchCriteria(tc.criteria, "p", &paramCounter)
			require.NoError(t, err)
			for _, want := range tc.wantSQL {
				assert.Contains(t, query, want)
			}
			assert.NotContains(t, query, " LIKE ")

			var bound []string
			for _, v := range args {
				if s, ok := v.(string); ok {
					bound = append(bound, s)
				}
			}
			assert.Contains(t, bound, "support@equishark.com", "term must be bound lowercased, exactly as typed")
			for _, s := range bound {
				assert.False(t, strings.HasPrefix(s, "%") || strings.HasSuffix(s, "%"), "no wildcard wrapping: %q", s)
			}
		})
	}
}

// TestHeaderSearchMatchesLiterally runs header searches against PostgreSQL and checks the
// semantics the rewrite must preserve and the one it fixes: substring match, case
// insensitive, NULL columns excluded, and LIKE metacharacters in the term taken literally.
func TestHeaderSearchMatchesLiterally(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, accountID, mailboxID := setupSearchTestDatabase(t)
	defer db.Close()
	ctx := context.Background()

	bs := imap.BodyStructure(&imap.BodyStructureSinglePart{Type: "text", Subtype: "plain", Size: 100})
	bsBytes, err := helpers.SerializeBodyStructureGob(&bs)
	require.NoError(t, err)

	// insertMsg writes the already-lowercased sort columns directly, as delivery does.
	insertMsg := func(uid int64, subject, fromName, fromEmail string) {
		var fromNameArg any = fromName
		if fromName == "" {
			fromNameArg = nil
		}
		_, err := db.GetWritePool().Exec(ctx, `
			WITH inserted AS (
				INSERT INTO messages
				(account_id, mailbox_id, mailbox_path, uid, message_id, in_reply_to, content_hash, s3_domain, s3_localpart,
				 internal_date, size, subject, sent_date, body_structure, recipients_json, created_modseq,
				 subject_sort, from_name_sort, from_email_sort, to_name_sort, to_email_sort, cc_email_sort)
				VALUES ($1,$2,'INBOX',$3,$4,'',$5,'d',$6, now(), 100, $7, now(), $8, '[]'::jsonb, nextval('messages_modseq'),
				        $9, $10, $11, '', '', '')
				RETURNING id, mailbox_id
			)
			INSERT INTO message_state (message_id, mailbox_id, flags, custom_flags)
			SELECT id, mailbox_id, 0, '[]'::jsonb FROM inserted`,
			accountID, mailboxID, uid, fmt.Sprintf("<%d@test>", uid), fmt.Sprintf("hash-%d", uid),
			fmt.Sprintf("lp-%d", uid), subject, bsBytes, normalizeForSort(subject), fromNameArg, fromEmail)
		require.NoError(t, err)
	}

	insertMsg(1, "Quarterly report", "equishark support", "support@equishark.com")
	insertMsg(2, "100% off everything", "", "promo@shop.example") // NULL from_name_sort
	insertMsg(3, "Re: order_42 shipped", "ann marie", "ann_marie@example.org")
	insertMsg(4, "nothing relevant", "bob", "bob@example.org")

	search := func(t *testing.T, c *imap.SearchCriteria) []imap.UID {
		t.Helper()
		res, err := db.SearchMessagesWithCriteria(ctx, mailboxID, accountID, c, 0, 4)
		require.NoError(t, err)
		var uids []imap.UID
		for _, r := range res {
			uids = append(uids, r.UID)
		}
		return uids
	}
	header := func(key, value string) *imap.SearchCriteria {
		return &imap.SearchCriteria{Header: []imap.SearchCriteriaHeaderField{{Key: key, Value: value}}}
	}

	t.Run("substring and case-insensitive", func(t *testing.T) {
		assert.ElementsMatch(t, []imap.UID{1}, search(t, header("From", "Support@EQUISHARK.com")))
		assert.ElementsMatch(t, []imap.UID{1}, search(t, header("From", "equishark")), "matches the display name too")
		assert.ElementsMatch(t, []imap.UID{1}, search(t, header("Subject", "REPORT")), "subject substring, case-folded")
		assert.ElementsMatch(t, []imap.UID{3}, search(t, header("Subject", "re: ")))
	})

	t.Run("LIKE metacharacters are literal", func(t *testing.T) {
		// With LIKE these were wildcards: "%" matched every message, "_" any one character.
		assert.ElementsMatch(t, []imap.UID{2}, search(t, header("Subject", "100%")))
		assert.ElementsMatch(t, []imap.UID{2}, search(t, header("Subject", "%")), "a lone % matches only the subject that literally contains one, not every message")
		assert.Empty(t, search(t, header("From", "%")), "no address contains a literal %")
		assert.ElementsMatch(t, []imap.UID{3}, search(t, header("From", "ann_marie")))
		assert.Empty(t, search(t, header("From", "ann_mari_")), "_ is not a single-character wildcard")
		assert.ElementsMatch(t, []imap.UID{3}, search(t, &imap.SearchCriteria{Text: []string{"order_42"}}))
		assert.Empty(t, search(t, &imap.SearchCriteria{Text: []string{"order_4_"}}))
	})

	t.Run("NULL header column is simply not a match", func(t *testing.T) {
		assert.ElementsMatch(t, []imap.UID{2}, search(t, header("From", "promo@shop")), "NULL from_name_sort must not hide the email match")
		assert.Empty(t, search(t, header("From", "zzznomatchzzz")))
	})
}
