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
// mailbox search) is written in one of two forms chosen by mailbox size (see headerMatch):
// strpos() for mailboxes up to headerScanMaxMailboxSize, LIKE ... ESCAPE above it.
//
// The form matters because LIKE on the subject and *_sort columns is served by the
// corpus-wide trigram GINs from migration 000034, whose planner cost estimate is wrong by
// three orders of magnitude: a production SEARCH FROM on a 777-message mailbox was planned
// through them at an estimated cost of 1106 and read 3.5 GB of GIN pages in 70 s, where a
// strpos filter of the mailbox's own rows took 744 pages and 0.26 s. On the largest
// mailbox (534k rows) the strpos scan read 1.8 GB of heap in 38 s while the trigram probe
// for a rare term took 0.45 s. These tests pin both forms and the choice between them.

// TestHeaderSearchLikeOnlyInsideHeaderMatch scans the source of the header-search builders
// for a LIKE token outside comments. The only permitted occurrence is the trigram branch of
// headerMatch.cond; a header predicate written as LIKE anywhere else would bypass the
// mailbox-size choice and hand small-mailbox searches back to the trigram plan.
func TestHeaderSearchLikeOnlyInsideHeaderMatch(t *testing.T) {
	likeToken := regexp.MustCompile(`\bLIKE\b`)
	for _, name := range []string{"search.go", "user_operations.go"} {
		src, err := os.ReadFile(name)
		require.NoError(t, err)
		inCond := false
		for i, line := range strings.Split(string(src), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "func (hm headerMatch) cond(") {
				inCond = true
			} else if inCond && trimmed == "}" {
				inCond = false
			}
			if inCond || strings.HasPrefix(trimmed, "//") || strings.HasPrefix(trimmed, "--") {
				continue
			}
			if likeToken.MatchString(line) {
				t.Errorf("%s:%d uses LIKE outside headerMatch.cond; header matching must go through headerMatch so the form is chosen by mailbox size: %s", name, i+1, trimmed)
			}
		}
	}
}

// TestHeaderMatchForThreshold pins the mailbox-size switch between the two forms.
func TestHeaderMatchForThreshold(t *testing.T) {
	assert.Equal(t, headerMatchScan, headerMatchFor(0))
	assert.Equal(t, headerMatchScan, headerMatchFor(777), "the mailbox from the production incident must scan")
	assert.Equal(t, headerMatchScan, headerMatchFor(headerScanMaxMailboxSize))
	assert.Equal(t, headerMatchTrigram, headerMatchFor(headerScanMaxMailboxSize+1))
	assert.Equal(t, headerMatchTrigram, headerMatchFor(534068), "the largest production mailbox must use the trigram form")
}

// TestHeaderSearchCriteriaBindLiteralTerms verifies, for every header criterion and both
// forms, that the SQL is the expected predicate and the bound value makes the match
// literal: the bare lowercased term for strpos, and for LIKE a wildcard-wrapped pattern
// with the term's own metacharacters escaped (the old code never escaped them, so a % or _
// in the term was a wildcard).
func TestHeaderSearchCriteriaBindLiteralTerms(t *testing.T) {
	var db *Database // the builder never dereferences its receiver

	header := func(key string) *imap.SearchCriteria {
		return &imap.SearchCriteria{Header: []imap.SearchCriteriaHeaderField{{Key: key, Value: "Support@Equishark.com"}}}
	}
	cases := []struct {
		name     string
		criteria *imap.SearchCriteria
		exprs    []string // lowercased expressions each form must test
	}{
		{"FROM", header("From"), []string{"m.from_email_sort", "m.from_name_sort"}},
		{"TO", header("To"), []string{"m.to_email_sort", "m.to_name_sort"}},
		{"CC", header("Cc"), []string{"m.cc_email_sort"}},
		{"SUBJECT", header("Subject"), []string{"LOWER(m.subject)"}},
		{"HEADER References", header("References"), []string{`LOWER(m."references")`}},
		{"TEXT", &imap.SearchCriteria{Text: []string{"Support@Equishark.com"}}, []string{"LOWER(m.subject)", "m.from_email_sort", "m.to_name_sort", "m.cc_email_sort"}},
	}
	forms := []struct {
		name    string
		hm      headerMatch
		sql     func(expr string) string
		bound   string
		notWant string
	}{
		{"scan", headerMatchScan, func(e string) string { return "strpos(" + e + ", @" }, "support@equishark.com", " LIKE "},
		{"trigram", headerMatchTrigram, func(e string) string { return e + " LIKE @" }, "%support@equishark.com%", "strpos("},
	}
	for _, form := range forms {
		for _, tc := range cases {
			t.Run(form.name+"/"+tc.name, func(t *testing.T) {
				paramCounter := 0
				query, args, err := db.buildSearchCriteriaWithPrefix(tc.criteria, "p", &paramCounter, "m", form.hm)
				require.NoError(t, err)
				for _, expr := range tc.exprs {
					assert.Contains(t, query, form.sql(expr))
				}
				assert.NotContains(t, query, form.notWant)
				if form.hm == headerMatchTrigram {
					assert.Contains(t, query, `ESCAPE '\'`, "LIKE form must declare its escape character")
				}
				var bound []string
				for _, v := range args {
					if s, ok := v.(string); ok {
						bound = append(bound, s)
					}
				}
				assert.Contains(t, bound, form.bound, "term must be bound lowercased in the form's literal shape")
			})
		}
	}

	t.Run("trigram form escapes LIKE metacharacters", func(t *testing.T) {
		assert.Equal(t, `%100\%\_x\\%`, headerMatchTrigram.bind(`100%_X\`))
		assert.Equal(t, `100%_x\`, headerMatchScan.bind(`100%_X\`), "strpos needs no escaping")
	})
}

// TestHeaderSearchMatchesLiterally runs header searches against PostgreSQL under both forms
// and checks the semantics they must share: substring match, case insensitive, NULL
// columns excluded, LIKE metacharacters in the term taken literally. The form is selected
// through the mailboxMessageCount argument the IMAP session passes, so the trigram form is
// exercised without a mailbox of headerScanMaxMailboxSize rows.
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
	insertMsg(4, `C:\path\file`, "bob", "bob@example.org")

	header := func(key, value string) *imap.SearchCriteria {
		return &imap.SearchCriteria{Header: []imap.SearchCriteriaHeaderField{{Key: key, Value: value}}}
	}

	forms := []struct {
		name         string
		mailboxCount int
	}{
		{"scan form (small mailbox)", 4},
		{"trigram form (large mailbox)", headerScanMaxMailboxSize + 1},
	}
	for _, form := range forms {
		t.Run(form.name, func(t *testing.T) {
			search := func(t *testing.T, c *imap.SearchCriteria) []imap.UID {
				t.Helper()
				res, err := db.SearchMessagesWithCriteria(ctx, mailboxID, accountID, c, 0, form.mailboxCount)
				require.NoError(t, err)
				var uids []imap.UID
				for _, r := range res {
					uids = append(uids, r.UID)
				}
				return uids
			}

			// Substring and case-insensitive.
			assert.ElementsMatch(t, []imap.UID{1}, search(t, header("From", "Support@EQUISHARK.com")))
			assert.ElementsMatch(t, []imap.UID{1}, search(t, header("From", "equishark")), "matches the display name too")
			assert.ElementsMatch(t, []imap.UID{1}, search(t, header("Subject", "REPORT")), "subject substring, case-folded")
			assert.ElementsMatch(t, []imap.UID{3}, search(t, header("Subject", "re: ")))

			// LIKE metacharacters are literal. Unescaped, "%" matched every message and "_"
			// any one character.
			assert.ElementsMatch(t, []imap.UID{2}, search(t, header("Subject", "100%")))
			assert.ElementsMatch(t, []imap.UID{2}, search(t, header("Subject", "%")), "a lone % matches only the subject that literally contains one")
			assert.Empty(t, search(t, header("From", "%")), "no address contains a literal %")
			assert.ElementsMatch(t, []imap.UID{3}, search(t, header("From", "ann_marie")))
			assert.Empty(t, search(t, header("From", "ann_mari_")), "_ is not a single-character wildcard")
			assert.ElementsMatch(t, []imap.UID{3}, search(t, &imap.SearchCriteria{Text: []string{"order_42"}}))
			assert.Empty(t, search(t, &imap.SearchCriteria{Text: []string{"order_4_"}}))
			assert.ElementsMatch(t, []imap.UID{4}, search(t, header("Subject", `c:\path`)), "a backslash in the term is literal")

			// A NULL header column is simply not a match and does not hide the other column.
			assert.ElementsMatch(t, []imap.UID{2}, search(t, header("From", "promo@shop")))
			assert.Empty(t, search(t, header("From", "zzznomatchzzz")))
		})
	}
}
