package service

import "testing"

// The fixtures below are ports of inflora-front's own test tables
// (src/models/rules/moderation.test.ts + donationReview.test.ts) plus the
// boundary cases pinned in CONTRACT_DRIFT D-038. If a client fixture changes,
// these cases must change with it — they define the parity contract.

func TestModerateMessagePassesOrdinaryChat(t *testing.T) {
	t.Parallel()
	// moderation.test.ts: 'passes ordinary chat'
	if got := ModerateMessage("That clutch was insane, gg!"); got.Flagged {
		t.Fatalf("clean chat flagged: %+v", got)
	}
}

func TestModerateMessageFlagsLinksPhrasesCapsSpam(t *testing.T) {
	t.Parallel()
	// moderation.test.ts: 'flags links, blocked phrases, shouting and spam'
	cases := []struct {
		text, want string
	}{
		{"check www.example.com", "Contains a link"},
		{"free money here", "Blocked phrase"},
		{"WHY IS NOBODY LISTENING", "All caps"},
		{"lolllllllllll", "Spam pattern"},
	}
	for _, tc := range cases {
		got := ModerateMessage(tc.text)
		if !got.Flagged || got.Reason != tc.want {
			t.Errorf("ModerateMessage(%q) = %+v, want flagged with %q", tc.text, got, tc.want)
		}
	}
}

func TestModerateMessageLinkPrecedesPhrases(t *testing.T) {
	t.Parallel()
	// donationReview.test.ts: 'free money at www.scam.xyz' -> 'Contains a link'
	// (link rule wins over the blocked phrase, matching the client's order).
	if got := ModerateMessage("free money at www.scam.xyz"); !got.Flagged || got.Reason != "Contains a link" {
		t.Fatalf("link precedence: %+v", got)
	}
}

func TestModerateMessageBlockedPhraseMatch(t *testing.T) {
	t.Parallel()
	// moderation.test.ts: 'holds flagged messages even with auto-approve on'
	if got := ModerateMessage("dm me for a deal"); !got.Flagged || got.Reason != "Blocked phrase" {
		t.Fatalf("blocked phrase: %+v", got)
	}
}

func TestModerateMessageCapsBoundary(t *testing.T) {
	t.Parallel()
	// D-038 pins the caps boundary at >= 12 letters on the letters-only
	// extraction; repetition in a non-caps string must not trip caps.
	if got := ModerateMessage("ABCDEFGHIJK"); got.Flagged {
		t.Fatalf("11 caps letters flagged: %+v", got)
	}
	if got := ModerateMessage("ABCDEFGHIJKL"); !got.Flagged || got.Reason != "All caps" {
		t.Fatalf("12 caps letters: %+v", got)
	}
}

func TestModerateMessageRepeatRunBoundary(t *testing.T) {
	t.Parallel()
	// D-038: run >= 8 identical UTF-16 code units, raw text, no case folding.
	// The alternating-case string passes (client (.)\1{7,} is case-sensitive);
	// a real 8-run flags as spam even though it is too short for the caps rule.
	if got := ModerateMessage("aAaAaAaA"); got.Flagged {
		t.Fatalf("alternating case flagged: %+v", got)
	}
	if got := ModerateMessage("AAAAAAAAAAA"); !got.Flagged || got.Reason != "Spam pattern" {
		t.Fatalf("run of 11 A: %+v", got)
	}
	if got := ModerateMessage("heyyyyyyyy"); !got.Flagged || got.Reason != "Spam pattern" {
		t.Fatalf("run of 8 y: %+v", got)
	}
	if got := ModerateMessage("heyyyyyyy"); got.Flagged {
		t.Fatalf("run of 7 y flagged: %+v", got)
	}
}

// TestModerateMessageRepeatRuleJSRegexParity pins the exact JS (.)\1{7,}
// semantics: code-unit scan with no 'u' flag. BMP repetitions flag; astral
// emoji (two alternating surrogate units) do not; line terminators break the
// run exactly like the '.' wildcard that never crosses them.
func TestModerateMessageRepeatRuleJSRegexParity(t *testing.T) {
	t.Parallel()
	if got := ModerateMessage("❤❤❤❤❤❤❤❤"); !got.Flagged || got.Reason != "Spam pattern" {
		t.Fatalf("8x U+2764 BMP repetition: %+v", got)
	}
	if got := ModerateMessage("éééééééé"); !got.Flagged || got.Reason != "Spam pattern" {
		t.Fatalf("8x e-acute repetition: %+v", got)
	}
	if got := ModerateMessage("\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600"); got.Flagged {
		t.Fatalf("8x astral emoji flagged (JS would not): %+v", got)
	}
	if got := ModerateMessage("aaaa\naaaa"); got.Flagged {
		t.Fatalf("line-feed-separated repetition flagged: %+v", got)
	}
	if got := ModerateMessage("aaaa\U00002028aaaa"); got.Flagged {
		t.Fatalf("U+2028-separated repetition flagged: %+v", got)
	}
	if got := ModerateMessage("\U0001F600aaaa\U0001F600"); got.Flagged {
		t.Fatalf("interleaved astral/latin run flagged: %+v", got)
	}
}

func TestModerateMessageWhitespaceAndEmpty(t *testing.T) {
	t.Parallel()
	for _, text := range []string{"", "   ", "\t\n"} {
		if got := ModerateMessage(text); got.Flagged {
			t.Errorf("ModerateMessage(%q) flagged: %+v", text, got)
		}
	}
}

func TestFlagReasonForMessage(t *testing.T) {
	t.Parallel()
	if got := FlagReasonForMessage("hello there"); got != "" {
		t.Fatalf("clean message reason = %q", got)
	}
	if got := FlagReasonForMessage(""); got != "" {
		t.Fatalf("empty message reason = %q", got)
	}
	if got := FlagReasonForMessage("click here now"); got != "Blocked phrase" {
		t.Fatalf("flagged message reason = %q", got)
	}
}