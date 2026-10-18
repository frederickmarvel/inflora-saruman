// Package ledger owns the vocabulary of ledger_transactions.source_tag
// (CONTRACT_DRIFT D-032). The tolkien balance `months` earnings split, the
// React EarningsSummary entity, and the CHECK constraint in migration
// 00012_ledger_source_tag all depend on these exact strings — change one and
// the other two must change with it.
package ledger

// Source tags for the assignable revenue streams behind ledger transactions.
// Every new money-moving writer must tag its transactions with one of these.
const (
	SourceDonation = "donation"
	SourceCampaign = "campaign"
	SourceClip     = "clip"
)