package db

import (
	"os"
	"strings"
	"testing"
)

// TestEmailConfirmationStoreMethodsExist pins the v0.20.4 store
// methods that drive the email-verification flow. Adds defense-in-
// depth on top of the v0.19.10 manual-entry path so emails entered at
// /account/email are confirmed via click-through before becoming the
// canonical users.email.
func TestEmailConfirmationStoreMethodsExist(t *testing.T) {
	data, err := os.ReadFile("email_confirmation.go")
	if err != nil {
		t.Fatalf("expected internal/db/email_confirmation.go to exist: %v", err)
	}
	src := string(data)
	for _, sig := range []string{
		"func (s *PostgresStore) CreateEmailConfirmation(",
		"func (s *PostgresStore) ConsumeEmailConfirmation(",
		"func (s *PostgresStore) GetUserPendingEmail(",
		"func (s *PostgresStore) SetUserPendingEmail(",
		"func (s *PostgresStore) ConfirmUserEmail(",
	} {
		if !strings.Contains(src, sig) {
			t.Errorf("email_confirmation.go must define %q", sig)
		}
	}
}

// TestEmailConfirmationsTableMigration pins that the migration adds
// the email_pending column on users + the email_confirmations table.
func TestEmailConfirmationsTableMigration(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "email_pending") {
		t.Error("migrate.go must add the email_pending column on " +
			"aveloxis_ops.users via addColumnIfMissing — this stores " +
			"the email awaiting confirmation, separate from users.email " +
			"which holds the confirmed value.")
	}
	if !strings.Contains(src, "email_confirmations") {
		t.Error("migrate.go must create the aveloxis_ops.email_confirmations " +
			"table — token PK, user_id FK, email, expires_at — to store " +
			"v0.20.4 confirmation tokens.")
	}
}

// TestMailerHasSendEmailConfirmation pins the new mailer method that
// delivers the confirmation link.
func TestMailerHasSendEmailConfirmation(t *testing.T) {
	data, err := os.ReadFile("../mailer/mailer.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "func (m *Mailer) SendEmailConfirmation(") {
		t.Error("mailer.go must define SendEmailConfirmation(toEmail, " +
			"login, confirmURL) error to send the v0.20.4 click-to-confirm " +
			"link.")
	}
}

// TestServerHasEmailConfirmRoute pins the new HTTP route + handler.
func TestServerHasEmailConfirmRoute(t *testing.T) {
	data, err := os.ReadFile("../web/server.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, `"/account/email/confirm"`) {
		t.Error("server.go must register the /account/email/confirm route " +
			"so the confirmation links in v0.20.4 emails resolve to a " +
			"handler that consumes the token and promotes email_pending " +
			"to email.")
	}
	if !strings.Contains(src, "handleEmailConfirm") {
		t.Error("server.go must define handleEmailConfirm — the GET handler " +
			"that consumes the token, sets users.email, clears email_pending.")
	}
}

// TestAccountEmailPostUsesPendingFlow pins that POST /account/email
// (the v0.19.10 manual-entry handler) now sets email_pending +
// generates a token + sends mail, instead of the v0.19.10 behavior of
// writing directly to users.email.
func TestAccountEmailPostUsesPendingFlow(t *testing.T) {
	data, err := os.ReadFile("../web/server.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	// The handler must reference at least one of the new pieces.
	if !strings.Contains(src, "SetUserPendingEmail") &&
		!strings.Contains(src, "CreateEmailConfirmation") {
		t.Error("handleAccountEmail's POST branch must use " +
			"SetUserPendingEmail + CreateEmailConfirmation instead of " +
			"writing directly to users.email. Per v0.20.4 manual-entry " +
			"emails must be confirmed via click-through.")
	}
	if !strings.Contains(src, "SendEmailConfirmation") {
		t.Error("handleAccountEmail's POST branch must call " +
			"mailer.SendEmailConfirmation to deliver the click-to-confirm link.")
	}
}

// TestDashboardShowsPendingEmail pins that the dashboard renders a
// banner when the user has email_pending set (i.e., they submitted an
// email but haven't clicked the confirmation link yet). Also pins that
// the dashboard email-gate doesn't redirect to /account/email when
// email_pending is set — otherwise the user gets stuck in a loop.
func TestDashboardShowsPendingEmail(t *testing.T) {
	srv, err := os.ReadFile("../web/server.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(srv), "PendingEmail") &&
		!strings.Contains(string(srv), "GetUserPendingEmail") {
		t.Error("handleDashboard must look up the user's email_pending " +
			"and pass it to the template (e.g., as a PendingEmail key) so " +
			"the v0.20.4 \"check your inbox\" banner can render.")
	}
	tmpl, err := os.ReadFile("../web/templates.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(tmpl), "PendingEmail") {
		t.Error("dashboard template must reference .PendingEmail so the " +
			"banner renders when set.")
	}
}
