// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

// v0.29.89 (operator, 2026-10-10) — `aveloxis signup-escrow`: the three
// steps of the sign-up address escrow (internal/db/signup_escrow.go; the
// runbook is docs/guide/signup-escrow.md).
//
//   keygen  makes the key pair on the OFFLINE machine: the public key goes in
//           aveloxis.json (web.signup_escrow_recipient), the private key into
//           the safe. No database.
//   export  runs on the server: one account's sealed records to a file. No
//           key — the envelopes stay sealed. Logged.
//   open    runs on the OFFLINE machine with the private key: decrypts an
//           export file. No database, no network.

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"filippo.io/age"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/spf13/cobra"
)

func signupEscrowCmd(cfgPath *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "signup-escrow",
		Short: "Sealed sign-up addresses: make the key pair, export sealed records, open them offline",
		Long: `New accounts' network addresses are kept only sealed to the operator's
offline public key (web.signup_escrow_recipient), for one year, so that they
can be opened if a legal requirement demands it. The server can seal but never
open them. See docs/guide/signup-escrow.md for the whole procedure.`,
	}
	cmd.AddCommand(signupEscrowKeygenCmd(), signupEscrowExportCmd(cfgPath), signupEscrowOpenCmd())
	return cmd
}

func signupEscrowKeygenCmd() *cobra.Command {
	var out string
	cmd := &cobra.Command{
		Use:   "keygen --out <identity file>",
		Short: "Make the escrow key pair (run on the offline machine)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if out == "" {
				return errors.New("--out is required: where to write the private key (never on the server)")
			}
			id, err := age.GenerateX25519Identity()
			if err != nil {
				return fmt.Errorf("generate the key pair: %w", err)
			}
			f, err := os.OpenFile(out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) // never overwrites a key
			if err != nil {
				return fmt.Errorf("write %s: %w", out, err)
			}
			body := fmt.Sprintf("# Aveloxis sign-up escrow private key. Keep offline; never on the server.\n# public key: %s\n%s\n", id.Recipient(), id)
			if _, err := io.WriteString(f, body); err != nil {
				_ = f.Close()
				return fmt.Errorf("write %s: %w", out, err)
			}
			if err := f.Close(); err != nil {
				return fmt.Errorf("write %s: %w", out, err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Private key written to %s (mode 600). Keep it offline.\n\nPut this public key in aveloxis.json on the server, under \"web\":\n\n  \"signup_escrow_recipient\": %q\n", out, id.Recipient().String())
			return nil
		},
	}
	cmd.Flags().StringVar(&out, "out", "", "file to write the private key to (refuses to overwrite)")
	return cmd
}

func signupEscrowExportCmd(cfgPath *string) *cobra.Command {
	var userID int
	var login, out string
	cmd := &cobra.Command{
		Use:   "export (--user <id> | --login <login>) --out <file>",
		Short: "Write one account's sealed sign-up records to a file (no key; run on the server)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if (userID == 0) == (strings.TrimSpace(login) == "") {
				return errors.New("name exactly one account: --user <id> or --login <login>")
			}
			if out == "" {
				return errors.New("--out is required")
			}
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)
			logger := newLogger(cfg)
			ctx := cmd.Context()
			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
			if err != nil {
				return fmt.Errorf("connecting to database: %w", err)
			}
			defer store.Close()
			// The account, resolved and shown with its forge before anything
			// is exported: check it against the request (round 4 R4-1). A
			// login matching more than one account apart from letter case is
			// refused — name it by --user.
			var a db.CapacityAccount
			if userID != 0 {
				a, err = store.AccountByID(ctx, userID)
			} else {
				a, err = store.AccountByLogin(ctx, login)
			}
			if err != nil {
				return fmt.Errorf("account: %w", err)
			}
			userID = a.UserID
			fmt.Fprintf(cmd.OutOrStdout(), "Account: user id %d, login %s, last signed in with %s.\n", a.UserID, a.Login, a.Forge())
			recs, err := store.SealedSignupsForUser(ctx, userID)
			if err != nil {
				return err
			}
			if len(recs) == 0 {
				return fmt.Errorf("account %d has no sealed sign-up record (none was sealed, or it is older than the retention)", userID)
			}
			b, err := json.MarshalIndent(recs, "", "  ")
			if err != nil {
				return err
			}
			// A new file only (mode 600): never written over an existing one.
			f, err := os.OpenFile(out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
			if err != nil {
				return fmt.Errorf("write %s: %w", out, err)
			}
			if _, err := f.Write(append(b, '\n')); err != nil {
				_ = f.Close()
				return fmt.Errorf("write %s: %w", out, err)
			}
			if err := f.Close(); err != nil {
				return fmt.Errorf("write %s: %w", out, err)
			}
			// The audit line: who was exported, when (the OS user is in the
			// shell's own history; the open step happens offline).
			logger.Warn("sign-up escrow: sealed records exported for opening offline", "user_id", userID, "records", len(recs), "file", out)
			fmt.Fprintf(cmd.OutOrStdout(), "%d sealed record(s) for account %d written to %s. Carry it to the offline machine and run:\n  aveloxis signup-escrow open --identity <private key file> %s\n", len(recs), userID, out, out)
			return nil
		},
	}
	cmd.Flags().IntVar(&userID, "user", 0, "account id")
	cmd.Flags().StringVar(&login, "login", "", "account login")
	cmd.Flags().StringVar(&out, "out", "", "file to write")
	return cmd
}

func signupEscrowOpenCmd() *cobra.Command {
	var identityPath string
	cmd := &cobra.Command{
		Use:   "open --identity <private key file> <export file>",
		Short: "Decrypt an export file with the private key (run on the offline machine)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if identityPath == "" {
				return errors.New("--identity is required: the private key file from keygen")
			}
			kf, err := os.Open(identityPath)
			if err != nil {
				return fmt.Errorf("read the private key: %w", err)
			}
			ids, err := age.ParseIdentities(kf)
			_ = kf.Close()
			if err != nil {
				return fmt.Errorf("read the private key: %w", err)
			}
			raw, err := os.ReadFile(args[0])
			if err != nil {
				return fmt.Errorf("read %s: %w", args[0], err)
			}
			var recs []db.SealedSignup
			if err := json.Unmarshal(raw, &recs); err != nil {
				return fmt.Errorf("%s is not an export file: %w", args[0], err)
			}
			if len(ids) == 0 {
				return errors.New("the private key file holds no key")
			}
			w := cmd.OutOrStdout()
			fmt.Fprintln(w, "signup_id\tuser_id\tlogin\tcreated_at (UTC)\taddress")
			failed := 0
			for _, r := range recs {
				addr, err := db.OpenSignupAddress(r.Sealed, ids...) // every key in the file (an old and a new one)
				if err != nil {
					failed++
					fmt.Fprintf(w, "%d\t%d\t%s\t%s\tcannot open: %v\n", r.SignupID, r.UserID, r.Login, r.CreatedAt.UTC().Format("2006-01-02 15:04:05"), err)
					continue
				}
				fmt.Fprintf(w, "%d\t%d\t%s\t%s\t%s\n", r.SignupID, r.UserID, r.Login, r.CreatedAt.UTC().Format("2006-01-02 15:04:05"), addr)
			}
			if failed > 0 {
				return errors.New(strconv.Itoa(failed) + " record(s) could not be opened with this key (sealed to a different key?)")
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&identityPath, "identity", "", "the private key file from keygen")
	return cmd
}
