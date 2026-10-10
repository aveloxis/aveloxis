# Sign-up address escrow

Aveloxis limits how many new accounts can be created from one network address
in a day. Counting needs the address only for that day, so the counting data is
a keyed hash whose key changes every UTC day and is deleted when the day ends.
After that, nobody can match a past day's hash to an address, including the
operator.

An operator may also need to answer a lawful request for the address an
account signed up from. For that case Aveloxis can keep each new account's
address **sealed** to a public key whose private half never touches the server.
The server can seal envelopes but cannot open them, and anyone holding a
database backup gets only sealed envelopes for the addresses themselves. Opening one takes the private key, an
offline machine, and the steps below.

This page covers setting it up once, opening an envelope when you are required
to, and changing or losing the key. It applies from v0.29.89.

## What is kept, and for how long

| Data | Kept | Who can read it |
|---|---|---|
| Address hash for the sign-up limit | Until the end of its UTC day; the day's key is deleted too, by the api process within a minute of the day ending | During the day, anyone who can read the database; afterwards, no one — except from a backup (below) |
| Sealed address (the full IPv4 or IPv6 address) | One year from sign-up, then deleted with the sign-up record | Only the holder of the private key |
| Sign-up record (account id, time) | One year | Administrators (the Capacity page's sign-ups per day) |

No envelope is written while `web.signup_escrow_recipient` is empty, and none
exists for accounts created before you set it. An envelope cannot be opened
once its year has passed: it has been deleted.

A backup is the one exception to "afterwards, no one". A database backup,
WAL archive or replica snapshot taken during a UTC day holds that day's secret
and that day's address hashes, and with both, that day's IPv4 sign-up
addresses can be recovered. Treat backups with the same care as the live
database, and expire them on a schedule; a backup older than your retention
needs keeps nothing the live database does not.

## Set it up (once)

1. **Prepare an offline machine.** Use a computer that is not connected to a
   network and copy the same `aveloxis` binary onto it as runs on the server.
   It needs no database and no configuration file. A laptop kept for this
   purpose is enough.

2. **Make the key pair on the offline machine.** The command writes the private
   key to the file you name, with permissions 600, and refuses to overwrite an
   existing file. It prints the public key.

   ```bash
   KEY_FILE=/media/escrow-usb/aveloxis-escrow-key.txt
   aveloxis signup-escrow keygen --out "$KEY_FILE"
   ```

3. **Put the private key in the safe.** The key file is one line of text
   beginning `AGE-SECRET-KEY-`. Keep two copies, for example the USB stick and
   a printout, in separate physically secure places. Record where they are and
   who may take them out. If you want two people to be needed, keep the copies
   under two-person control; the software does not enforce it.

4. **Give the server the public key.** Add the printed line under `"web"` in
   `aveloxis.json` on the server. The public key is not secret.

   ```json
   "signup_escrow_recipient": "age1..."
   ```

   Then restart the web process:

   ```bash
   aveloxis stop web && aveloxis start web
   ```

5. **Check that sealing is on.** The web log says so at start.

   ```bash
   grep "sign-up escrow" ~/.aveloxis/web.log | tail -n 1
   ```

   The line reads `sign-up escrow: each new account's address is sealed to the
   configured public key and kept for a year`, with the public key. If it reads
   `sign-up escrow: off`, the setting is empty or was not picked up. An invalid
   key stops the web process at start with an error naming the setting.

## Open an envelope when you are required to

Do these steps only when your institution or legal counsel has confirmed that a
request obliges you to disclose the address. Each step says why it is there.

1. **Record the request before you start.** Note the request's reference, who
   authorised the disclosure, the account it names, and the date. Aveloxis logs
   the export in step 2. It cannot log step 4, because that step happens
   offline, so your record is the only account of it.

2. **Export the account's envelopes on the server.** The export holds sealed
   envelopes only. Running it does not need the private key, and the file
   cannot be read without it. Name the account by login or by user id.

   ```bash
   ACCOUNT_LOGIN=the-login-named-in-the-request
   EXPORT_FILE=/tmp/aveloxis-escrow-export.json
   aveloxis signup-escrow export --login "$ACCOUNT_LOGIN" --out "$EXPORT_FILE"
   ```

   It reads the same `aveloxis.json` as the other commands (pass `-c` with its
   path if you run it from another directory). It prints the account's id,
   login and the forge of its last sign-in (GitHub, or GitLab and its instance):
   check them against the request before going on. It writes a new file only
   (mode 600) and refuses to overwrite one. A login that more than one account
   has, apart from letter case, is refused even if one matches exactly (GitHub
   and GitLab both ignore case in names, so case cannot tell the people
   apart). The refusal lists each such account's user id, login and forge:
   pick the one the request names and run the export again with `--user` and
   that id. It refuses if the account has
   no sealed record: the account signed up before sealing was set up, or more
   than a year ago. It prints the log line `sign-up escrow: sealed records
   exported for opening offline`, with the account id and the file; keep that
   line with your record.

3. **Carry the export file to the offline machine** on removable media. Then
   delete it from the server, so that sealed copies do not accumulate where
   they are not needed:

   ```bash
   rm "$EXPORT_FILE"
   ```

4. **Open it on the offline machine with the private key.** Take the key from
   the safe for this step only.

   ```bash
   KEY_FILE=/media/escrow-usb/aveloxis-escrow-key.txt
   EXPORT_FILE=/media/escrow-usb/aveloxis-escrow-export.json
   aveloxis signup-escrow open --identity "$KEY_FILE" "$EXPORT_FILE"
   ```

   It prints one line per sign-up record: the record id, the user id, the
   login, the sign-up time in UTC, and the address. If a record was sealed to a
   different key, that line says it cannot be opened, and the command exits
   with an error: see "Change the key" below.

5. **Disclose only what the request requires,** and add the result to your
   record from step 1.

6. **Put the key back and remove the working copies.** Return the key to the
   safe, and delete the export file and any copy of the output from the
   offline machine and the removable media.

## Change the key, or lose it

- **Change the key.** Make a new pair (set-up steps 2 to 5). New sign-ups are
  sealed to the new key. Envelopes sealed earlier open only with the old
  private key, so keep it in the safe for a year after the change, until the
  last of its envelopes has been deleted. `open` tries every key in the key
  file, so a file holding both keys, one per line, opens either kind.
- **Lose the private key.** Envelopes sealed to it can never be opened, by
  anyone. Make a new pair at once, so that later sign-ups can be opened.
- **Stop sealing.** Empty `web.signup_escrow_recipient` and restart the web
  process. Existing envelopes stay until their year has passed.
- **Roll back to a release older than 0.29.89.** Older releases never delete
  sign-up records, envelopes or the day's secret. Run the cleanup the deploy
  checklist gives right after rolling back, and know that the one-year
  deletion resumes only when a 0.29.89 or later api runs again.

## What an envelope does and does not show

An envelope shows the address that was sealed for that sign-up record. It
does not prove where the record came from: sealing needs only the public key,
so anyone able to write to the database could write an envelope for any row.
Treat an opened address as what Aveloxis recorded, alongside your other
evidence, not as proof on its own.

## What this does not cover

- A person who controls the running web server can see addresses as requests
  arrive, whatever is stored. Escrow protects what is stored and backed up.
- Whether to keep addresses at all, and whether to disclose one, are decisions
  for you and your institution's data-protection and legal advisers. The
  one-year retention is the operator's choice for this deployment.
