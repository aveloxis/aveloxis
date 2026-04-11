package db

import (
	"context"
	"strings"
)

// affiliationCandidate is a contributor with email and company data,
// used to build the domain→organization mapping for contributor_affiliations.
type affiliationCandidate struct {
	Email   string
	Company string
}

// PopulateAffiliations auto-populates the contributor_affiliations table by:
// 1. Querying all contributors with both an email and a company
// 2. Extracting email domains and mapping them to the contributor's company
// 3. Using consensus (most common company per domain) for domains shared by
//    multiple contributors
// 4. Upserting into contributor_affiliations
//
// This is called after contributor enrichment so that company data from
// GitHub/GitLab user profiles is available. Only corporate/institutional
// email domains are populated — public providers (gmail, yahoo, etc.) are
// excluded since they don't represent organizational affiliation.
func (s *PostgresStore) PopulateAffiliations(ctx context.Context) (int, error) {
	// Fetch all contributors with email + company.
	rows, err := s.pool.Query(ctx, `
		SELECT cntrb_email, cntrb_company
		FROM aveloxis_data.contributors
		WHERE cntrb_email != '' AND cntrb_company != '' AND cntrb_company != 'None'
		  AND cntrb_email NOT LIKE '%noreply%'`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var candidates []affiliationCandidate
	for rows.Next() {
		var c affiliationCandidate
		if err := rows.Scan(&c.Email, &c.Company); err != nil {
			continue
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	// Also gather unique domains from commit author emails that have a matching
	// contributor with company data. This picks up domains seen only in git log.
	commitRows, err := s.pool.Query(ctx, `
		SELECT DISTINCT c.cmt_author_email, co.cntrb_company
		FROM aveloxis_data.commits c
		JOIN aveloxis_data.contributors co ON LOWER(c.cmt_author_email) = LOWER(co.cntrb_email)
		WHERE c.cmt_author_email != '' AND co.cntrb_company != '' AND co.cntrb_company != 'None'
		  AND c.cmt_author_email NOT LIKE '%noreply%'`)
	if err == nil {
		defer commitRows.Close()
		for commitRows.Next() {
			var c affiliationCandidate
			if err := commitRows.Scan(&c.Email, &c.Company); err != nil {
				continue
			}
			candidates = append(candidates, c)
		}
	}

	affMap := buildAffiliationMap(candidates)

	// Upsert into contributor_affiliations.
	count := 0
	for domain, company := range affMap {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.contributor_affiliations
				(ca_domain, ca_affiliation, ca_active, ca_last_used,
				 tool_source, data_source, data_collection_date)
			VALUES ($1, $2, 1, NOW(), 'aveloxis', 'auto-populated from contributor profiles', NOW())
			ON CONFLICT (ca_domain) DO UPDATE SET
				ca_affiliation = EXCLUDED.ca_affiliation,
				ca_last_used = NOW()`,
			domain, company)
		if err != nil {
			continue
		}
		count++
	}
	return count, nil
}

// buildAffiliationMap builds a domain→company mapping from contributor data.
// Uses consensus: when multiple contributors share a domain, the most common
// company name wins. Public email domains (gmail, yahoo, etc.) are excluded.
func buildAffiliationMap(candidates []affiliationCandidate) map[string]string {
	// Count domain→company occurrences.
	type domainCompany struct {
		company string
		count   int
	}
	domainCounts := make(map[string]map[string]int) // domain -> company -> count

	for _, c := range candidates {
		domain := extractDomain(c.Email)
		if domain == "" || isPublicEmailDomain(domain) {
			continue
		}
		company := normalizeCompany(c.Company)
		if company == "" {
			continue
		}
		if domainCounts[domain] == nil {
			domainCounts[domain] = make(map[string]int)
		}
		domainCounts[domain][company]++
	}

	// Pick the most common company per domain.
	result := make(map[string]string)
	for domain, companies := range domainCounts {
		best := ""
		bestCount := 0
		for company, count := range companies {
			if count > bestCount {
				best = company
				bestCount = count
			}
		}
		if best != "" {
			result[domain] = best
		}
	}
	return result
}

// normalizeCompany cleans up company names from GitHub/GitLab profiles.
// - Strips leading @ (GitHub org references: "@microsoft" → "Microsoft")
// - Takes only the first org if multiple are listed ("@esnet @chaoss" → "esnet")
// - Strips "None", "N/A", etc.
func normalizeCompany(company string) string {
	company = strings.TrimSpace(company)
	if company == "" {
		return ""
	}

	// Strip sentinel values.
	lower := strings.ToLower(company)
	if lower == "none" || lower == "n/a" || lower == "null" {
		return ""
	}

	// Handle "@org" references — take the first one.
	if strings.HasPrefix(company, "@") {
		parts := strings.Fields(company)
		if len(parts) > 0 {
			org := strings.TrimPrefix(parts[0], "@")
			// Capitalize first letter.
			if len(org) > 0 {
				return strings.ToUpper(org[:1]) + org[1:]
			}
		}
		return ""
	}

	return company
}

// isPublicEmailDomain returns true for common free email providers whose domains
// don't indicate organizational affiliation.
func isPublicEmailDomain(domain string) bool {
	domain = strings.ToLower(domain)
	return publicEmailDomains[domain]
}

var publicEmailDomains = map[string]bool{
	"gmail.com":       true,
	"yahoo.com":       true,
	"yahoo.co.in":     true,
	"yahoo.co.uk":     true,
	"hotmail.com":     true,
	"outlook.com":     true,
	"live.com":        true,
	"msn.com":         true,
	"protonmail.com":  true,
	"proton.me":       true,
	"icloud.com":      true,
	"me.com":          true,
	"aol.com":         true,
	"mail.com":        true,
	"zoho.com":        true,
	"yandex.ru":       true,
	"yandex.com":      true,
	"qq.com":          true,
	"163.com":         true,
	"126.com":         true,
	"sina.com":        true,
	"foxmail.com":     true,
	"gmx.com":        true,
	"gmx.de":         true,
	"web.de":         true,
	"mail.ru":        true,
	"pm.me":          true,
	"tutanota.com":   true,
	"fastmail.com":   true,
	"users.noreply.github.com": true,
}
