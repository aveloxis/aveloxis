// Package collector — sbom.go generates CycloneDX and SPDX Software Bill of
// Materials from the dependency and libyear data collected for a repository.
package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/augurlabs/aveloxis/internal/db"
	"github.com/google/uuid"
)

// SBOMFormat specifies the output format.
type SBOMFormat string

const (
	FormatCycloneDX SBOMFormat = "cyclonedx"
	FormatSPDX      SBOMFormat = "spdx"
)

// GenerateSBOM creates an SBOM for a repository from its collected dependency
// data and ScanCode source code analysis. ScanCode provides:
//   - Concluded license: aggregated SPDX expression from file-level detections
//   - Copyright holders: extracted from source file headers
//
// If ScanCode data is not available (tool not installed, or no scan yet),
// the SBOM is still generated with registry-only license data.
func GenerateSBOM(ctx context.Context, store *db.PostgresStore, repoID int64, format SBOMFormat) ([]byte, error) {
	repo, err := store.GetRepoForSBOM(ctx, repoID)
	if err != nil {
		return nil, fmt.Errorf("repo %d not found: %w", repoID, err)
	}

	deps, err := store.GetRepoLibyearDeps(ctx, repoID)
	if err != nil {
		return nil, fmt.Errorf("loading dependencies: %w", err)
	}

	// ScanCode enrichment: concluded license + copyrights from source analysis.
	// Non-fatal — if no scancode data exists, we proceed without it.
	scanData, _ := store.GetScancodeForSBOM(ctx, repoID)

	switch format {
	case FormatCycloneDX:
		return generateCycloneDX(repo, deps, scanData)
	case FormatSPDX:
		return generateSPDX(repo, deps, scanData)
	default:
		return nil, fmt.Errorf("unknown format: %s", format)
	}
}

// ============================================================
// CycloneDX 1.5
// ============================================================

type cycloneDX struct {
	BOMFormat    string           `json:"bomFormat"`
	SpecVersion  string           `json:"specVersion"`
	SerialNumber string           `json:"serialNumber"`
	Version      int              `json:"version"`
	Metadata     cdxMetadata      `json:"metadata"`
	Components   []cdxComponent   `json:"components"`
}

type cdxMetadata struct {
	Timestamp string      `json:"timestamp"`
	Tools     []cdxTool   `json:"tools"`
	Component *cdxComponent `json:"component,omitempty"`
}

type cdxTool struct {
	Vendor  string `json:"vendor"`
	Name    string `json:"name"`
	Version string `json:"version"`
}

type cdxComponent struct {
	Type       string       `json:"type"`
	Name       string       `json:"name"`
	Version    string       `json:"version,omitempty"`
	Purl       string       `json:"purl,omitempty"`
	BOMRef     string       `json:"bom-ref,omitempty"`
	Licenses   []cdxLicense `json:"licenses,omitempty"`
	Scope      string       `json:"scope,omitempty"`
	Copyright  string       `json:"copyright,omitempty"`
	Evidence   *cdxEvidence `json:"evidence,omitempty"`
}

type cdxLicense struct {
	License struct {
		ID   string `json:"id,omitempty"`
		Name string `json:"name,omitempty"`
	} `json:"license"`
}

// cdxEvidence holds CycloneDX 1.5 evidence for concluded (detected) data.
// Used to distinguish source-code-detected licenses from registry-declared ones.
type cdxEvidence struct {
	Licenses  []cdxLicense `json:"licenses,omitempty"`
	Copyright []cdxCopyrightEvidence `json:"copyright,omitempty"`
}

type cdxCopyrightEvidence struct {
	Text string `json:"text"`
}

func generateCycloneDX(repo *db.RepoForSBOM, deps []db.SBOMDep, scanData *db.ScancodeForSBOM) ([]byte, error) {
	rootComp := &cdxComponent{
		Type: "application",
		Name: repo.Name,
	}

	// Enrich root component with ScanCode data if available.
	if scanData != nil {
		if scanData.ConcludedLicenseSPDX != "" {
			rootComp.Evidence = &cdxEvidence{
				Licenses: []cdxLicense{{
					License: struct {
						ID   string `json:"id,omitempty"`
						Name string `json:"name,omitempty"`
					}{Name: scanData.ConcludedLicenseSPDX},
				}},
			}
		}
		if len(scanData.Copyrights) > 0 {
			if rootComp.Evidence == nil {
				rootComp.Evidence = &cdxEvidence{}
			}
			for _, c := range scanData.Copyrights {
				rootComp.Evidence.Copyright = append(rootComp.Evidence.Copyright,
					cdxCopyrightEvidence{Text: c})
			}
			// Also set the top-level copyright field with the first holder.
			rootComp.Copyright = scanData.Copyrights[0]
			if len(scanData.Copyrights) > 1 {
				rootComp.Copyright += fmt.Sprintf(" (and %d others)", len(scanData.Copyrights)-1)
			}
		}
	}

	bom := cycloneDX{
		BOMFormat:    "CycloneDX",
		SpecVersion:  "1.5",
		SerialNumber: "urn:uuid:" + uuid.New().String(),
		Version:      1,
		Metadata: cdxMetadata{
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Tools: []cdxTool{{
				Vendor:  "Augur Labs",
				Name:    "aveloxis",
				Version: db.ToolVersion,
			}},
			Component: rootComp,
		},
	}

	for _, dep := range deps {
		comp := cdxComponent{
			Type:    "library",
			Name:    dep.Name,
			Version: dep.CurrentVersion,
			Purl:    dep.Purl,
			BOMRef:  dep.Purl,
		}
		if dep.Type == "dev" {
			comp.Scope = "optional"
		} else {
			comp.Scope = "required"
		}
		if dep.License != "" {
			comp.Licenses = []cdxLicense{{
				License: struct {
					ID   string `json:"id,omitempty"`
					Name string `json:"name,omitempty"`
				}{Name: dep.License},
			}}
		}
		bom.Components = append(bom.Components, comp)
	}

	return json.MarshalIndent(bom, "", "  ")
}

// ============================================================
// SPDX 2.3
// ============================================================

type spdxDoc struct {
	SPDXVersion       string         `json:"spdxVersion"`
	DataLicense       string         `json:"dataLicense"`
	SPDXID            string         `json:"SPDXID"`
	Name              string         `json:"name"`
	DocumentNamespace string         `json:"documentNamespace"`
	CreationInfo      spdxCreation   `json:"creationInfo"`
	Packages          []spdxPackage  `json:"packages"`
	Relationships     []spdxRelation `json:"relationships"`
}

type spdxCreation struct {
	Created  string   `json:"created"`
	Creators []string `json:"creators"`
}

type spdxPackage struct {
	SPDXID           string            `json:"SPDXID"`
	Name             string            `json:"name"`
	VersionInfo      string            `json:"versionInfo,omitempty"`
	DownloadLocation string            `json:"downloadLocation"`
	LicenseConcluded string            `json:"licenseConcluded"`
	LicenseDeclared  string            `json:"licenseDeclared"`
	CopyrightText    string            `json:"copyrightText,omitempty"`
	ExternalRefs     []spdxExternalRef `json:"externalRefs,omitempty"`
}

type spdxExternalRef struct {
	ReferenceCategory string `json:"referenceCategory"`
	ReferenceType     string `json:"referenceType"`
	ReferenceLocator  string `json:"referenceLocator"`
}

type spdxRelation struct {
	SpdxElementId      string `json:"spdxElementId"`
	RelationshipType   string `json:"relationshipType"`
	RelatedSpdxElement string `json:"relatedSpdxElement"`
}

func generateSPDX(repo *db.RepoForSBOM, deps []db.SBOMDep, scanData *db.ScancodeForSBOM) ([]byte, error) {
	doc := spdxDoc{
		SPDXVersion:       "SPDX-2.3",
		DataLicense:       "CC0-1.0",
		SPDXID:            "SPDXRef-DOCUMENT",
		Name:              repo.Name,
		DocumentNamespace: fmt.Sprintf("https://aveloxis.io/spdx/%s/%s", repo.Owner, repo.Name),
		CreationInfo: spdxCreation{
			Created:  time.Now().UTC().Format(time.RFC3339),
			Creators: []string{"Tool: aveloxis-" + db.ToolVersion},
		},
	}

	// Root package for the repo itself.
	// LicenseDeclared = from GitHub/GitLab API (what the repo claims).
	// LicenseConcluded = from ScanCode source analysis (what's actually detected).
	concludedLicense := orNoAssertion(repo.License)
	copyrightText := "NOASSERTION"
	if scanData != nil {
		if scanData.ConcludedLicenseSPDX != "" {
			concludedLicense = scanData.ConcludedLicenseSPDX
		}
		if len(scanData.Copyrights) > 0 {
			copyrightText = strings.Join(scanData.Copyrights, "\n")
		}
	}
	rootPkg := spdxPackage{
		SPDXID:           "SPDXRef-RootPackage",
		Name:             repo.Name,
		DownloadLocation: repo.GitURL,
		LicenseConcluded: concludedLicense,
		LicenseDeclared:  orNoAssertion(repo.License),
		CopyrightText:    copyrightText,
	}
	doc.Packages = append(doc.Packages, rootPkg)

	for i, dep := range deps {
		pkgID := fmt.Sprintf("SPDXRef-Package-%d", i+1)
		license := orNoAssertion(dep.License)

		pkg := spdxPackage{
			SPDXID:           pkgID,
			Name:             dep.Name,
			VersionInfo:      dep.CurrentVersion,
			DownloadLocation: "NOASSERTION",
			LicenseConcluded: license,
			LicenseDeclared:  license,
		}
		if dep.Purl != "" {
			pkg.ExternalRefs = []spdxExternalRef{{
				ReferenceCategory: "PACKAGE-MANAGER",
				ReferenceType:     "purl",
				ReferenceLocator:  dep.Purl,
			}}
			pkg.DownloadLocation = dep.Purl
		}
		doc.Packages = append(doc.Packages, pkg)

		doc.Relationships = append(doc.Relationships, spdxRelation{
			SpdxElementId:      "SPDXRef-RootPackage",
			RelationshipType:   "DEPENDS_ON",
			RelatedSpdxElement: pkgID,
		})
	}

	// Document describes root package.
	doc.Relationships = append(doc.Relationships, spdxRelation{
		SpdxElementId:      "SPDXRef-DOCUMENT",
		RelationshipType:   "DESCRIBES",
		RelatedSpdxElement: "SPDXRef-RootPackage",
	})

	return json.MarshalIndent(doc, "", "  ")
}

func orNoAssertion(s string) string {
	if s == "" {
		return "NOASSERTION"
	}
	return s
}

// StoreSBOM saves the generated SBOM JSON to repo_sbom_scans.
func StoreSBOM(ctx context.Context, store *db.PostgresStore, repoID int64, sbomJSON []byte) error {
	return store.InsertSBOM(ctx, repoID, sbomJSON)
}

// GenerateAndStoreSBOMs generates both CycloneDX and SPDX SBOMs for a repo
// and stores them in the database. Called at the end of each collection run.
// Errors are non-fatal — if SBOM generation fails, collection still succeeds.
func GenerateAndStoreSBOMs(ctx context.Context, store *db.PostgresStore, repoID int64, logger *slog.Logger) {
	for _, spec := range []struct {
		format  SBOMFormat
		name    string
		version string
	}{
		{FormatCycloneDX, "cyclonedx", "1.5"},
		{FormatSPDX, "spdx", "2.3"},
	} {
		data, err := GenerateSBOM(ctx, store, repoID, spec.format)
		if err != nil {
			logger.Debug("SBOM generation skipped", "repo_id", repoID, "format", spec.name, "error", err)
			continue
		}
		if err := store.InsertSBOMWithFormat(ctx, repoID, data, spec.name, spec.version); err != nil {
			logger.Warn("failed to store SBOM", "repo_id", repoID, "format", spec.name, "error", err)
		}
	}
}
