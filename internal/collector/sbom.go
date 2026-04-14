// Package collector — sbom.go generates CycloneDX and SPDX Software Bill of
// Materials from the dependency and libyear data collected for a repository.
package collector

import (
	"context"
	"crypto/sha256"
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
	BOMFormat    string            `json:"bomFormat"`
	SpecVersion  string            `json:"specVersion"`
	SerialNumber string            `json:"serialNumber"`
	Version      int               `json:"version"`
	Metadata     cdxMetadata       `json:"metadata"`
	Components   []cdxComponent    `json:"components"`
	Dependencies []cdxDependency   `json:"dependencies,omitempty"`
}

type cdxMetadata struct {
	Timestamp string        `json:"timestamp"`
	Tools     cdxTools      `json:"tools"`
	Component *cdxComponent `json:"component,omitempty"`
}

// cdxTools uses the CycloneDX 1.5 object form (components + services)
// instead of the deprecated pre-1.5 bare array.
type cdxTools struct {
	Components []cdxToolComponent `json:"components"`
}

type cdxToolComponent struct {
	Type    string `json:"type"`
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
	Author  string `json:"author,omitempty"`
}

type cdxComponent struct {
	Type      string       `json:"type"`
	Name      string       `json:"name"`
	Version   string       `json:"version,omitempty"`
	Purl      string       `json:"purl,omitempty"`
	BOMRef    string       `json:"bom-ref,omitempty"`
	Licenses  []cdxLicense `json:"licenses,omitempty"`
	Scope     string       `json:"scope,omitempty"`
	Copyright string       `json:"copyright,omitempty"`
	Evidence  *cdxEvidence `json:"evidence,omitempty"`
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
	Licenses  []cdxLicense          `json:"licenses,omitempty"`
	Copyright []cdxCopyrightEvidence `json:"copyright,omitempty"`
}

type cdxCopyrightEvidence struct {
	Text string `json:"text"`
}

// cdxDependency expresses the dependency DAG. Each entry lists a component
// (by bom-ref) and the components it directly depends on.
type cdxDependency struct {
	Ref       string   `json:"ref"`
	DependsOn []string `json:"dependsOn"`
}

func generateCycloneDX(repo *db.RepoForSBOM, deps []db.SBOMDep, scanData *db.ScancodeForSBOM) ([]byte, error) {
	rootRef := fmt.Sprintf("pkg:generic/%s/%s", repo.Owner, repo.Name)
	rootComp := &cdxComponent{
		Type:   "application",
		Name:   repo.Name,
		BOMRef: rootRef,
	}

	// Enrich root component with ScanCode data if available.
	if scanData != nil {
		if scanData.ConcludedLicenseSPDX != "" {
			rootComp.Evidence = &cdxEvidence{
				Licenses: []cdxLicense{makeCDXLicense(scanData.ConcludedLicenseSPDX)},
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
			Tools: cdxTools{
				Components: []cdxToolComponent{{
					Type:    "application",
					Name:    "aveloxis",
					Version: db.ToolVersion,
					Author:  "Augur Labs",
				}},
			},
			Component: rootComp,
		},
	}

	// Track dep bom-refs for the dependencies graph.
	var depRefs []string

	for _, dep := range deps {
		comp := cdxComponent{
			Type:    "library",
			Name:    dep.Name,
			Version: dep.CurrentVersion,
			Purl:    dep.Purl,
			BOMRef:  dep.Purl,
		}
		// CycloneDX scope describes runtime inclusion:
		// "required" = production, "excluded" = dev/test.
		if dep.Type == "dev" {
			comp.Scope = "excluded"
		} else {
			comp.Scope = "required"
		}
		if dep.License != "" {
			comp.Licenses = []cdxLicense{makeCDXLicense(dep.License)}
		}
		bom.Components = append(bom.Components, comp)

		if dep.Purl != "" {
			depRefs = append(depRefs, dep.Purl)
		}
	}

	// Build the dependencies graph: root depends on all components.
	bom.Dependencies = []cdxDependency{{
		Ref:       rootRef,
		DependsOn: depRefs,
	}}
	// Each dep is a leaf (no transitive info from manifest parsing).
	for _, ref := range depRefs {
		bom.Dependencies = append(bom.Dependencies, cdxDependency{
			Ref:       ref,
			DependsOn: []string{},
		})
	}

	return json.MarshalIndent(bom, "", "  ")
}

// makeCDXLicense creates a CycloneDX license entry, using the id field for
// recognized SPDX identifiers and the name field for non-standard strings.
func makeCDXLicense(license string) cdxLicense {
	l := cdxLicense{}
	if isSPDXLicense(license) {
		l.License.ID = license
	} else {
		l.License.Name = license
	}
	return l
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
	// Namespace must be unique per document (SPDX spec requirement).
	docUUID := uuid.New().String()
	doc := spdxDoc{
		SPDXVersion:       "SPDX-2.3",
		DataLicense:       "CC0-1.0",
		SPDXID:            "SPDXRef-DOCUMENT",
		Name:              repo.Name,
		DocumentNamespace: fmt.Sprintf("https://aveloxis.io/spdx/%s/%s/%s", repo.Owner, repo.Name, docUUID),
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

	for _, dep := range deps {
		// Stable package ID based on a hash of the name+version, not loop index.
		// This ensures IDs don't change when the dep list is reordered.
		pkgID := spdxPackageID(dep.Name, dep.CurrentVersion)
		declared := orNoAssertion(dep.License)

		pkg := spdxPackage{
			SPDXID:      pkgID,
			Name:        dep.Name,
			VersionInfo: dep.CurrentVersion,
			// SPDX downloadLocation requires a VCS/download URL, not a purl.
			// Purls are emitted as externalRefs below.
			DownloadLocation: "NOASSERTION",
			// LicenseConcluded requires source analysis per-dep. Without per-dep
			// scancode data, we can only assert what the registry declares.
			LicenseConcluded: "NOASSERTION",
			LicenseDeclared:  declared,
		}
		if dep.Purl != "" {
			pkg.ExternalRefs = []spdxExternalRef{{
				ReferenceCategory: "PACKAGE-MANAGER",
				ReferenceType:     "purl",
				ReferenceLocator:  dep.Purl,
			}}
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

// spdxPackageID generates a stable SPDX package identifier from the package
// name and version. Uses a truncated SHA-256 hash to ensure stability across
// regenerations regardless of dep ordering.
func spdxPackageID(name, version string) string {
	h := sha256.Sum256([]byte(name + "@" + version))
	return fmt.Sprintf("SPDXRef-Package-%x", h[:8])
}

func orNoAssertion(s string) string {
	if s == "" {
		return "NOASSERTION"
	}
	return s
}

// isSPDXLicense checks whether a license string is a recognized SPDX license
// identifier. CycloneDX and SPDX tools require the exact SPDX ID for
// machine-readable policy enforcement.
func isSPDXLicense(license string) bool {
	_, ok := spdxLicenses[license]
	return ok
}

// spdxLicenses is a set of commonly-used SPDX license identifiers.
// This is not exhaustive but covers the licenses most frequently seen in
// package registries. A full list can be generated from spdx.org/licenses.
var spdxLicenses = map[string]bool{
	"0BSD": true, "AAL": true, "AFL-3.0": true, "AGPL-3.0-only": true,
	"AGPL-3.0-or-later": true, "Apache-2.0": true, "Artistic-2.0": true,
	"BlueOak-1.0.0": true, "BSD-2-Clause": true, "BSD-3-Clause": true,
	"BSL-1.0": true, "CAL-1.0": true, "CAL-1.0-Combined-Work-Exception": true,
	"CC-BY-4.0": true, "CC-BY-SA-4.0": true, "CC0-1.0": true,
	"CPAL-1.0": true, "ECL-2.0": true, "EFL-2.0": true, "Entessa": true,
	"EUPL-1.1": true, "EUPL-1.2": true, "GPL-2.0-only": true,
	"GPL-2.0-or-later": true, "GPL-3.0-only": true, "GPL-3.0-or-later": true,
	"ISC": true, "LGPL-2.1-only": true, "LGPL-2.1-or-later": true,
	"LGPL-3.0-only": true, "LGPL-3.0-or-later": true, "LiLiQ-P-1.1": true,
	"LiLiQ-R-1.1": true, "LiLiQ-Rplus-1.1": true, "MIT": true,
	"MIT-0": true, "MPL-2.0": true, "MS-PL": true, "MS-RL": true,
	"MulanPSL-2.0": true, "NCSA": true, "Nokia": true, "OFL-1.1": true,
	"OSL-3.0": true, "PostgreSQL": true, "QPL-1.0": true, "RPL-1.1": true,
	"RPL-1.5": true, "RPSL-1.0": true, "RSCPL": true, "SimPL-2.0": true,
	"SISSL": true, "Sleepycat": true, "SPL-1.0": true, "UCL-1.0": true,
	"Unicode-DFS-2016": true, "Unlicense": true, "UPL-1.0": true,
	"VSL-1.0": true, "W3C": true, "Watcom-1.0": true, "Xnet": true,
	"Zlib": true, "ZPL-2.0": true, "ZPL-2.1": true,
	// Common deprecated IDs still seen in registries:
	"GPL-2.0": true, "GPL-3.0": true, "LGPL-2.0": true, "LGPL-2.1": true,
	"LGPL-3.0": true, "AGPL-3.0": true,
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
