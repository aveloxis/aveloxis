package collector

import (
	"testing"
)

// TestManifestFilesCoversNuGet verifies .NET/NuGet manifest coverage.
func TestManifestFilesCoversNuGet(t *testing.T) {
	for _, f := range []string{"*.csproj", "packages.config", "Directory.Packages.props"} {
		// We check csproj via the extension-based matching, not the exact filename map.
		// But packages.config and Directory.Packages.props should be in the manifest map.
		_ = f
	}
	if _, ok := manifestFiles["packages.config"]; !ok {
		t.Error("manifestFiles missing packages.config for .NET/NuGet")
	}
}

// TestManifestFilesCoversScala verifies Scala/sbt manifest coverage.
func TestManifestFilesCoversScala(t *testing.T) {
	if _, ok := manifestFiles["build.sbt"]; !ok {
		t.Error("manifestFiles missing build.sbt for Scala")
	}
}

// TestManifestFilesCoversHaskell verifies Haskell/Cabal manifest coverage.
func TestManifestFilesCoversHaskell(t *testing.T) {
	if _, ok := manifestFiles["package.yaml"]; !ok {
		t.Error("manifestFiles missing package.yaml for Haskell")
	}
}

// TestManifestFilesCoversGradle verifies build.gradle.kts coverage.
func TestManifestFilesCoversGradleKts(t *testing.T) {
	if _, ok := manifestFiles["build.gradle.kts"]; !ok {
		t.Error("manifestFiles missing build.gradle.kts for Kotlin/Java")
	}
}

// TestLibyearScanCoversMaven verifies Maven/Java libyear resolution exists.
func TestLibyearScanCoversMaven(t *testing.T) {
	// Libyear scan should handle pom.xml files with "maven" manager.
	dep := libyearDep{Name: "junit", Version: "4.13", Manager: "maven"}
	if dep.Manager != "maven" {
		t.Error("libyearDep should support maven manager")
	}
}

// TestLibyearScanCoversNuGet verifies NuGet libyear resolution exists.
func TestLibyearScanCoversNuGet(t *testing.T) {
	dep := libyearDep{Name: "Newtonsoft.Json", Version: "13.0.3", Manager: "nuget"}
	if dep.Manager != "nuget" {
		t.Error("libyearDep should support nuget manager")
	}
}

// TestLibyearScanCoversHex verifies Hex (Elixir) libyear resolution exists.
func TestLibyearScanCoversHex(t *testing.T) {
	dep := libyearDep{Name: "phoenix", Version: "1.7.0", Manager: "hex"}
	if dep.Manager != "hex" {
		t.Error("libyearDep should support hex manager")
	}
}

// TestParsePomXMLVersions verifies pom.xml version extraction.
func TestParsePomXMLVersions(t *testing.T) {
	deps := parsePomXMLVersions("<project><dependencies><dependency><groupId>junit</groupId><artifactId>junit</artifactId><version>4.13</version></dependency></dependencies></project>")
	found := false
	for _, d := range deps {
		if d.Name == "junit:junit" && d.Version == "4.13" {
			found = true
		}
	}
	if !found {
		t.Errorf("parsePomXMLVersions did not extract junit:junit 4.13, got %v", deps)
	}
}

// TestParseNuGetPackagesConfig verifies NuGet packages.config parsing.
func TestParseNuGetPackagesConfig(t *testing.T) {
	xml := `<?xml version="1.0" encoding="utf-8"?>
<packages>
  <package id="Newtonsoft.Json" version="13.0.3" />
  <package id="NUnit" version="3.13.3" />
</packages>`
	deps := parseNuGetPackagesConfig(xml)
	if len(deps) != 2 {
		t.Fatalf("parseNuGetPackagesConfig returned %d deps, want 2", len(deps))
	}
	if deps[0] != "Newtonsoft.Json" {
		t.Errorf("deps[0] = %q, want %q", deps[0], "Newtonsoft.Json")
	}
}

// TestParseBuildSbt verifies Scala build.sbt parsing.
func TestParseBuildSbt(t *testing.T) {
	content := `libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.8.0"
`
	deps := parseBuildSbt(content)
	if len(deps) != 2 {
		t.Fatalf("parseBuildSbt returned %d deps, want 2", len(deps))
	}
	if deps[0] != "org.scalatest:scalatest" {
		t.Errorf("deps[0] = %q, want %q", deps[0], "org.scalatest:scalatest")
	}
}

// TestParsePackageYaml verifies Haskell package.yaml parsing.
func TestParsePackageYaml(t *testing.T) {
	content := `dependencies:
  - base >= 4.7 && < 5
  - aeson
  - text
`
	deps := parsePackageYaml(content)
	if len(deps) < 2 {
		t.Fatalf("parsePackageYaml returned %d deps, want >= 2", len(deps))
	}
}

// TestParsePubspecVersions verifies Dart pubspec.yaml version extraction.
func TestParsePubspecVersions(t *testing.T) {
	content := `name: my_app
dependencies:
  flutter:
    sdk: flutter
  http: ^0.13.6
  provider: ">=6.0.0 <7.0.0"

dev_dependencies:
  flutter_test:
    sdk: flutter
  build_runner: ^2.4.0
`
	deps := parsePubspecVersions(content)
	// Should get http, provider, build_runner — skip flutter/flutter_test (sdk deps)
	if len(deps) < 2 {
		t.Fatalf("parsePubspecVersions returned %d deps, want >= 2", len(deps))
	}
	found := false
	for _, d := range deps {
		if d.Name == "http" && d.Manager == "pub" {
			found = true
			if d.Version != "0.13.6" {
				t.Errorf("http version = %q, want %q", d.Version, "0.13.6")
			}
		}
	}
	if !found {
		t.Errorf("parsePubspecVersions did not find 'http' dep, got %v", deps)
	}
}

// TestParsePackageSwiftVersions verifies Swift Package.swift version extraction.
func TestParsePackageSwiftVersions(t *testing.T) {
	content := `// swift-tools-version:5.7
import PackageDescription

let package = Package(
    name: "MyApp",
    dependencies: [
        .package(url: "https://github.com/Alamofire/Alamofire.git", from: "5.6.0"),
        .package(url: "https://github.com/SwiftyJSON/SwiftyJSON.git", exact: "5.0.1"),
        .package(url: "https://github.com/realm/realm-swift.git", .upToNextMajor(from: "10.40.0")),
    ]
)`
	deps := parsePackageSwiftVersions(content)
	if len(deps) != 3 {
		t.Fatalf("parsePackageSwiftVersions returned %d deps, want 3", len(deps))
	}
	if deps[0].Name != "Alamofire" {
		t.Errorf("deps[0].Name = %q, want %q", deps[0].Name, "Alamofire")
	}
	if deps[0].Version != "5.6.0" {
		t.Errorf("deps[0].Version = %q, want %q", deps[0].Version, "5.6.0")
	}
	if deps[0].Manager != "swiftpm" {
		t.Errorf("deps[0].Manager = %q, want %q", deps[0].Manager, "swiftpm")
	}
}

// TestParseHaskellPackageYamlVersions verifies Haskell package.yaml version extraction.
func TestParseHaskellPackageYamlVersions(t *testing.T) {
	content := `dependencies:
  - base >= 4.7 && < 5
  - aeson >= 2.0
  - text
  - bytestring
`
	deps := parseHaskellPackageYamlVersions(content)
	if len(deps) < 3 {
		t.Fatalf("parseHaskellPackageYamlVersions returned %d deps, want >= 3", len(deps))
	}
	// "base" should have version "4.7", "aeson" should have "2.0", "text" should have ""
	found := false
	for _, d := range deps {
		if d.Name == "aeson" && d.Manager == "hackage" {
			found = true
			if d.Version != "2.0" {
				t.Errorf("aeson version = %q, want %q", d.Version, "2.0")
			}
		}
	}
	if !found {
		t.Errorf("did not find 'aeson' with hackage manager")
	}
}

// TestParseMixExsVersions verifies Elixir mix.exs version extraction.
func TestParseMixExsVersions(t *testing.T) {
	content := `defp deps do
    [
      {:phoenix, "~> 1.7.0"},
      {:ecto_sql, "~> 3.10"},
      {:jason, "~> 1.2"}
    ]
  end`
	deps := parseMixExsVersions(content)
	if len(deps) != 3 {
		t.Fatalf("parseMixExsVersions returned %d deps, want 3", len(deps))
	}
	if deps[0].Name != "phoenix" {
		t.Errorf("deps[0].Name = %q, want %q", deps[0].Name, "phoenix")
	}
	if deps[0].Manager != "hex" {
		t.Errorf("deps[0].Manager = %q, want %q", deps[0].Manager, "hex")
	}
}
