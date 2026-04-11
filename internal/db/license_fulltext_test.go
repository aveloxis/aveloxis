package db

import (
	"testing"
)

// Tests for detecting full license texts (e.g., Python packages that store the
// entire BSD/MIT/Apache license body in the "license" field) and mapping them
// to canonical SPDX identifiers.

func TestNormalizeLicense_FullBSD3Text(t *testing.T) {
	// Must include "neither the name" to distinguish from BSD-2-Clause.
	fullText := `BSD 3-Clause License Copyright (c) 2008-2011, AQR Capital Management, LLC. All rights reserved. Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met: * Redistributions of source code must retain the above copyright notice. * Neither the name of the copyright holder nor the names of its contributors may be used.`
	got := NormalizeLicenseToSPDX(fullText)
	if got != "BSD-3-Clause" {
		t.Errorf("full BSD-3 text: got %q, want BSD-3-Clause", got)
	}
}

func TestNormalizeLicense_FullMITText(t *testing.T) {
	fullText := `MIT License Copyright (c) 2019 Someone Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction. THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND.`
	got := NormalizeLicenseToSPDX(fullText)
	if got != "MIT" {
		t.Errorf("full MIT text: got %q, want MIT", got)
	}
}

func TestNormalizeLicense_FullApache2Text(t *testing.T) {
	fullText := `Apache License Version 2.0, January 2004 http://www.apache.org/licenses/ TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION...`
	got := NormalizeLicenseToSPDX(fullText)
	if got != "Apache-2.0" {
		t.Errorf("full Apache text: got %q, want Apache-2.0", got)
	}
}

func TestNormalizeLicense_FullPSFText(t *testing.T) {
	fullText := `A. HISTORY OF THE SOFTWARE ========================== Python was created in the early 1990s by Guido van Rossum at Stichting Mathematisch Centrum...PYTHON SOFTWARE FOUNDATION LICENSE VERSION 2...`
	got := NormalizeLicenseToSPDX(fullText)
	if got != "PSF-2.0" {
		t.Errorf("full PSF text: got %q, want PSF-2.0", got)
	}
}

func TestNormalizeLicense_PermissionHerebyGrantedMIT(t *testing.T) {
	// MIT-style text that starts with "Permission is hereby granted".
	fullText := `Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction. THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED.`
	got := NormalizeLicenseToSPDX(fullText)
	if got != "MIT" {
		t.Errorf("MIT permission text: got %q, want MIT", got)
	}
}

func TestNormalizeLicense_FullISCText(t *testing.T) {
	fullText := `ISC License Copyright (c) 2023, Someone Permission to use, copy, modify, and/or distribute this software for any purpose with or without fee is hereby granted...`
	got := NormalizeLicenseToSPDX(fullText)
	if got != "ISC" {
		t.Errorf("full ISC text: got %q, want ISC", got)
	}
}

func TestNormalizeLicense_LongTextTruncation(t *testing.T) {
	// Completely unrecognizable long text should be truncated, not passed through.
	longJunk := ""
	for i := 0; i < 500; i++ {
		longJunk += "some random text "
	}
	got := NormalizeLicenseToSPDX(longJunk)
	if len(got) > 100 {
		t.Errorf("unrecognized long text should be truncated, got len=%d", len(got))
	}
}

func TestNormalizeLicense_MultiLicenseText(t *testing.T) {
	// Multiple license texts concatenated (common in Python).
	// BSD-3 has "neither the name" so it matches BSD-3-Clause, not BSD-2.
	multi := `BSD 3-Clause License Copyright (c) 2005-2023, NumPy Developers. All rights reserved. Redistribution and use in source and binary forms, with or without modification, are permitted. Neither the name of the copyright holder may be used. Apache License Version 2.0, January 2004.`
	got := NormalizeLicenseToSPDX(multi)
	if got != "BSD-3-Clause" {
		t.Errorf("multi-license text: got %q, want BSD-3-Clause (first match)", got)
	}
}

func TestNormalizeLicense_ShortStringUnchanged(t *testing.T) {
	// Short unrecognized strings should pass through unchanged.
	got := NormalizeLicenseToSPDX("Custom License v3")
	if got != "Custom License v3" {
		t.Errorf("short unrecognized: got %q, want unchanged", got)
	}
}
