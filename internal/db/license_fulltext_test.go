// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/spdx"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Worklist 60 (v0.29.67 review round 15): the full-text fingerprint mapped
// every GPL-family text to a GPL "-only" ID. An FSF "or (at your option) any
// later version" header read GPL-3.0-only, a Classpath notice dropped its
// exception, and the whole AGPL-3.0 license read GPL-3.0-only. Under the
// round-7 class decision (never assert a license falsely; keep the text when
// unsure) the fingerprint is either exact or answers nothing.

// readLicenseFixture reads a verbatim license text from testdata/licenses.
// The GPL-family texts permit verbatim copies. Each is byte-identical to the
// file a module ships, except LGPL-3.0.txt: the LGPL text from a COPYING file
// with that file's preface cut and the title's indent trimmed (the words are
// verbatim; the reader compares words, not layout). Worklist 60.
func readLicenseFixture(t *testing.T, name string) string {
	t.Helper()
	return srctest.Read(t, "internal/db/testdata/licenses/"+name)
}

// assertKeptAsText fails when the normalizer asserted a license (a valid SPDX
// expression or a family label) for a text it cannot state exactly.
func assertKeptAsText(t *testing.T, label, text string) {
	t.Helper()
	if got := NormalizeLicenseToSPDX(text); spdx.Valid(got) || spdx.FamilyLabels[got] {
		t.Errorf("%s normalized to %q; want the text kept (no license asserted)", label, got)
	}
}

func TestFullTextFingerprintKeepsVersionRangeAndExceptions(t *testing.T) {
	const (
		fsf3 = "This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version."
		fsf2 = "This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation; either version 2 of the License, or (at your option) any later version."
	)
	exact := map[string]string{
		fsf3: "GPL-3.0-or-later",
		fsf2: "GPL-2.0-or-later",
		"Licensed under the GNU General Public License version 2 or later; see the COPYING file in the top-level directory of this distribution.": "GPL-2.0-or-later",
		// No version range: the notice grants that version only.
		"This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License version 2 as published by the Free Software Foundation.": "GPL-2.0-only",
		"This program is free software: you can redistribute it under the terms of the GNU General Public License version 3.":                                                               "GPL-3.0-only",
		// The OpenJDK notice: GPL-2.0 only, with the Classpath exception.
		`This code is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License version 2 only, as published by the Free Software Foundation. Oracle designates this particular file as subject to the "Classpath" exception as provided by Oracle in the LICENSE file that accompanied this code.`: "GPL-2.0-only WITH Classpath-exception-2.0",
		"Licensed under the Apache License v2.0 with LLVM Exceptions. See https://llvm.org/LICENSE.txt for license information.": "Apache-2.0 WITH LLVM-exception",
		// Apache has no "-or-later" ID; SPDX writes the range with "+".
		"Licensed under the Apache License, Version 2.0 or (at your option) any later version of the Apache License, see LICENSE.": "Apache-2.0+",
		// Bodies keep the ScanCode convention: the text alone is the "-only" ID.
		readLicenseFixture(t, "GPL-3.0.txt"):    "GPL-3.0-only",
		readLicenseFixture(t, "GPL-2.0.txt"):    "GPL-2.0-only",
		readLicenseFixture(t, "Apache-2.0.txt"): "Apache-2.0",
		// The MPL-2.0 body names the GNU GPL (its "Secondary License"
		// definition, versions 2.0 and 3.0); it is still MPL-2.0.
		readLicenseFixture(t, "MPL-2.0.txt"): "MPL-2.0",
		// The OpenJDK LICENSE file: the GPL-2.0 body with the exception after it.
		readLicenseFixture(t, "GPL-2.0.txt") + "\n\"CLASSPATH\" EXCEPTION TO THE GPL\n\nCertain source files distributed by Oracle America and/or its affiliates are subject to the following clarification and special exception to the GPL, but only where Oracle has expressly included in the particular source file's header the words \"Oracle designates this particular file as subject to the \"Classpath\" exception as provided by Oracle in the LICENSE file that accompanied this code.\"\n": "GPL-2.0-only WITH Classpath-exception-2.0",
	}
	// A body cut before its terms end (packages embed partial copies) still
	// carries its own section-7 "exceptions" wording; that is the license's,
	// not an appended exception.
	gpl3 := readLicenseFixture(t, "GPL-3.0.txt")
	cut := strings.Index(strings.ToUpper(gpl3), "END OF TERMS AND CONDITIONS")
	if cut < 0 || !strings.Contains(strings.ToLower(gpl3[:cut]), "exception") {
		t.Fatal("fixture premise: the GPL-3.0 body says \"exception\" before its terms end")
	}
	exact[gpl3[:cut]] = "GPL-3.0-only"
	for text, want := range exact {
		if got := NormalizeLicenseToSPDX(text); got != want {
			t.Errorf("NormalizeLicenseToSPDX(%.70q...) = %q, want %q", text, got, want)
		}
	}

	kept := map[string]string{
		// Not the GPL: each names its own license first and mentions the
		// GPL after it.
		"the AGPL-3.0 body": readLicenseFixture(t, "AGPL-3.0.txt"),
		"the LGPL-3.0 body": readLicenseFixture(t, "LGPL-3.0.txt"),
		"an AGPL notice":    "This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.",
		"an LGPL notice":    "This library is free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.",
		// Two versions named: no one ID states the choice.
		"a two-version notice": "Licensed under the GNU General Public License version 2 or version 3, at your choice, see COPYING for details.",
		// An exception the fingerprint cannot name exactly.
		"a GCC runtime notice":                      "GCC is free software under the GNU General Public License version 3, with the GCC Runtime Library Exception version 3.1 applying to the runtime libraries.",
		"an Apache notice with an exception":        "Licensed under the Apache License, Version 2.0, with the Example Corp linking exception described in the EXCEPTIONS file of this distribution.",
		"a GPL-3.0 body with an appended exception": readLicenseFixture(t, "GPL-3.0.txt") + "\nGCC RUNTIME LIBRARY EXCEPTION\n\nVersion 3.1, 31 March 2009\n\nThis GCC Runtime Library Exception is an additional permission under section 7 of the GNU General Public License, version 3.\n",
		// Classpath is written for GPL-2.0; on GPL-3.0 it is not the SPDX exception.
		"a GPL-3.0 Classpath notice": "This code is free software under the terms of the GNU General Public License version 3, subject to the Classpath exception described in the LICENSE file.",
	}
	for label, text := range kept {
		assertKeptAsText(t, label, text)
	}
}

// TestNameListInheritsTheExactFingerprint: nameListVerdict agrees parts on the
// fingerprint's answer, so the exact reading decides the list (the round-15 C1
// doc note). "GPL-3.0-only OR <an or-later header>" used to agree on
// GPL-3.0-only; the parts now disagree and the text is kept.
func TestNameListInheritsTheExactFingerprint(t *testing.T) {
	fsf3 := "This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version."
	if got := NormalizeLicenseToSPDX("GPL-3.0-or-later OR " + fsf3); got != "GPL-3.0-or-later" {
		t.Errorf("agreeing or-later parts = %q, want GPL-3.0-or-later", got)
	}
	assertKeptAsText(t, "GPL-3.0-only OR an or-later header", "GPL-3.0-only OR "+fsf3)
	if !strings.Contains(fsf3, "any later version") {
		t.Fatal("fixture premise: the header carries the version range")
	}
}

// TestFingerprintReadsTheClauseNotTheText — review round 17: the first
// worklist-60 fix counted "or later", "exception", "classpath" and a version
// anywhere in the text, so a range or exception that belonged to something
// else turned into a false "-or-later", "+" or WITH. Decided as a class: a
// notice is answered only when it names one license and one version, and every
// range word and exception sits in the exact phrase attached to that version;
// otherwise the text is kept. A body is read from its own title, and appended
// text counts only as an exception TO that license.
func TestFingerprintReadsTheClauseNotTheText(t *testing.T) {
	apacheBody := readLicenseFixture(t, "Apache-2.0.txt")
	gpl2Body := readLicenseFixture(t, "GPL-2.0.txt")
	gpl3Body := readLicenseFixture(t, "GPL-3.0.txt")
	fsf3 := "This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version."
	exact := map[string]string{
		// A range that belongs to something else (S1).
		"Licensed under the Apache License, Version 2.0. This project requires Java 11 to build and run the tests.":                                                      "Apache-2.0",
		"This program is free software; you can redistribute it under the terms of the GNU General Public License version 2; see COPYING.":                               "GPL-2.0-only",
		apacheBody + "\nTHIRD-PARTY NOTICES\n\nlibfoo is distributed under the GNU Lesser General Public License, version 2.1, or (at your option) any later version.\n": "Apache-2.0",
		apacheBody + "\nThis product includes software developed at Example Corp, with the exception of the files in contrib/.\n":                                        "Apache-2.0",
		// "classpath" that is not the exception (C1): no exception at all.
		"This program is free software; you can redistribute it under the terms of the GNU General Public License version 2. See COPYING for the full terms.": "GPL-2.0-only",
		// LLVM's LICENSE.TXT: the Apache body, then the exceptions to it.
		apacheBody + "\n---- LLVM Exceptions to the Apache 2.0 License ----\n\nAs an exception, if, as a result of your compiling your source code, portions of this Software are embedded into an Object form of such source code, you may redistribute such embedded portions in such Object form without complying with the conditions of Sections 4(a), 4(b) and 4(d) of the License.\n": "Apache-2.0 WITH LLVM-exception",
		// S2: every word of the shared range list reads as the same range.
		"This program is free software; you can redistribute it under the terms of the GNU General Public License, version 2 or any newer version.":      "GPL-2.0-or-later",
		"This program is free software; you can redistribute it under the terms of the GNU General Public License, version 2 and above, at your option.": "GPL-2.0-or-later",
		"This program is free software; you can redistribute it under the terms of the GNU General Public License version 3 or higher, at your option.":  "GPL-3.0-or-later",
		"Licensed under the Apache License, Version 2.0 or any subsequent version of the Apache License, at your option.":                                "Apache-2.0+",
		// The standard license headers stay what they were.
		`Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0`:                                                                                                                                                                                                                                                                      "Apache-2.0",
		"This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation; either version 2 of the License, or (at your option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.": "GPL-2.0-or-later",
		// A GPL body read from its own title; a notice before it that agrees
		// on the version decides the range (C2).
		fsf3 + "\n\n" + gpl3Body:             "GPL-3.0-or-later",
		"GPL version 2 only.\n\n" + gpl2Body: "GPL-2.0-only",
	}
	for text, want := range exact {
		if got := NormalizeLicenseToSPDX(text); got != want {
			t.Errorf("NormalizeLicenseToSPDX(%.90q...) = %q, want %q", text, got, want)
		}
	}
	kept := map[string]string{
		// S1: a range word outside the version's own clause.
		"a range in another sentence":   "This program is free software; you can redistribute it under the terms of the GNU General Public License version 2. Requires Python 3.8 or later to run.",
		"a range after the clause ends": "This program is free software; you can redistribute it under the terms of the GNU General Public License version 2; earlier versions or later versions of this license do not apply to it.",
		"Apache and GPL-or-later":       "Licensed under the Apache License 2.0 or the GNU General Public License version 2 or later, at your choice.",
		"Apache and GPL, dual":          "Dual licensed under the Apache License, Version 2.0 and the GNU General Public License version 3 or later; see LICENSE-APACHE and LICENSE-GPL.",
		// S3: another license named in the notice.
		"MIT and GPL, dual":   "Dual licensed under the MIT license and the GNU General Public License version 2 or later. See MIT-LICENSE.txt and GPL-LICENSE.txt for details.",
		"GPL and Apache docs": "Released under the GNU General Public License version 2 (GPL) -- the Apache License 2.0 applies to the bundled documentation files.",
		// C1: a "classpath" that is not the exception, next to an exception word.
		"classpath and another exception": "This program is free software under the GNU General Public License version 2, without exception. Add the jar to your classpath.",
		// C2: a notice before a body that disagrees on the version.
		"a v3 notice over the GPL-2.0 body":  fsf3 + "\n\n" + gpl2Body,
		"KDE-style two versions over a body": "This library is free software; you can redistribute it under the terms of the GNU General Public License as published by the Free Software Foundation; either version 2 of the License or (at your option) version 3 or any later version accepted by the membership of KDE e.V.\n\n" + gpl2Body,
		// An exception to the Apache license that is not LLVM's.
		"an Apache body with another exception": apacheBody + "\n---- Example Corp Exceptions to the Apache 2.0 License ----\n\nAs an exception, you may link this library into proprietary programs.\n",
		// COPYING.LESSER + COPYING: the LGPL-3.0 text, then the GPL-3.0 text.
		"the LGPL-3.0 body before the GPL-3.0 body": readLicenseFixture(t, "LGPL-3.0.txt") + "\n\n" + gpl3Body,
		// Two versions of one family, a point release.
		"GPL version 2.1": "This program is free software; you can redistribute it under the terms of the GNU General Public License version 2.1 as published by the Free Software Foundation.",
	}
	for label, text := range kept {
		assertKeptAsText(t, label, text)
	}
}

// TestFingerprintEnforcesItsOwnRule — review round 18: places where the
// clause reader did not yet enforce what the round-17 rule states. A "+" after
// the version is a range (the SPDX and GNU shorthand); a bare second number
// joined to the version is a second version; two license bodies in one text
// are not one license; the LLVM SPDX line names the LLVM exception; a version
// before the license name is the license's only as "version N of the ...
// license"; a prefix that asserts nothing defers to the body.
func TestFingerprintEnforcesItsOwnRule(t *testing.T) {
	gpl2Body := readLicenseFixture(t, "GPL-2.0.txt")
	gpl3Body := readLicenseFixture(t, "GPL-3.0.txt")
	apacheBody := readLicenseFixture(t, "Apache-2.0.txt")
	exact := map[string]string{
		// S1: "+" is the range.
		"This file is distributed under the terms of the GNU General Public License, GPL-2.0+. See COPYING.":                              "GPL-2.0-or-later",
		"This program is free software, distributed under the terms of the GNU General Public License (GPLv3+); see the COPYING file.":    "GPL-3.0-or-later",
		"This program is free software; you can redistribute it under the GNU General Public License version 2+ as published by the FSF.": "GPL-2.0-or-later",
		"Licensed under the Apache License 2.0+ ; see the LICENSE file distributed with this work for the terms.":                         "Apache-2.0+",
		// S4: the LLVM file header, SPDX line included.
		"Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions. See https://llvm.org/LICENSE.txt for license information. SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception": "Apache-2.0 WITH LLVM-exception",
		// C1: "version N of the ... license" before the name is the license's.
		"This program is free software; you can redistribute it under version 2 of the GNU General Public License as published by the Free Software Foundation.": "GPL-2.0-only",
		// C2: a prefix that asserts nothing defers to the body.
		"GPL\n\n" + gpl3Body: "GPL-3.0-only",
	}
	for text, want := range exact {
		if got := NormalizeLicenseToSPDX(text); got != want {
			t.Errorf("NormalizeLicenseToSPDX(%.90q...) = %q, want %q", text, got, want)
		}
	}
	kept := map[string]string{
		// S2: a bare second version.
		"version 2 or 3":   "This program is free software under the GNU General Public License version 2 or 3, at your option, as published by the FSF.",
		"(version 2 or 3)": "This program is free software under the GNU General Public License (version 2 or 3), as published by the FSF.",
		"GPLv2 or 3":       "This program is free software under the GNU General Public License: GPLv2 or 3 at your option, see COPYING.",
		"version 3 (or 2)": "This program is free software under the GNU General Public License version 3 (or 2) as published by the FSF.",
		"version 2 and 3":  "This program is free software under the GNU General Public License version 2 and 3, see the COPYING files.",
		"version 2/3":      "This program is free software under the GNU General Public License version 2/3, see the COPYING files here.",
		// S3: two license bodies in one text.
		"GPL-2.0 + GPL-3.0 bodies":  gpl2Body + "\n\n" + gpl3Body,
		"GPL-3.0 + GPL-2.0 bodies":  gpl3Body + "\n\n" + gpl2Body,
		"GPL-3.0 + AGPL-3.0 bodies": gpl3Body + "\n\n" + readLicenseFixture(t, "AGPL-3.0.txt"),
		"GPL-2.0 + Apache bodies":   gpl2Body + "\n\n" + apacheBody,
		"Apache + GPL-2.0 bodies":   apacheBody + "\n\n" + gpl2Body,
		// C1: a product version before the license name.
		"a product version":              "MyTool v3 is free software; you can redistribute it under the terms of the GNU General Public License as published by the Free Software Foundation.",
		"a product version, spelled out": "MyTool version 3 is free software; you can redistribute it under the terms of the GNU General Public License as published by the FSF.",
		// C3: other licenses the list did not name.
		"GPL and the FDL":   "This program is free software under the GNU General Public License version 3; the documentation is under the GNU Free Documentation License.",
		"GPL or commercial": "This program is free software under the GNU General Public License version 3, or under a commercial license agreement with Example Corp.",
	}
	for label, text := range kept {
		assertKeptAsText(t, label, text)
	}
}

// TestNoticeAccountsForEveryLicenseWord — review round 19, decided as a class
// (the fourth round on this boundary): a word list of OTHER licenses can never
// be complete, so a notice is read only when every "license" word in it is
// accounted for as the license's own name or a reference back to it ("the
// License", "LICENSE.txt", the license URL). Any other "... license" is
// another license, and the text is kept. A version after the name must be in
// the name's sentence; an SPDX-License-Identifier line that holds a valid
// expression states the license exactly.
func TestNoticeAccountsForEveryLicenseWord(t *testing.T) {
	gpl3Body := readLicenseFixture(t, "GPL-3.0.txt")
	apacheBody := readLicenseFixture(t, "Apache-2.0.txt")
	exact := map[string]string{
		// C2: a Go import path is not a version (the real go-cty-yaml NOTICE).
		`This package is derived from gopkg.in/yaml.v2, which is copyright 2011-2016 Canonical Ltd. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0`: "Apache-2.0",
		// C4: the SPDX line states the license, exception included.
		"/* SPDX-License-Identifier: GPL-2.0 WITH Linux-syscall-note */\nThis program is free software; you can redistribute it under the terms of the GNU General Public License version 2.": "GPL-2.0 WITH Linux-syscall-note",
		"# SPDX-License-Identifier: GPL-2.0-or-later\n# This program is free software; you can redistribute it under the terms of the GNU General Public License.":                            "GPL-2.0-or-later",
		// S2: a notice above an Apache body decides its range.
		"Licensed under the Apache License, Version 2.0 or (at your option) any later version.\n\n" + apacheBody: "Apache-2.0+",
		// The real LLVM LICENSE.TXT opening line above the body.
		"The LLVM Project is under the Apache License v2.0 with LLVM Exceptions:\n\n" + apacheBody: "Apache-2.0 WITH LLVM-exception",
		// The tag counts only as the file header's statement: after comment
		// markers, a shebang or copyright lines, and not in a text holding a
		// license body (apache/arrow's LICENSE.txt tags a bundled LLVM
		// section at line 248; the review-19 sweep).
		"#!/usr/bin/env python3\n# Copyright (c) 2024 Example Corp\n# SPDX-License-Identifier: Apache-2.0\n# Licensed under the terms stated in the header above; see the project documentation.": "Apache-2.0",
		// The standard ASF source header (the review-19 sweep: thrift).
		`Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0`: "Apache-2.0",
		// The Elastic NOTICE grant (the review-19 sweep: gosigar).
		"Copyright (c) 2012-2014 Example Corp. All rights reserved. This product is licensed to you under the Apache License, Version 2.0 (the \"License\"). You may not use this product except in compliance with the License.": "Apache-2.0",
		// The version between the words of the name.
		"These icons are distributed by Example Corp under the Apache 2.0 license (https://www.apache.org/licenses/LICENSE-2.0).": "Apache-2.0",
		// The OpenJDK header, whose "LICENSE file" is a reference back.
		`This code is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License version 2 only, as published by the Free Software Foundation. Oracle designates this particular file as subject to the "Classpath" exception as provided by Oracle in the LICENSE file that accompanied this code.`: "GPL-2.0-only WITH Classpath-exception-2.0",
	}
	for text, want := range exact {
		if got := NormalizeLicenseToSPDX(text); got != want {
			t.Errorf("NormalizeLicenseToSPDX(%.90q...) = %q, want %q", text, got, want)
		}
	}
	kept := map[string]string{
		// S1: another license in unlisted words, before a body and in a notice.
		"a proprietary offer over a body":  "This software is available under a proprietary license or under the GNU General Public License below.\n\n" + gpl3Body,
		"a separate agreement over a body": "This software is dual-licensed. You may use it under the GNU General Public License reproduced below, or under the terms of a separate license agreement with Example Corp.\n\n" + gpl3Body,
		"GPL or a proprietary license":     "This program is free software under the GNU General Public License version 3 or under a proprietary license from Example Corp.",
		// S2: an Apache-or-GPL offer above an Apache body.
		"Apache or GPL over an Apache body": "This project may be used, at your option, under either the Apache License, Version 2.0 or the GNU General Public License, version 2.0 or later.\n\n" + apacheBody,
		// S3: CC-BY / CC-BY-SA documentation.
		"GPL code, CC-BY-SA docs": "This program is free software under the GNU General Public License version 3 or later; the documentation is licensed under CC-BY-SA 4.0.",
		"GPL code, CC BY-SA docs": "Code: GNU General Public License version 3 or later. Documentation: CC BY-SA 4.0 International.",
		"Apache code, CC-BY docs": "Licensed under the Apache License, Version 2.0; documentation is licensed under CC-BY 4.0 International.",
		// C1: a version in another sentence, or an import path.
		"a GTK version":    "This program is free software under the terms of the GNU General Public License. It is built with GTK version 3.",
		"a yaml.v3 import": "This program is free software under the terms of the GNU General Public License. It uses gopkg.in/yaml.v3 for configuration files.",
		// C3: a prefix that names another license or a range without "GPL".
		"an MIT offer over a body": "You may use this software under the MIT License, or under the terms below.\n\n" + gpl3Body,
		"a range over a body":      "Version 3 or any later version applies to this work.\n\n" + gpl3Body,
		// C5: the X11 license.
		"GPL or X11": "This file is dual-licensed: you can use it either under the terms of the GPL, or the X11 license, at your option. Either version 2 of the License, or (at your option) any later version.",
		// Another license the word list does not name: only the license-word
		// accounting sees it.
		"GPL or an unlisted license": "This program is free software under the GNU General Public License version 3, or under the Example Corp Enterprise License at your choice.",
		// A prefix whose only claim is another license, over a body.
		"another license over a body": "Parts of this project are covered by a different license; see NOTICE for the list.\n\n" + gpl3Body,
		// A tag above a body of another license: the text states two things.
		"an MIT tag over an Apache body": "SPDX-License-Identifier: MIT\n\n" + apacheBody,
		// Two tags that disagree.
		"two disagreeing tags": "// SPDX-License-Identifier: MIT\n// SPDX-License-Identifier: Apache-2.0\n// This file is part of Example; the project documentation describes its licensing.",
		// A bundled section's tag after the body states something the text
		// as a whole does not (round 20 S3; round 19 read it as Apache-2.0).
		"a bundled LLVM tag after the body": apacheBody + "\n\nThird-party: vendored/llvm-support\n\nApache License v2.0 with LLVM Exceptions.\nSPDX-License-Identifier: Apache-2.0 WITH LLVM-exception\n",
		// A tag that does not open the text describes something else.
		"a tag inside a bundled section": "Parts of this package are available under several licenses as listed below; see the repository for details.\n\nvendored/foo:\nSPDX-License-Identifier: MIT\n",
		// C6: a dash range.
		"version 2-3": "This program is free software under the GNU General Public License version 2-3 as published by the Free Software Foundation.",
	}
	for label, text := range kept {
		assertKeptAsText(t, label, text)
	}
}

// fsfGPL2Notice and the others are the standard headers verbatim, as source
// files carry them: line-wrapped, and behind a comment marker.
const (
	fsfGPL2Notice = `This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.`
	fsfGPL3Notice = `This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.`
	asfHeader = `Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.`
)

// commented puts each line of text behind a comment marker, the way a
// source file carries its header.
func commented(text, open, marker, close string) string {
	lines := strings.Split(text, "\n")
	for i, l := range lines {
		lines[i] = strings.TrimRight(marker+l, " ")
	}
	out := strings.Join(lines, "\n")
	if open != "" {
		out = open + "\n" + out
	}
	if close != "" {
		out += "\n" + close
	}
	return out
}

// TestStandardHeadersBehindCommentMarkers — review round 20 S1/S2: the ASF
// and FSF headers wrap their phrases across lines, and in a source file each
// line starts with a comment marker, which sat inside "The ASF licenses this
// file # to you under" and "or # (at your option) any later version".
func TestStandardHeadersBehindCommentMarkers(t *testing.T) {
	forms := []struct{ open, marker, close string }{
		{"", "", ""}, {"/*", " * ", " */"}, {"", "# ", ""}, {"", "// ", ""}, {"", "-- ", ""}, {"", "; ", ""},
	}
	for _, f := range forms {
		for text, want := range map[string]string{
			fsfGPL2Notice: "GPL-2.0-or-later",
			fsfGPL3Notice: "GPL-3.0-or-later",
			asfHeader:     "Apache-2.0",
		} {
			in := commented(text, f.open, f.marker, f.close)
			if got := NormalizeLicenseToSPDX(in); got != want {
				t.Errorf("marker %q: NormalizeLicenseToSPDX(%.50q...) = %q, want %q", f.marker, in, got, want)
			}
		}
	}
}

// TestPartialStatementsDoNotSpeakForTheWhole — review round 20 S3, S4, C2,
// C3: a "+", an exception, an SPDX tag or a choice that applies to part of the
// code, or to something else, does not describe the whole; neither does a
// range word the reader does not know, or a restriction appended to a body.
func TestPartialStatementsDoNotSpeakForTheWhole(t *testing.T) {
	apacheBody := readLicenseFixture(t, "Apache-2.0.txt")
	gpl3Body := readLicenseFixture(t, "GPL-3.0.txt")
	exact := map[string]string{
		// A tag inside a notice that agrees with it (the LLVM header).
		"//===-- lib/Foo.cpp - Foo ----*- C++ -*-===//\n//\n// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.\n// See https://llvm.org/LICENSE.txt for license information.\n// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception\n//\n//===----------------------------------------------------------------------===//": "Apache-2.0 WITH LLVM-exception",
		// "future" is a range word.
		"This program is free software; you can redistribute it under the terms of the GNU General Public License v2 or any future version.": "GPL-2.0-or-later",
		// A notice pointing at its LICENSE file by URL.
		"This program is free software under the GNU General Public License version 3; see https://github.com/example/project/blob/main/LICENSE for the text.": "GPL-3.0-only",
	}
	for text, want := range exact {
		if got := NormalizeLicenseToSPDX(text); got != want {
			t.Errorf("NormalizeLicenseToSPDX(%.90q...) = %q, want %q", text, got, want)
		}
	}
	kept := map[string]string{
		// S3: part of the code.
		"a bundled file's tag":       "This program is free software; you can redistribute it under the terms of the GNU General Public License version 2.\n\nThe bundled file lib/foo.c carries its own tag:\nSPDX-License-Identifier: GPL-2.0+",
		"a bundled LLVM tag":         "Licensed under the Apache License, Version 2.0 (the \"License\"); you may not use this file except in compliance with the License.\n\nthird_party/llvm-support:\nSPDX-License-Identifier: Apache-2.0 WITH LLVM-exception",
		"contrib files are GPL-2.0+": "This program is free software; you can redistribute it under the terms of the GNU General Public License version 2. A few helper files in contrib/ are GPL-2.0+.",
		"java/ carries Classpath":    "This program is free software; you can redistribute it under the terms of the GNU General Public License version 2. The files in the java/ directory carry the Classpath exception.",
		// S4: a range the list does not know.
		"or any version thereafter": "This program is free software; you can redistribute it under the terms of the GNU General Public License version 3 or any version thereafter.",
		"or any following version":  "This program is free software; you can redistribute it under the terms of the GNU General Public License version 3 or any following version.",
		// C2: a choice made without the word "license".
		"or the Ruby terms":      "This program is free software; you can redistribute it under the terms of the GNU General Public License version 2, or the Ruby terms, at your choice.",
		"or OpenSSL's terms":     "This program is free software; you can redistribute it under the terms of the GNU General Public License version 2, or under OpenSSL's terms at your option.",
		"or a PolyForm URL":      "This program is free software under the GNU General Public License version 3, or alternatively under https://polyformproject.org/licenses/noncommercial/1.0.0 for noncommercial use.",
		"Apache or PolyForm URL": "Licensed under the Apache License 2.0 (the \"License\"); or the PolyForm Shield terms at https://polyformproject.org/licenses/shield/1.0.0 at your option.",
		// Each signal on its own: a choice word, a "terms" word, another
		// license's URL.
		"only a choice word":       "This program is free software under the GNU General Public License version 2, or alternatively under the Example Corp EULA.",
		"only a terms word":        "This program is free software under the GNU General Public License version 2, or the Ruby terms.",
		"only another license URL": "This program is free software under the GNU General Public License version 3; a noncommercial grant applies under https://polyformproject.org/licenses/noncommercial/1.0.0 as well.",
		// C3: a restriction or additional permission appended to a body.
		"Commons Clause after Apache": apacheBody + "\n\n\"Commons Clause\" License Condition v1.0\n\nThe Software is provided to you by the Licensor under the License, as defined below, subject to the following condition.\n",
		"GPL-3 section 7 permission":  gpl3Body + "\n\nAdditional permission under GNU GPL version 3 section 7\n\nIf you modify this Program, or any covered work, by linking or combining it with Example Library, the licensors of this Program grant you additional permission to convey the resulting work.\n",
	}
	for label, text := range kept {
		assertKeptAsText(t, label, text)
	}
}

// TestNoticeCostIsLinear — review round 20 S5: the reader compared every
// version mention with every anchor, quadratic in the input, and the input is
// registry-controlled (a 199 KB field took 0.7 s). Four times the input must
// cost well under sixteen times the time: linear work is about four times,
// quadratic sixteen, and eight separates them. The fastest of several runs
// is compared, so a loaded machine or -race (which slows both alike) does
// not decide it.
func TestNoticeCostIsLinear(t *testing.T) {
	if testing.Short() {
		t.Skip("timing comparison")
	}
	build := func(n int) string {
		return "gnu general public license " + strings.Repeat("gpl2 ", n)
	}
	fastest := func(s string) time.Duration {
		best := time.Duration(1<<63 - 1)
		for range 5 {
			start := time.Now()
			detectGPLText(s)
			if d := time.Since(start); d < best {
				best = d
			}
		}
		return best
	}
	// Many version mentions, and many "license" words to account for.
	for _, build := range []func(int) string{
		build,
		func(n int) string { return "gnu general public license " + strings.Repeat("the license ", n) },
		func(n int) string { return "gnu general public license " + strings.Repeat("gplv2 or later ", n) },
	} {
		small, large := build(10000), build(40000)
		ts, tl := fastest(small), fastest(large)
		if tl > 8*ts {
			t.Errorf("notice reading is superlinear: %v for %d bytes, %v for %d bytes", ts, len(small), tl, len(large))
		}
	}
}

// TestEveryMentionSaysTheSameThing — review round 21: "the first mention
// decides" let a partial statement made FIRST speak for the whole ("The
// contrib/ files are GPL-2.0+. Everything else ... version 2."). A partial
// statement cannot be told from a restatement without parsing the prose, so
// every version mention must state the same range, in any order; a notice
// whose mentions disagree keeps the text. So does one that names a version
// bare ("GPL 3"), a range the reader does not know, another agreement, or an
// SPDX tag it cannot read.
func TestEveryMentionSaysTheSameThing(t *testing.T) {
	exact := map[string]string{
		// C4: the pre-SPDX kernel boilerplate.
		"This software is licensed under the terms of the GNU General Public License version 2, as published by the Free Software Foundation, and may be copied, distributed, and modified under those terms.": "GPL-2.0-only",
	}
	for text, want := range exact {
		for _, in := range []string{text, commented(text, "/*", " * ", " */")} {
			if got := NormalizeLicenseToSPDX(in); got != want {
				t.Errorf("NormalizeLicenseToSPDX(%.90q...) = %q, want %q", in, got, want)
			}
		}
	}
	kept := map[string]string{
		// S2: a partial range stated first, or a restriction stated later.
		"contrib first":        "The contrib/ files are GPL-2.0+. Everything else in this program is free software under the GNU General Public License version 2.",
		"contrib first, ;":     "The contrib/ files are licensed under GPL-2.0+; for everything else this program is free software under the GNU General Public License version 2.",
		"lib/ first":           "Files in lib/ are free software under the GNU General Public License version 2 or (at your option) any later version. The rest of this program is under GPL version 2 only.",
		"one file only, later": "This program is free software under the GNU General Public License version 2 or (at your option) any later version. The file foo.c is GPL version 2 only.",
		// C1: a bare second version.
		"GPL 3 for some files": "Licensed under the GNU General Public License version 2. Some of the files are licensed under the GPL 3.",
		"docs under GPL 3.0":   "Licensed under the GNU General Public License version 2. The documentation is under GPL 3.0.",
		// C2: range wording the list does not know.
		"any more recent version": "This program is free software under the GNU General Public License version 2 or any more recent version.",
		"or beyond":               "This program is free software under the GNU General Public License version 2 or beyond, at the user's discretion.",
		"and onwards":             "This program is free software under the GNU General Public License version 2 and onwards.",
		"or successor versions":   "This program is free software under the GNU General Public License version 2 or successor versions of it.",
		// C3 and observed: other agreements and terms.
		"the License Agreement of Acme": "This program is free software under the GNU General Public License version 2. Use in hosted services requires the License Agreement of Acme Corp.",
		"an agreement you signed":       "This program is free software under the GNU General Public License version 3; the enterprise modules are covered by the license agreement you signed with Acme Corp.",
		"Apache and an ee/ agreement":   "Licensed under the Apache License, Version 2.0 (the \"License\"); you may not use this file except in compliance with the License. The ee/ directory is covered by the license agreement at https://example.com/ee.",
		"also available separately":     "This program is free software under the GNU General Public License version 2. Also available under a separate agreement from Acme Corp.",
		"an EULA":                       "This program is free software under the GNU General Public License version 2. You may also use this under the Acme EULA.",
		"except for a file":             "This program is free software under the GNU General Public License version 3 except for the file foo.c.",
		// Observed: an SPDX tag that is not a valid expression.
		"an unreadable tag": "SPDX-License-Identifier: SSPL\n" + fsfGPL3Notice,
	}
	for label, text := range kept {
		assertKeptAsText(t, label, text)
	}
}
