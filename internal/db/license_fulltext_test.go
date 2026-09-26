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
	if testing.Short() || raceBuild {
		t.Skip("timing comparison (not under -short or the race detector)")
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
		func(n int) string { return "gnu general public license " + strings.Repeat("gplv2 or later of ", n) },
		// One long clause of range and exception phrases: each span's rest
		// of clause must not be rescanned (round 23).
		func(n int) string { return "gnu general public license " + strings.Repeat("gplv2 or later, ", n) },
		func(n int) string { return "gnu general public license " + strings.Repeat("gplv2 or later; ", n) },
		// Negated mentions inside URLs: each looked its URL up by copying and
		// sorting every URL span (round 31).
		func(n int) string {
			return "this program is free software under the gnu general public license. " + strings.Repeat("no http://a/gpl2 ", n)
		},
		// Many range phrases in one clause ending in a long punctuation run:
		// the clause's end was trimmed again for every phrase (round 26).
		func(n int) string {
			return "gnu general public license " + strings.Repeat(", version 2 or later", n) + strings.Repeat(" ,", 10*n) + "."
		},
		// Many version mentions far from the name: the sentence check per
		// mention rescanned back to the anchor (round 24 S1).
		func(n int) string { return "gnu general public license " + strings.Repeat(" v2.x", n) },
		func(n int) string { return "gnu general public license " + strings.Repeat("version 2.0 x.y ", n) },
		func(n int) string {
			return "gnu general public license " + strings.Repeat("gplv2 with the classpath exception, ", n)
		},
		func(n int) string {
			return "gnu general public license version 2.\nspdx-license-identifier: " + strings.Repeat("gpl-2.0-only/", n) + "gpl-2.0-only\n"
		},
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

// TestRound22SecondVersionsNegationAndClauseEnds — review round 22: more ways
// a notice names a second version ("2 and/or 3", "2 & 3", "version two"), a
// "terms" that points forward to other terms, a negated exception, a
// conditional range ("... any later version approved by the author"), and
// long prose split on "/" into an expression.
func TestRound22SecondVersionsNegationAndClauseEnds(t *testing.T) {
	lead := "This program is free software; you can redistribute it under the terms of the GNU General Public License "
	kept := map[string]string{
		"2 and/or 3":       lead + "version 2 and/or 3.",
		"2, 3":             lead + "version 2, 3.",
		"2 & 3":            lead + "version 2 & 3.",
		"2 or/and 3":       lead + "version 2 or/and 3.",
		"2+3":              lead + "version 2+3.",
		"version two":      lead + "version two. The GPL version 3 applies to the documentation.",
		"such terms as":    lead + "version 2. Binary redistribution in app stores is permitted under such terms as the author grants in writing.",
		"these terms:":     lead + "version 2. The firmware may be distributed under these terms: no modification permitted.",
		"those terms plus": `Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. The logo may be used under those terms plus attribution in the About box.`,
		"not subject to":   lead + "version 2 only, and this file is not subject to the Classpath exception.",
		"not with":         lead + "version 2, not with the Classpath exception.",
		"approved by":      lead + "version 2 or (at your option) any later version approved by the author.",
		"GFDL manual":      lead + "version 2 or (at your option) any later version. The manual is GFDL.",
		"SSPL":             `Licensed under the Apache License, Version 2.0 (the "License"). The server components are SSPL.`,
		"vendor/MIT":       lead + "version 2. Files in vendor/MIT",
		"lib/GPL-3.0":      lead + "version 2, except files in lib/GPL-3.0",
	}
	for label, text := range kept {
		assertKeptAsText(t, label, text)
	}
	// A spelled-out version on its own is that version.
	if got := NormalizeLicenseToSPDX(lead + "version two, as published by the Free Software Foundation."); got != "GPL-2.0-only" {
		t.Errorf("a notice for \"version two\" normalized to %q, want GPL-2.0-only", got)
	}
}

// TestRound23NumberWordsConditionsAndNegations — review round 23: a
// spelled-out second version ("version two or three"), a number word that is
// not a version ("... License, two copies of which ..."), a condition after a
// range phrase, a negation after an exception phrase, and "2 through 3".
func TestRound23NumberWordsConditionsAndNegations(t *testing.T) {
	lead := "This program is free software; you can redistribute it under the terms of the GNU General Public License "
	kept := map[string]string{
		"two or three":   lead + "version two or three.",
		"two (or three)": lead + "version two (or three).",
		"two and three":  lead + "version two and three.",
		"two, three":     lead + "version two, three.",
		"two/three":      lead + "version two/three.",
		"three or two":   lead + "version three or two.",
		"2 or three":     lead + "version 2 or three.",
		"3 or two":       lead + "version 3 or two.",
		"two copies":     "Licensed under the GNU General Public License, two copies of which are included in this repository for convenience.",
		"if approved":    lead + "version 2 or any later version, if approved by the author.",
		"(if approved)":  lead + "version 2 or any later version (if approved by the author).",
		"- subject to":   lead + "version 2 or any later version - subject to the maintainer's approval.",
		"as approved":    lead + "version 2 or any later version, as approved by the maintainer.",
		"does not apply": lead + "version 2 with the Classpath exception, which does not apply to this file.",
		"not applying":   lead + "version 2, with the classpath exception not applying.",
		"2 through 3":    lead + "version 2 through 3.",
		"2 to 3":         lead + "version 2 to 3.",
	}
	for label, text := range kept {
		assertKeptAsText(t, label, text)
	}
	// "as published by the Free Software Foundation" after a range is the
	// FSF's own wording, not a condition.
	if got := NormalizeLicenseToSPDX(lead + "version 2 or (at your option) any later version, as published by the Free Software Foundation."); got != "GPL-2.0-or-later" {
		t.Errorf("an FSF range followed by \"as published by\" = %q, want GPL-2.0-or-later", got)
	}
}

// TestPositionLookupsAreLogarithmic — review round 23: the clause checks ask
// "the next clause break after here" and "a condition word in this range" once
// per span. Inside a whole notice read, a linear scan per lookup hides under
// the regex passes, so the lookups are pinned on their own: four times the
// positions must cost well under four times the time (a binary search grows
// by about 1.1, a scan by 4; 2 separates them).
func TestPositionLookupsAreLogarithmic(t *testing.T) {
	if testing.Short() || raceBuild {
		t.Skip("timing comparison (not under -short or the race detector)")
	}
	build := func(n int) positions {
		p := make(positions, n)
		for i := range p {
			p[i] = 2 * i
		}
		return p
	}
	fastest := func(p positions) time.Duration {
		best := time.Duration(1<<63 - 1)
		end := 2 * len(p)
		for range 5 {
			start := time.Now()
			for i := range 20000 {
				from := end - 1 - i%64
				p.next(from, end)
				p.between(from, end)
			}
			if d := time.Since(start); d < best {
				best = d
			}
		}
		return best
	}
	small, large := build(100000), build(400000)
	ts, tl := fastest(small), fastest(large)
	if tl > 2*ts {
		t.Errorf("position lookups grow with the positions: %v for %d, %v for %d", ts, len(small), tl, len(large))
	}
}

// TestRound24ContractionsConditionsAndJoins — review round 24: a negation as a
// contraction ("doesn't"), a negation before the exception in its clause,
// conditions the list missed, "e.g." hiding a condition behind a false
// sentence end, more second-version joins, "excluding", a version followed by
// letters, and headers behind "%%" / "!!" comment markers.
func TestRound24ContractionsConditionsAndJoins(t *testing.T) {
	lead := "This program is free software; you can redistribute it under the terms of the GNU General Public License "
	kept := map[string]string{
		"doesn't":        lead + "version 2 with the Classpath exception, which doesn't apply to files in lib/.",
		"won't":          lead + "version 2 with the Classpath exception, which won't apply to the tools.",
		"isn't":          lead + "version 2 with the Classpath exception, which isn't granted for the tests.",
		"cannot":         lead + "version 2 with the Classpath exception, which cannot be used for the daemon.",
		"LLVM don't":     "Licensed under the Apache License v2.0 with LLVM Exceptions, which don't apply to files under test/.",
		"no ... before":  "No file here is free software under the GNU General Public License version 2 with the Classpath exception.",
		"as long as":     lead + "version 2 or any later version, as long as the author agrees.",
		"conditional on": lead + "version 2 or any later version, conditional on the author's consent.",
		"discretion":     lead + "version 2 or any later version, at the author's discretion.",
		"e.g. if":        lead + "version 2 or any later version, e.g. if approved by the author.",
		"thru":           lead + "version 2 thru 3.",
		"until":          lead + "version 2 until 3.",
		"up to":          lead + "version 2 up to 3.",
		"[or 3]":         lead + "version 2 [or 3].",
		"excluding":      lead + "version 2, excluding the files in lib/.",
		"version 3a":     lead + "version two. Some parts under version 3a.",
		"GPLv2x":         lead + "version 3 or GPLv2x.",
	}
	for label, text := range kept {
		assertKeptAsText(t, label, text)
	}
	for _, marker := range []string{"%% ", "!! ", "% "} {
		if got := NormalizeLicenseToSPDX(commented(fsfGPL3Notice, "", marker, "")); got != "GPL-3.0-or-later" {
			t.Errorf("the FSF GPL-3 notice behind %q = %q, want GPL-3.0-or-later", marker, got)
		}
	}
}

// fsfGPL3PartOf is the FSF "This file is part of" GPL-3 header, whose line
// break falls inside "any later version".
const fsfGPL3PartOf = `This file is part of Foo.

Foo is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later
version.`

// TestRound25MarkerCompanionsApostrophesAndClauseRest — review round 25:
// comment markers with a companion character ("//!", "#'", "-- |", "#:",
// "%!"), typographic apostrophes in contractions, a range followed by more of
// its clause (decided as a class: only the FSF's "as published by the Free
// Software Foundation" may follow), a "License, 2 copies" count, and versions
// run into letters.
func TestRound25MarkerCompanionsApostrophesAndClauseRest(t *testing.T) {
	for _, marker := range []string{"//! ", "#' ", "-- | ", "#: ", "%! ", "!% ", "/// ", "## "} {
		if got := NormalizeLicenseToSPDX(commented(fsfGPL3PartOf, "", marker, "")); got != "GPL-3.0-or-later" {
			t.Errorf("the FSF part-of header behind %q = %q, want GPL-3.0-or-later", marker, got)
		}
	}
	lead := "This program is free software; you can redistribute it under the terms of the GNU General Public License "
	kept := map[string]string{
		"doesn\u2019t":        lead + "version 2 with the Classpath exception, which doesn\u2019t apply to this file.",
		"doesn\u02bct":        lead + "version 2 with the Classpath exception, which doesn\u02bct apply to this file.",
		"pending approval":    lead + "either version 2 of the License, or (at your option) any later version, pending approval by the author.",
		"holder's approval":   lead + "either version 2 of the License, or (at your option) any later version, with the copyright holder's approval.",
		"with permission":     lead + "either version 2 of the License, or (at your option) any later version, with the author's permission.",
		"once agrees":         lead + "either version 2 of the License, or (at your option) any later version, once the author agrees.",
		"except contrib":      lead + "version 2 or any later version, except the files in contrib.",
		"License, 2 copies":   "Licensed under the GNU General Public License, 2 copies of which are included in this repository for convenience.",
		"version 3rd edition": lead + "version 3rd edition.",
		"version 2x":          lead + "version 2x.",
		"v2.0rc1":             lead + "v2.0rc1 as published.",
	}
	for label, text := range kept {
		assertKeptAsText(t, label, text)
	}
	if got := NormalizeLicenseToSPDX(lead + "version 2 or (at your option) any later version, as published by the Free Software Foundation."); got != "GPL-2.0-or-later" {
		t.Errorf("the FSF's own \"as published by\" after a range = %q, want GPL-2.0-or-later", got)
	}
}

// TestRound26LicenseNumbersPointersAndMarkers — review round 26: an undotted
// number after "License" hid a second version instead of keeping the text; a
// "see ..." pointer let a condition follow it; and the FSF part-of header
// behind the Fortran/Doxygen and batch markers.
func TestRound26LicenseNumbersPointersAndMarkers(t *testing.T) {
	lead := "This program is free software; you can redistribute it under the terms of the GNU General Public License "
	fsf2 := lead + "as published by the Free Software Foundation; either version 2 of the License, or (at your option) any later version"
	kept := map[string]string{
		"also License 3":      lead + "version 2 as published by the Free Software Foundation. It may also be distributed under the GNU General Public License 3.",
		"or License 3":        lead + "version 2 as published by the FSF, or the GNU General Public License 3 as published by the FSF.",
		"License 3, see 2":    "This program is free software; you can redistribute it under the terms of the GNU General Public License 3 as published by the Free Software Foundation; see the GNU General Public License version 2 for details.",
		"Apache or License 1": `Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License, or under the License 1 as you choose.`,
		"see, but only":       fsf2 + ", see COPYING, but only with the written permission of the author.",
		"see unless":          fsf2 + ", see COPYING unless the author revokes this grant.",
		"(see) if approved":   fsf2 + " (see COPYING) if approved by the author.",
	}
	for label, text := range kept {
		assertKeptAsText(t, label, text)
	}
	// A range phrase that ends in punctuation of its own: the clause's
	// trimmed end falls before the phrase's end.
	if got := NormalizeLicenseToSPDX(lead + "version 2 or any later version, (at your option)."); got != "GPL-2.0-or-later" {
		t.Errorf("a range ending in its own parenthesis = %q, want GPL-2.0-or-later", got)
	}
	for _, text := range []string{fsf2 + ", see COPYING.", fsf2 + " (see the COPYING file).", fsf2 + ", see https://www.gnu.org/licenses/."} {
		if got := NormalizeLicenseToSPDX(text); got != "GPL-2.0-or-later" {
			t.Errorf("a pointer after the range: %.60q... = %q, want GPL-2.0-or-later", text[len(text)-40:], got)
		}
	}
	for _, marker := range []string{"!> ", "!< ", ":: ", "//!< "} {
		if got := NormalizeLicenseToSPDX(commented(fsfGPL3PartOf, "", marker, "")); got != "GPL-3.0-or-later" {
			t.Errorf("the FSF part-of header behind %q = %q, want GPL-3.0-or-later", marker, got)
		}
	}
}

// TestRound27PointersAndVersionSpellings — review round 27 (observed, taken):
// a "see" pointer to another license's URL or an unrelated token is not a
// pointer to this license's text, and "version III", "the third version" and
// "version-3" name a second version.
func TestRound27PointersAndVersionSpellings(t *testing.T) {
	lead := "This program is free software; you can redistribute it under the terms of the GNU General Public License "
	fsf2 := lead + "as published by the Free Software Foundation; either version 2 of the License, or (at your option) any later version"
	kept := map[string]string{
		"see another license URL": fsf2 + ", see https://polyformproject.org/noncommercial/1.0.0.",
		"see an unrelated token":  fsf2 + ", see NONCOMMERCIAL-USE-ONLY.",
		"version III":             lead + "version 2, or version III.",
		"the third version":       lead + "version 2, or the third version.",
		"version-3":               lead + "version 2 or version-3.",
	}
	for label, text := range kept {
		assertKeptAsText(t, label, text)
	}
	for _, text := range []string{fsf2 + ", see COPYING.", fsf2 + ", see LICENSE.txt.", fsf2 + ", see https://www.gnu.org/licenses/."} {
		if got := NormalizeLicenseToSPDX(text); got != "GPL-2.0-or-later" {
			t.Errorf("a pointer to this license's text = %q, want GPL-2.0-or-later", got)
		}
	}
}

// TestVersionsMustBeTiedToTheLicense — review round 28, decided as a class: a
// version mention that belongs to something else in the same sentence ("the
// second version of this file", "version iii is the stable branch", "the
// version 3 branch") was read as the license's version. A mention now counts
// only when it is tied to the license: part of the name ("GPLv2", "Apache
// License 2.0"), right after a name with only spaces, commas, colons or
// parentheses between ("GNU General Public License, version 2"), or followed
// by "of the ... License" ("version 2 of the License").
func TestVersionsMustBeTiedToTheLicense(t *testing.T) {
	kept := map[string]string{
		"second version of this file": "This program is licensed under the GNU General Public License; the second version of this file adds a GUI.",
		"second version of codebase":  "Released under the GNU General Public License, second version of the codebase, released in the year 2004",
		"third version of handbook":   "This program is licensed under the GNU General Public License (see the third version of the handbook).",
		"version iii branch":          "This program is licensed under the GNU General Public License; version iii is the stable release branch.",
		"version-3 branch":            "This program is licensed under the GNU General Public License; the version-3 branch is the stable release.",
		"version 3 branch (digits)":   "This program is licensed under the GNU General Public License; the version 3 branch is the stable release.",
		"version 3 of the tool":       "This program is free software under the GNU General Public License; this is version 3 of the tool.",
		"version two of this tool":    "This program is free software under the GNU General Public License; version two of this tool adds plugins.",
		"words in the gap":            "Released under the GNU General Public License by the maintainers, and version 3 is the stable release.",
	}
	for label, text := range kept {
		assertKeptAsText(t, label, text)
	}
	exact := map[string]string{
		"This program is free software under the GNU General Public License, version 2, as published by the Free Software Foundation.":                           "GPL-2.0-only",
		"This program is free software under the terms of the GNU General Public License (GPL) version 3 as published by the FSF.":                               "GPL-3.0-only",
		"This program is free software; you can redistribute it under version 2 of the GNU General Public License as published by the Free Software Foundation.": "GPL-2.0-only",
		"This program is free software under the GNU General Public License: version 3, as published by the Free Software Foundation.":                           "GPL-3.0-only",
		"This program is free software under the third version of the GNU General Public License, as published by the FSF.":                                      "GPL-3.0-only",
	}
	for text, want := range exact {
		if got := NormalizeLicenseToSPDX(text); got != want {
			t.Errorf("NormalizeLicenseToSPDX(%.80q...) = %q, want %q", text, got, want)
		}
	}
}

// fsfGPL2OldNotice is the older FSF GPL-2 header ("either version 2, or"),
// which thousands of real files carry, GCC's with version 3.
const fsfGPL2OldNotice = `This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2, or (at your option)
any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.`

// TestRound29FSFLeadSentenceAndNegation — review round 29: the round-28 tie
// rule rejected the FSF's own "as published by the Free Software Foundation;
// either version N" (the older header, GCC's), and it tied "version N of the
// License" in any sentence, negations included.
func TestRound29FSFLeadSentenceAndNegation(t *testing.T) {
	for _, f := range []struct{ open, marker, close string }{{"", "", ""}, {"/*", " * ", " */"}, {"", "# ", ""}, {"", "// ", ""}} {
		if got := NormalizeLicenseToSPDX(commented(fsfGPL2OldNotice, f.open, f.marker, f.close)); got != "GPL-2.0-or-later" {
			t.Errorf("the older FSF GPL-2 header behind %q = %q, want GPL-2.0-or-later", f.marker, got)
		}
	}
	lead := "This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation"
	exact := map[string]string{
		lead + "; either version 3, or (at your option) any later version.": "GPL-3.0-or-later",
		lead + ", version 3.": "GPL-3.0-only",
		lead + "; version 2.": "GPL-2.0-only",
	}
	for text, want := range exact {
		if got := NormalizeLicenseToSPDX(text); got != want {
			t.Errorf("NormalizeLicenseToSPDX(%.60q...) = %q, want %q", text[len(text)-60:], got, want)
		}
	}
	kept := map[string]string{
		"another sentence":         lead + ". Releases before 2010 were distributed under version 2 of the License.",
		"not covered":              "This program is free software under the terms of the GNU General Public License. This program is not covered by version 3 of the License.",
		"not available":            lead + ". It is not available under version 3 of the License.",
		"do not assume":            lead + ". Please do not assume version 2 of the License applies.",
		"plan to move":             "Licensed under the GNU General Public License. We plan to move to version 3 of the GPL next year.",
		"Apache, another sentence": "Licensed under the Apache License (see the NOTICE file). Do not use version 2.0 of the License for this file.",
		"relicensed":               lead + ", and may not be relicensed under version 3 of the License.",
		"relicensed, no negation":  lead + ", and was relicensed under version 3 of the License in 2010.",
		"licensee's code":          lead + ", and version 3 of the licensee's own code is separate.",
	}
	for label, text := range kept {
		assertKeptAsText(t, label, text)
	}
}

// TestRealHeaderVariantsFromTheCorpus — review round 29: shapes found by
// running 1,610 real license comment blocks (from source files in the module
// cache) through the reader against HEAD. Each read Apache-2.0 or GPL at HEAD
// and was text or wrong here until round 29.
func TestRealHeaderVariantsFromTheCorpus(t *testing.T) {
	apacheTail := ` you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0`
	exact := map[string]string{
		// A colon where the standard header has a semicolon.
		`Licensed under the Apache License, Version 2.0 (the "License"):` + apacheTail: "Apache-2.0",
		// Quote variants around "License".
		`Licensed under the Apache License, Version 2.0 (the 'License');` + apacheTail:           "Apache-2.0",
		"Licensed under the Apache License, Version 2.0 (the \u201cLicense\u201d);" + apacheTail: "Apache-2.0",
		`Licensed under the Apache License, Version 2.0 (the \"License\");` + apacheTail:         "Apache-2.0",
		// A JSDoc @license tag that agrees.
		commented(`Licensed under the Apache License, Version 2.0 (the "License");`+apacheTail+"\n\n@license Apache-2.0", "/*", " * ", " */"): "Apache-2.0",
		// The ASF header behind m4's dnl, behind "REM *", and repeated.
		commented(asfHeader, "", "dnl ", ""):                                                       "Apache-2.0",
		commented(commented(asfHeader, "/*", " * ", " */"), "", "REM ", ""):                        "Apache-2.0",
		commented(asfHeader, "/*", " * ", " */") + "\n" + commented(asfHeader, "/*", " * ", " */"): "Apache-2.0",
		// The ASF template with another owner.
		`Licensed to Elasticsearch B.V. under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. Elasticsearch B.V. licenses this file to you under the Apache License, Version 2.0 (the "License");` + apacheTail: "Apache-2.0",
		// MPL 2.0 named with a bare version.
		"Mozilla Public License 2.0 (MPL 2.0) - see https://www.mozilla.org/en-US/MPL/2.0/ for details": "MPL-2.0",
	}
	for text, want := range exact {
		if got := NormalizeLicenseToSPDX(text); got != want {
			t.Errorf("NormalizeLicenseToSPDX(%.80q...) = %q, want %q", text, got, want)
		}
	}
	// The MPL 1.1 tri-license block is not MPL-2.0 (it read so once the GPL
	// reader stopped answering it, round 29).
	assertKeptAsText(t, "MPL 1.1 tri-license", "***** BEGIN LICENSE BLOCK *****\nVersion: MPL 1.1/GPL 2.0/LGPL 2.1\n\nThe contents of this file are subject to the Mozilla Public License Version 1.1 (the \"License\"); you may not use this file except in compliance with the License.")
}

// TestRound30TagsURLsAndCommonVariants — review round 30: an @license tag's
// expression was never read (only the word "license" was accounted), a
// version inside the FSF's "If not, see <.../gpl-2.0.html>" URL fell in a
// negation's clause, "DO NOT EDIT!" ran into the header after it, and common
// headers the base also missed: Linux's "under the terms of version 2 of the
// GNU General Public License", "Mozilla Public\nLicense" wrapped, and the
// FSF's "Free Software Foundation, Inc.; either" / "(FSF); either".
func TestRound30TagsURLsAndCommonVariants(t *testing.T) {
	apache := `Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.`
	// An @license tag heading the text is the author's statement, as an
	// SPDX-License-Identifier tag is: the answer is its whole expression,
	// never the one term the prose also names.
	for tag, want := range map[string]string{
		"@license Apache-2.0 OR Python-2.0":         "Apache-2.0 OR Python-2.0",
		"@license Apache-2.0 AND OpenSSL":           "Apache-2.0 AND OpenSSL",
		"@license (Apache-2.0 OR Unicode-DFS-2016)": "Apache-2.0 OR Unicode-DFS-2016",
	} {
		if got := NormalizeLicenseToSPDX(commented(tag+"\n"+apache, "/**", " * ", " */")); got != want {
			t.Errorf("%s over an Apache header = %q, want %q", tag, got, want)
		}
	}
	// "!" ends a sentence: a version tied only through "of the GPL" in the
	// next one is not the license's.
	assertKeptAsText(t, "next sentence after !", "Licensed under the GNU General Public License! We plan to move to version 3 of the GPL next year.")
	// Lower in a notice, a tag must agree with the prose.
	assertKeptAsText(t, "@license disagreeing, lower down", apache+"\n\n@license Apache-2.0 OR Python-2.0")
	gpl2 := "This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License version 2 as published by the Free Software Foundation. You should have received a copy of the GNU General Public License version 2 along with this program; "
	exact := map[string]string{
		commented("@license Apache-2.0\n"+apache, "/**", " * ", " */"):                "Apache-2.0",
		gpl2 + "If not, see http://www.gnu.org/licenses/gpl-2.0.html":                 "GPL-2.0-only",
		gpl2 + "if not, see <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>.": "GPL-2.0-only",
		"DO NOT EDIT!\n" + apache: "Apache-2.0",
		"This program is free software; you can redistribute it and/or modify it under the terms of version 2 of the GNU General Public License as published by the Free Software Foundation.":                                                                  "GPL-2.0-only",
		"This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, Inc.; either version 2, or (at your option) any later version.":                "GPL-2.0-or-later",
		"This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation (FSF); either version 3 of the License, or (at your option) any later version.": "GPL-3.0-or-later",
	}
	for text, want := range exact {
		if got := NormalizeLicenseToSPDX(text); got != want {
			t.Errorf("NormalizeLicenseToSPDX(%.70q...) = %q, want %q", text, got, want)
		}
	}
}

// TestRound31TagValuesURLNegationsAndShebangs — review round 31: a
// value-less or prose "@license" (JSDoc's marker, "@license Copyright 2017
// Google Inc.") is not a tag; a negated grant whose only version is inside a
// URL is withdrawn; and only a real shebang ("#!") may stand before a heading
// tag on its line.
func TestRound31TagValuesURLNegationsAndShebangs(t *testing.T) {
	apache := `Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.`
	exact := map[string]string{
		commented("@license\nCopyright 2018 Google LLC. All Rights Reserved.\n"+apache, "/**", " * ", " */"): "Apache-2.0",
		commented("@license Copyright 2017 Google Inc. All Rights Reserved.\n"+apache, "/**", " * ", " */"):  "Apache-2.0",
	}
	for text, want := range exact {
		if got := NormalizeLicenseToSPDX(text); got != want {
			t.Errorf("NormalizeLicenseToSPDX(%.70q...) = %q, want %q", text, got, want)
		}
	}
	kept := map[string]string{
		"not distributed, URL only": "This file is not distributed under the GNU General Public License https://www.gnu.org/licenses/gpl-2.0.html any more.",
		"no longer, URL only":       "This file is no longer distributed under the GNU General Public License <https://www.gnu.org/licenses/gpl-2.0.html>.",
		"/*! before @license":       "/*! Portions of this bundle are GPL-3.0-only; the loader alone is @license MIT */\nvar x = 1;",
		"! before @license":         "! Most of this file is GPL-3.0-only. The helper below is @license MIT\n",
		"MPL in a dual, wrapped":    "This library is dual licensed under the MIT license or the Mozilla Public\nLicense 2.0, at your option.",
	}
	for label, text := range kept {
		assertKeptAsText(t, label, text)
	}
}

// TestRound32LicenseNoticesAndUnreadableTags — review round 32: "license
// notice(s)" is not a reference to this license ("Third-party files keep
// their own license notices" names other licenses), and an "@license" whose
// value is neither an expression, empty (JSDoc's bare marker) nor a copyright
// line states a license the reader cannot read, so the text is kept, as for
// an unreadable SPDX tag.
func TestRound32LicenseNoticesAndUnreadableTags(t *testing.T) {
	apache := `Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.`
	kept := map[string]string{
		"own license notices": "This program is distributed under the GNU General Public License v2. Third-party files keep their own license notices.",
		"a license notice":    "This program is distributed under the GNU General Public License v2. Some files carry a different license notice.",
		"@license Kopimi":     apache + "\n@license Kopimi",
		"@license OR Foo":     apache + "\n@license Apache-2.0 OR Foo-1.0",
		"@license list":       apache + "\n@license Apache-2.0, Foo",
		"@license WITH prose": commented("@license GPL-2.0-only WITH Classpath\nThis program is free software; you can redistribute it under the terms of the GNU General Public License version 2.", "/**", " * ", " */"),
		"@license PHPDoc URL": commented("@license http://www.apache.org/licenses/LICENSE-2.0 Apache License 2.0\n"+apache, "/**", " * ", " */"),
		// Round 33: an SPDX tag on an "@license Copyright ..." line is still
		// read, and an empty SPDX tag keeps the text.
		"SPDX tag on the @license line": commented("@license Copyright 2020 Foo SPDX-License-Identifier: Kopimi\n"+apache, "/**", " * ", " */"),
		"empty SPDX tag":                "SPDX-License-Identifier:\n" + apache,
		// A copyright line starts the value; "copyright" later in an
		// unreadable value does not make it prose (round 36).
		"@license Kopimi, copyright ...": commented("@license Kopimi, copyright 2020 Foo\n"+apache, "/**", " * ", " */"),
	}
	for label, text := range kept {
		assertKeptAsText(t, label, text)
	}
}

// TestHeadingTagsFollowTheSameTagRule — review round 34: the heading-tag
// reader (spdxIdentifierLine) still used the one-pass union of both tag
// forms, so a bare "@license" or "@license Copyright ..." counted there as an
// unreadable tag, and an SPDX tag on an "@license" line was swallowed. Both
// readers now find tags the same way (SR-17).
func TestHeadingTagsFollowTheSameTagRule(t *testing.T) {
	for text, want := range map[string]string{
		// "(c)" and "\u00a9" lines are copyright lines for both rules (round 35).
		"/**\n * @license (c) 2020 Foo Inc.\n * SPDX-License-Identifier: MIT\n * The helpers below are bundled from the foo package.\n */":    "MIT",
		"/**\n * @license \u00a9 2020 Foo Inc.\n * SPDX-License-Identifier: MIT\n * The helpers below are bundled from the foo package.\n */": "MIT",
		// The earliest tag heads the text: an @license at the top, an SPDX
		// tag later in a bundle.
		"/** @license Apache-2.0 */\nconst helpers = require('./helpers'); // bundled from helpers.js, SPDX-License-Identifier: Apache-2.0": "Apache-2.0",
		// Heading the text (after markers, a bare @license and a copyright
		// line), the tag is the author's statement: its whole expression.
		"/**\n * @license Copyright 2020 Foo SPDX-License-Identifier: Apache-2.0 OR BlueOak-1.0.0\n * Licensed under the Apache License, Version 2.0 (the \"License\").\n */": "Apache-2.0 OR BlueOak-1.0.0",
		"// SPDX-License-Identifier: Apache-2.0\n\n/**\n * @license\n * Copyright 2024 Google Inc.\n */":                                                                      "Apache-2.0",
		"// SPDX-License-Identifier: Apache-2.0\n\n/**\n * @license Copyright 2024 Google Inc.\n */":                                                                          "Apache-2.0",
		"/**\n * @license SPDX-License-Identifier: Apache-2.0 */\n/* The product text follows below, see the documentation. */":                                               "Apache-2.0",
	} {
		if got := NormalizeLicenseToSPDX(text); got != want {
			t.Errorf("NormalizeLicenseToSPDX(%q) = %q, want %q", text, got, want)
		}
	}
}

// mitBody and bsd3Body are the MIT and BSD-3-Clause texts as registries and
// LICENSE files carry them (the fingerprints read them by wording).
const (
	mitBody  = `Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction. THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND.`
	bsd3Body = `Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products.`
)

// TestEveryFingerprintHonoursTags — review round 38: the tag rules (a tag
// must be readable and agree with the text) held only in the GPL and Apache
// readers; the MIT, ISC, PSF, BSD, MPL, Unlicense and CC0 fingerprints never
// looked at tags. Real files read BSD-3-Clause under a "BSD-3-Clause AND
// MPL-2.0" tag (cyphar/filepath-securejoin) and under "Apache-2.0 OR
// BSD-3-Clause" (dd-trace-go), dropping a term or a choice. One agreement
// check now covers every fingerprint's answer.
func TestEveryFingerprintHonoursTags(t *testing.T) {
	kept := map[string]string{
		"BSD body, tag AND MPL":   bsd3Body + "\n\nSPDX-License-Identifier: BSD-3-Clause AND MPL-2.0",
		"BSD body, tag OR":        bsd3Body + "\n\nSPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause",
		"MIT body, lower GPL tag": mitBody + "\n\nSPDX-License-Identifier: GPL-3.0-only",
		"MIT body, unreadable":    "SPDX-License-Identifier: Acme-Custom\n\n" + mitBody,
	}
	for label, text := range kept {
		assertKeptAsText(t, label, text)
	}
	if got := NormalizeLicenseToSPDX(mitBody + "\n\nSPDX-License-Identifier: MIT"); got != "MIT" {
		t.Errorf("an MIT body with an agreeing tag = %q, want MIT", got)
	}
}

// TestHeadingTagsBehindEveryCommentMarker — review round 38: the heading
// check knew fewer comment markers than the marker stripper (Erlang "%%",
// elisp ";;", Rust "///" and "//!", "REM", m4 "dnl"). Both now use one
// marker spelling (SR-17).
func TestHeadingTagsBehindEveryCommentMarker(t *testing.T) {
	for _, m := range []string{"%% ", ";; ", "/// ", "//! ", "REM ", "dnl ", "# ", "// "} {
		text := m + "SPDX-License-Identifier: Apache-2.0\n" + m + "Copyright 2024 Example Corp. The module below is part of the example project."
		if got := NormalizeLicenseToSPDX(text); got != "Apache-2.0" {
			t.Errorf("a heading tag behind %q = %q, want Apache-2.0", m, got)
		}
	}
}

// TestTwoLicenseTextsInOneFieldStayText — review round 39: an Apache-2.0 body
// followed by a BSD or MIT text (go.opentelemetry.io/otel's LICENSE: Apache,
// then the Go Authors' BSD) read BSD-3-Clause or MIT, dropping Apache: the
// MIT/ISC/PSF/BSD fingerprints ran first and won. Two license texts in one
// field are not one license.
func TestTwoLicenseTextsInOneFieldStayText(t *testing.T) {
	apacheBody := readLicenseFixture(t, "Apache-2.0.txt")
	gpl2Body := readLicenseFixture(t, "GPL-2.0.txt")
	kept := map[string]string{
		"Apache + BSD-3": apacheBody + "\n\n" + bsd3Body,
		"Apache + MIT":   apacheBody + "\n\n" + mitBody,
		"BSD-3 + Apache": bsd3Body + "\n\n" + apacheBody,
		"GPL-2.0 + MIT":  gpl2Body + "\n\n" + mitBody,
	}
	for label, text := range kept {
		assertKeptAsText(t, label, text)
	}
	for text, want := range map[string]string{apacheBody: "Apache-2.0", mitBody + " Copyright 2020 Example Corp and its contributors, all of them.": "MIT"} {
		if got := NormalizeLicenseToSPDX(text); got != want {
			t.Errorf("one license text alone = %q, want %q", got, want)
		}
	}
}

// TestBSDClauseCount — the BSD fingerprint tells BSD-3-Clause from
// BSD-2-Clause by the third clause ("Neither the name ..."); nothing pinned
// the split until round 39 moved it into permissiveTextID.
func TestBSDClauseCount(t *testing.T) {
	bsd2 := "Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met: redistributions of source code must retain the above copyright notice."
	for text, want := range map[string]string{bsd3Body: "BSD-3-Clause", bsd2: "BSD-2-Clause"} {
		if got := NormalizeLicenseToSPDX(text); got != want {
			t.Errorf("NormalizeLicenseToSPDX(%.50q...) = %q, want %q", text, got, want)
		}
	}
}

// TestCommentedLicenseBodiesCountAsBodies — review round 40: a GNU or Apache
// body wrapped in comment markers is still a body. The two gates that ask
// "does this text hold a GNU or Apache body" (a heading tag's, and the
// two-license-texts gate of round 39) read the text with its markers, so a
// title split over two commented lines hid the body: an MIT tag above a
// commented Apache body read MIT, and a commented Apache body plus the MIT
// grant read MIT, dropping Apache. Both keep the text, as their plain forms do.
func TestCommentedLicenseBodiesCountAsBodies(t *testing.T) {
	apacheBody := readLicenseFixture(t, "Apache-2.0.txt")
	comment := func(prefix, s string) string {
		lines := strings.Split(s, "\n")
		for i, l := range lines {
			lines[i] = prefix + l
		}
		return strings.Join(lines, "\n")
	}
	for name, text := range map[string]string{
		"tag above a commented body": "# SPDX-License-Identifier: MIT\n#\n" + comment("# ", apacheBody),
		"commented body and MIT":     comment("// ", apacheBody+"\n\n"+mitBody),
	} {
		plain := strings.NewReplacer("# ", "", "// ", "").Replace(text)
		if got := NormalizeLicenseToSPDX(plain); spdx.Valid(got) {
			t.Fatalf("%s: the plain form reads %q; the fixture no longer shows the gap", name, got)
		}
		if got := NormalizeLicenseToSPDX(text); spdx.Valid(got) {
			t.Errorf("%s: NormalizeLicenseToSPDX = %q, want the text kept", name, got)
		}
	}
	// The commented body alone is still read.
	if got := NormalizeLicenseToSPDX(comment("// ", apacheBody)); got != "Apache-2.0" {
		t.Errorf("commented Apache body = %q, want Apache-2.0", got)
	}
}

// TestLicenseStatementAboveHeadingTagKeepsText — review round 40: a copyright
// line may stand above a heading tag, but not one that states a license; two
// statements are not the tag's alone (no real file of 541 tagged headers has
// one, so the class costs nothing).
func TestLicenseStatementAboveHeadingTagKeepsText(t *testing.T) {
	for _, text := range []string{
		"// Copyright 2020 Foo Inc. Licensed under the GNU GPL v3 except the glue below\n// SPDX-License-Identifier: MIT\nint x;\n and some more text here to be long enough",
		"// Copyright 2020 Foo Inc. Licensed under the Apache License, Version 2.0\n// SPDX-License-Identifier: MIT\nint x;\n and some more text here to be long enough",
		"// Copyright 2020 Foo Inc., Apache 2.0\n// SPDX-License-Identifier: MIT\nint x;\n and some more text here to be long enough",
	} {
		if got := NormalizeLicenseToSPDX(text); spdx.Valid(got) {
			t.Errorf("NormalizeLicenseToSPDX(%.60q...) = %q, want the text kept", text, got)
		}
	}
	// A plain copyright line still stands above the tag.
	if got := NormalizeLicenseToSPDX("// Copyright 2020 Foo Inc. All rights reserved.\n// SPDX-License-Identifier: MIT\nint x;\n and some more text here to be long enough"); got != "MIT" {
		t.Errorf("plain copyright line above the tag = %q, want MIT", got)
	}
}

// TestRound41HeaderLinesAndSecondTexts — review round 41:
//   - a license name glued to its version ("AGPLv3", "LGPLv2.1+", "Apache2",
//     "MPLv2", "EUPLv1.2") on a copyright line is a license statement, while
//     a holder named like a license ("Internet Systems Consortium, Inc.
//     ("ISC")", BIND's header on every file; "The Apache Software
//     Foundation"; "MIT CSAIL") is not one;
//   - FreeBSD's style(9) license-comment opener "/*-" is a comment marker;
//   - a GNU or Apache body next to an ISC, Unlicense or CC0 text is two
//     license texts (the Apache and GPL readers answered first).
func TestRound41HeaderLinesAndSecondTexts(t *testing.T) {
	tail := "\nint x;\n and some more text here to be long enough to be read as a header\n"
	for _, name := range []string{"AGPLv3", "LGPLv2.1+", "Apache2", "MPLv2", "EUPLv1.2"} {
		text := "/*\n// Copyright (c) 2020 Example Corp, " + name + "\n//\n// SPDX-License-Identifier: MIT" + tail
		if got := NormalizeLicenseToSPDX(text); spdx.Valid(got) {
			t.Errorf("copyright line stating %s above an MIT tag = %q, want the text kept", name, got)
		}
	}
	// Decided as a class in round 42 (the third round on this boundary): a
	// line above the tag that names any listed license, in any spelling,
	// keeps the text. A holder named like a license keeps it too, as base
	// did; a version-spelling list was never complete ("MPL version 2.0",
	// "CC-BY-SA-4.0", "Apache/2.0").
	for _, line := range []string{
		`Copyright (C) Internet Systems Consortium, Inc. ("ISC")`,
		"Copyright (c) 2020 The Apache Software Foundation",
		"Copyright (c) 2020 Foo, MPL version 2.0",
		"Copyright (c) 2020 Foo, Apache version 2.0",
		"Copyright (c) 2020 Foo Corporation, EPL version 2.0",
		"Copyright (c) 2020 Foo Corporation, CC-BY-SA-4.0",
		"Copyright (c) 2020 Foo, CC BY-NC 4.0",
		"Copyright (c) 2020 Foo, Apache/2.0",
	} {
		text := "/*-\n * " + line + "\n *\n * SPDX-License-Identifier: MIT" + tail
		if got := NormalizeLicenseToSPDX(text); spdx.Valid(got) {
			t.Errorf("line %q above an MIT tag = %q, want the text kept", line, got)
		}
	}
	// A plain holder still stands above the tag.
	for _, holder := range []string{"Copyright (c) 2013 Mitchell Hashimoto", "Copyright (c) 2020 The Kubernetes Authors.", "Copyright (C) 2004-2020 Example Corp. All rights reserved."} {
		if got := NormalizeLicenseToSPDX("/*\n * " + holder + "\n *\n * SPDX-License-Identifier: MIT" + tail); got != "MIT" {
			t.Errorf("holder %q above an MIT tag = %q, want MIT", holder, got)
		}
	}
	freebsd := "/*-\n * SPDX-License-Identifier: BSD-2-Clause-FreeBSD\n *\n * Copyright (c) 2002 Example Author\n * All rights reserved.\n *\n * Redistribution and use in source and binary forms, with or without\n * modification, are permitted provided that the following conditions\n * are met:\n */\n"
	if got := NormalizeLicenseToSPDX(freebsd); got != "BSD-2-Clause-FreeBSD" {
		t.Errorf("FreeBSD /*- header = %q, want BSD-2-Clause-FreeBSD", got)
	}
	apacheBody := readLicenseFixture(t, "Apache-2.0.txt")
	gpl3Body := readLicenseFixture(t, "GPL-3.0.txt")
	isc := "ISC License\n\nCopyright (c) 2012 Example\n\nPermission to use, copy, modify, and/or distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.\n"
	unlicense := "This is free and unencumbered software released into the public domain.\n\nAnyone is free to copy, modify, publish, use, compile, sell, or distribute this software.\n"
	cc0 := "Creative Commons Legal Code\n\nCC0 1.0 Universal\n\nThe person who associated a work with this deed has dedicated the work to the public domain.\n"
	for name, text := range map[string]string{
		"Apache + ISC":       apacheBody + "\n\n" + isc,
		"Apache + Unlicense": apacheBody + "\n\n" + unlicense,
		"Apache + CC0":       apacheBody + "\n\n" + cc0,
		"GPL-3.0 + ISC":      gpl3Body + "\n\n" + isc,
		// Round 42: CGAL's LICENSE is prose naming the GPL and, for the
		// examples, CC0; the GPL reader declines, and CC0 must not answer.
		"GPL prose + CC0 examples": "The library is licensed under the GNU General Public License, version 3 or later. All examples and demos are licensed under the Creative Commons CC0 1.0 license.",
		"GPL prose + Unlicense":    "The library is licensed under the GNU General Public License, version 3 or later, or the Unlicense. This is free and unencumbered software released into the public domain.",
	} {
		if got := NormalizeLicenseToSPDX(text); spdx.Valid(got) {
			t.Errorf("%s = %q, want the text kept (two license texts)", name, got)
		}
	}
}

// TestRound43 — review round 43:
//   - ninja's COPYING is the Apache-2.0 text titled "January 2010"; base read
//     it Apache-2.0, and the body marker's exact date kept it text;
//   - "names the GNU GPL" includes "GNU GPL" and "GPLv3", not only the words
//     spelled out;
//   - a whole word is whole in any script: "Mitä Oy", "Émit Systèmes" and
//     "Rødual AS" are holders, not MIT or "dual".
func TestRound43(t *testing.T) {
	apacheBody := readLicenseFixture(t, "Apache-2.0.txt")
	ninja := strings.Replace(apacheBody, "January 2004", "January 2010", 1)
	if ninja == apacheBody {
		t.Fatal("the Apache fixture no longer carries the January 2004 title")
	}
	if got := NormalizeLicenseToSPDX(ninja); got != "Apache-2.0" {
		t.Errorf("Apache text titled January 2010 = %q, want Apache-2.0", got)
	}
	for _, text := range []string{
		"This is free and unencumbered software released into the public domain.\n\nAlternatively, this software may be used under the terms of the GNU GPL version 3.",
		"Parts of this package are licensed under the GNU GPLv3; the example files are released under Creative Commons CC0 1.0 Universal (public domain dedication).",
	} {
		if got := NormalizeLicenseToSPDX(text); spdx.Valid(got) {
			t.Errorf("NormalizeLicenseToSPDX(%.50q...) = %q, want the text kept", text, got)
		}
	}
	tail := "\n// SPDX-License-Identifier: MIT\n\npackage foo and some more text here to be long enough\n"
	for _, holder := range []string{"// Copyright (c) 2020 Mitä Oy", "// Copyright (c) 2019 Émit Systèmes SARL", "// Copyright 2020 Rødual AS"} {
		if got := NormalizeLicenseToSPDX(holder + tail); got != "MIT" {
			t.Errorf("holder %q above an MIT tag = %q, want MIT", holder, got)
		}
	}
}

// TestRound44 — review round 44:
//   - Chinese and Japanese put no spaces between words, so a license name
//     touching CJK letters ("采用MIT许可证", "MITライセンス") is still a name
//     on a line above the tag; only letters of spaced scripts (Latin,
//     Greek, Cyrillic) make it part of a longer word ("Mitä", "Rødual");
//   - a GPL name wrapped across lines ("GNU General Public\nLicense") is
//     still named, as the GPL reader reads it (folded).
func TestRound44(t *testing.T) {
	for _, text := range []string{
		"Copyright (c) 2021 某公司。本项目采用MIT许可证。\nSPDX-License-Identifier: Apache-2.0\n",
		"/*\n * Copyright (c) 2021 Foo 株式会社 MITライセンスで公開\n * SPDX-License-Identifier: GPL-2.0-or-later\n */\n",
		"// Copyright 2020 Foo, 本软件遵循GPL协议\n// SPDX-License-Identifier: MIT\n\npackage foo and some more text here\n",
		"// Copyright 2020 Foo, 依據Apache授權\n// SPDX-License-Identifier: MIT\n\npackage foo and some more text here\n",
		"The documentation is dedicated to the public domain under the Creative Commons CC0 1.0\ndedication. The code is licensed under the GNU General Public\nLicense, version 3.",
		"This is free and unencumbered software released into the public domain, except the\nplugins, which are licensed under the GNU General Public\nLicense, version 3.",
	} {
		if got := NormalizeLicenseToSPDX(text); spdx.Valid(got) {
			t.Errorf("NormalizeLicenseToSPDX(%.60q...) = %q, want the text kept", text, got)
		}
	}
	// Greek and Cyrillic neighbours are part of the word, as Latin ones are.
	for _, holder := range []string{"// Copyright 2020 Mitκο Α.Ε.", "// Copyright 2020 Mitя ООО"} {
		if got := NormalizeLicenseToSPDX(holder + "\n// SPDX-License-Identifier: MIT\n\npackage foo and some more text here\n"); got != "MIT" {
			t.Errorf("holder %q above an MIT tag = %q, want MIT", holder, got)
		}
	}
}

// TestRound45 — review round 45:
//   - a GPL name wrapped across commented lines is still named: namesTheGPL
//     reads the text as the body readers do (markers stripped, then folded);
//   - a digit or underscore before a name is an edge ("0BSD" names BSD):
//     only a spaced-script letter joins a name to a longer word.
func TestRound45(t *testing.T) {
	for _, text := range []string{
		"# To the extent possible under law, the author has dedicated all copyright and related rights to this software to the public domain worldwide under the Creative Commons CC0 1.0 Universal dedication.\n# Parts are covered by the GNU General Public\n# License version 2 instead.\n",
		" * This is free and unencumbered software released into the public domain.\n * Parts are covered by the GNU General Public\n * License version 2 instead.\n",
		"// This is free and unencumbered software released into the public domain.\n// Parts are covered by the GNU General Public\n// License version 2 instead.\n",
		"// Copyright 2020 Foo, 0BSD\n// SPDX-License-Identifier: MIT\n\npackage foo\n// and some more text here to be long enough to be read as a header ok\n",
		"// Copyright 2020 Foo, lib_mit\n// SPDX-License-Identifier: Apache-2.0\n\npackage foo\n// and some more text here to be long enough to be read as a header ok\n",
	} {
		if got := NormalizeLicenseToSPDX(text); spdx.Valid(got) {
			t.Errorf("NormalizeLicenseToSPDX(%.60q...) = %q, want the text kept", text, got)
		}
	}
}

// TestWorklist61ExactMPLCC0Unlicense — worklist 61: the MPL, CC0 and
// Unlicense wordings are read exactly, as the GPL and Apache ones are: a
// license body by its own title and wording with nothing else stated around
// it, or a notice through the notice reader (every license word accounted
// for, another license keeps the text, the version tied to the name). A
// mention in prose keeps the text. The baseline (2026-09-25, scratchpad w61)
// had six real CC-BY-4.0 texts reading CC0-1.0 ("creative commons" and
// "public domain").
func TestWorklist61ExactMPLCC0Unlicense(t *testing.T) {
	mpl := readLicenseFixture(t, "MPL-2.0.txt")
	cc0 := readLicenseFixture(t, "CC0-1.0.txt")
	unl := readLicenseFixture(t, "Unlicense.txt")
	notice := "This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0. If a copy of the MPL was not distributed with this file, You can obtain one at http://mozilla.org/MPL/2.0/."
	comment := func(prefix, s string) string {
		lines := strings.Split(s, "\n")
		for i := range lines {
			lines[i] = prefix + lines[i]
		}
		return strings.Join(lines, "\n")
	}
	for name, c := range map[string]struct{ text, want string }{
		"MPL body":                        {mpl, "MPL-2.0"},
		"MPL body, HashiCorp title":       {strings.Replace(mpl, "Mozilla Public License Version 2.0", "Mozilla Public License, version 2.0", 1), "MPL-2.0"},
		"MPL body under a copyright line": {"Copyright (c) 2015 Example, Inc.\n\n" + mpl, "MPL-2.0"},
		"MPL notice":                      {notice, "MPL-2.0"},
		"MPL notice in comments":          {comment("// ", "This Source Code Form is subject to the terms of the Mozilla Public\nLicense, v. 2.0. If a copy of the MPL was not distributed with this\nfile, You can obtain one at http://mozilla.org/MPL/2.0/."), "MPL-2.0"},
		"MPL notice, no-copyleft exhibit": {notice + "\n\nThis Source Code Form is \"Incompatible With Secondary Licenses\", as defined by the Mozilla Public License, v. 2.0.", "MPL-2.0-no-copyleft-exception"},
		"MPL 1.1 notice":                  {`The contents of this file are subject to the Mozilla Public License Version 1.1 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.mozilla.org/MPL/`, "MPL-1.1"},
		"CC0 legal code":                  {cc0, "CC0-1.0"},
		"CC0 waiver":                      {"To the extent possible under law, the author(s) have dedicated all copyright and related and neighboring rights to this software to the public domain worldwide. This software is distributed without any warranty.\n\nYou should have received a copy of the CC0 Public Domain Dedication along with this software. If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.", "CC0-1.0"},
		"CC0 waiver, waived form":         {"To the extent possible under law, Jane Doe has waived all copyright and related or neighboring rights to this work. See https://creativecommons.org/publicdomain/zero/1.0/ for the CC0 dedication.", "CC0-1.0"},
		"Unlicense body":                  {unl, "Unlicense"},
		"Unlicense body with its heading": {"The Unlicense\n\n" + unl, "Unlicense"},
	} {
		if got := NormalizeLicenseToSPDX(c.text); got != c.want {
			t.Errorf("%s = %q, want %q", name, got, c.want)
		}
	}
	for name, text := range map[string]string{
		"MIT or MPL prose":             "This library is dual licensed under the MIT license or the Mozilla Public License 2.0, at your choice.",
		"MPL body with an MIT note":    mpl + "\n\nThe files under vendor/ are under the MIT license instead.",
		"MPL notice with an MIT note":  notice + " The build scripts are MIT licensed.",
		"MPL 1.1 tri-license":          "Version: MPL 1.1/GPL 2.0/LGPL 2.1\n\nThe contents of this file are subject to the Mozilla Public License Version 1.1 (the \"License\"); you may not use this file except in compliance with the License. Alternatively, the contents of this file may be used under the terms of either the GNU General Public License Version 2 or later (the \"GPL\"), or the GNU Lesser General Public License Version 2.1 or later (the \"LGPL\").",
		"CC-BY-4.0 text":               "Creative Commons Attribution 4.0 International Public License\n\nBy exercising the Licensed Rights (defined below), You accept and agree to be bound by the terms and conditions of this Creative Commons Attribution 4.0 International Public License. Licensed Material in the public domain is not covered.",
		"Creative Commons in prose":    "The code is under the MIT License; the documentation is under Creative Commons, and the logo is in the public domain.",
		"CC0 waiver with LGPL":         "To the extent possible under law, Jane Doe has waived all copyright and related or neighboring rights to this work under CC0 1.0. Parts under the LGPL.",
		"CC0 legal code with MIT note": cc0 + "\n\nThe examples/ directory is MIT licensed.",
		"Unlicense with MIT choice":    unl + "\n\nAlternatively, you may use this software under the MIT license.",
		"Unlicense named in prose":     "This is free and unencumbered software, but the parser is under the Apache License 2.0 and the data files are CC-BY.",
		"MPL notice with a range":      "This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0 or (at your option) any later version.",
		"Unlicense notice with MIT":    "This project is released into the public domain under the Unlicense, except the parser in lib/, which is MIT.",
	} {
		assertKeptAsText(t, name, text)
	}
}

// TestWorklist61TypographicQuotes — HashiCorp's MPL-2.0 LICENSE files write
// Exhibit B with typographic quotes (U+201C Incompatible With
// Secondary Licenses U+201D); about 93 real files lost their answer in the first
// worklist-61 draft, which spelled the end of the body with ASCII quotes.
func TestWorklist61TypographicQuotes(t *testing.T) {
	mpl := readLicenseFixture(t, "MPL-2.0.txt")
	curly := strings.ReplaceAll(strings.Replace(mpl, "Mozilla Public License Version 2.0", "Mozilla Public License, version 2.0", 1), `"Incompatible With Secondary Licenses"`, "\u201cIncompatible With Secondary Licenses\u201d")
	if curly == mpl || !strings.Contains(curly, "\u201c") {
		t.Fatal("the fixture no longer carries the ASCII-quoted Exhibit B")
	}
	if got := NormalizeLicenseToSPDX("Copyright (c) 2018 HashiCorp, Inc.\n\n" + curly); got != "MPL-2.0" {
		t.Errorf("HashiCorp-style MPL body = %q, want MPL-2.0", got)
	}
}

// TestWorklist61NoticeShapes — worklist-61 review round 1: a CC0 or
// Unlicense notice is a notice's shape (CC0's waiver clause, the Unlicense's
// opening dedication, "released under the Unlicense", or a field that is only
// the license's name and URL), not any text that names the license and has
// no license word. A negation in the grant's clause withdraws it.
func TestWorklist61NoticeShapes(t *testing.T) {
	for _, text := range []string{
		"This package is NOT released under the Unlicense; all rights reserved by Acme Corp, 2024. Contact sales before any use.",
		"These images are not CC0: do not redistribute the photographs in this directory without written permission from the photographer.",
		"We considered CC0 but chose to keep all rights reserved; please email acme@example.com before reusing any of this material.",
		"This file is not CC0; the images in this directory belong to the photographer, who retains all rights; ask before reuse.",
		"The demo scenes in this repository use CC0 textures downloaded from ambientCG; the textures are not part of the package.",
		"A C port of a hashing routine from a project released under the Unlicense, plus bindings written by Acme Corp; all rights reserved.",
		`// test fixture: {"spdxVersion":"SPDX-2.3","dataLicense":"CC0-1.0","SPDXID":"SPDXRef-DOCUMENT","name":"example"}`,
		"The code is under CC0, the documentation is under Creative Commons, and the logo belongs to Acme Corp with every right held.",
		"To the extent possible under law, Jane Doe has not waived all copyright and related or neighboring rights to this work (CC0).",
		"The Unlicense (Unlicense), https://unlicense.org/, https://opensource.org/licenses/MIT, https://spdx.org/licenses/",
		"CC0 1.0 Universal (CC0 1.0) Public Domain Dedication, Creative Commons, https://creativecommons.org/licenses/by/4.0/",
		"The Unlicense was considered for this project, but the maintainers have yet to pick a dedication for the parser code.",
	} {
		assertKeptAsText(t, text, text)
	}
	for text, want := range map[string]string{
		"This project is released under the Unlicense. For more information, please refer to <https://unlicense.org>.":                                                                                                                                 "Unlicense",
		"The Unlicense (Unlicense), https://unlicense.org/, https://spdx.org/licenses/Unlicense.html":                                                                                                                                                  "Unlicense",
		"This work is dedicated to the public domain under CC0. Its authors are Jane Doe and John Roe, 2024.":                                                                                                                                          "CC0-1.0",
		"To the extent possible under law, Pascal S. de Kloe has waived all copyright and related or neighboring rights to Go Enterprice. This work is published from The Netherlands.\n\nhttps://creativecommons.org/publicdomain/zero/1.0/legalcode": "CC0-1.0",
	} {
		if got := NormalizeLicenseToSPDX(text); got != want {
			t.Errorf("NormalizeLicenseToSPDX(%.50q) = %q, want %q", text, got, want)
		}
	}
}

// TestWorklist61Review2 — worklist-61 review round 2:
//   - certifi's LICENSE (vendored in every pip) wraps the standard MPL notice
//     in Mozilla's "***** BEGIN/END LICENSE BLOCK *****"; the wrapper refers
//     to the notice, it is not another license;
//   - a grant's license name must end its clause: "with CC0 assets",
//     "under CC0-style terms" use the name as an adjective;
//   - "nothing", "none", "neither ... nor" and "formerly" withdraw a grant;
//   - a name-only field's URL must be the license's own host, not
//     unlicense.org.example.com.
func TestWorklist61Review2(t *testing.T) {
	certifi := "This package contains a modified version of ca-bundle.crt:\n\nThis Source Code Form is subject to the terms of the Mozilla Public License,\nv. 2.0. If a copy of the MPL was not distributed with this file, You can obtain\none at http://mozilla.org/MPL/2.0/.\n\n***** END LICENSE BLOCK *****"
	for name, text := range map[string]string{
		"certifi":           "***** BEGIN LICENSE BLOCK *****\n" + certifi[strings.Index(certifi, "This Source"):],
		"notice + END only": certifi[strings.Index(certifi, "This Source"):],
	} {
		if got := NormalizeLicenseToSPDX(text); got != "MPL-2.0" {
			t.Errorf("%s = %q, want MPL-2.0", name, got)
		}
	}
	// certifi's whole LICENSE (the ca-bundle description names an
	// "Apache+mod_ssl webserver"): the block delimits the license statement.
	realCertifi := "This package contains a modified version of ca-bundle.crt:\n\nca-bundle.crt -- Bundle of CA Root Certificates\n\nThis is a bundle of X.509 certificates of public Certificate Authorities\n(CA). These were automatically extracted from Mozilla's root certificates\nfile (certdata.txt).  This file can be found in the mozilla source tree:\nhttps://hg.mozilla.org/mozilla-central/file/tip/security/nss/lib/ckfw/builtins/certdata.txt\nIt contains the certificates in PEM format and therefore\ncan be directly used with curl / libcurl / php_curl, or with\nan Apache+mod_ssl webserver for SSL client authentication.\nJust configure this file as the SSLCACertificateFile.#\n\n***** BEGIN LICENSE BLOCK *****\n" + certifi[strings.Index(certifi, "This Source"):] + "\n@(#) $RCSfile: certdata.txt,v $ $Revision: 1.80 $ $Date: 2011/11/03 15:11:58 $\n"
	if got := NormalizeLicenseToSPDX(realCertifi); got != "MPL-2.0" {
		t.Errorf("certifi's LICENSE = %q, want MPL-2.0", got)
	}
	// A negation in an earlier sentence does not reach a grant's clause.
	if got := NormalizeLicenseToSPDX("This software comes with no warranty whatsoever. It is released under the Unlicense, see https://unlicense.org."); got != "Unlicense" {
		t.Errorf("grant after a negated sentence = %q, want Unlicense", got)
	}
	// Outside the block, a license statement still keeps the text.
	assertKeptAsText(t, "block + GPL outside", "The tools/ directory is licensed under the GNU GPL version 2.\n\n***** BEGIN LICENSE BLOCK *****\n"+certifi[strings.Index(certifi, "This Source"):])
	for _, text := range []string{
		"The demo game is made available with CC0 assets from Kenney (https://kenney.nl), thanks to him.",
		"This game is published with CC0 sprites and sounds from OpenGameArt; the code itself is ours to keep.",
		"This dataset is released under CC0-style terms of our own devising; attribution is appreciated by the team.",
		"This dataset is released under the Unlicense-style terms of our own devising; attribution appreciated by us.",
		"Nothing in this repository is released under the Unlicense; ask the author before reuse of any file.",
		"None of this code is released under the Unlicense, whatever the old README said, please ask first.",
		"Neither the code nor the data here is released under the Unlicense; contact the authors for reuse permission.",
		"This project was formerly released under the Unlicense. The current terms are in the TERMS file of the repo.",
		"The Unlicense https://unlicense.org.example.com/ https://unlicense.org/ The Unlicense The Unlicense",
	} {
		assertKeptAsText(t, text, text)
	}
}

// TestWorklist61Review3 — worklist-61 review round 3:
//   - an abbreviation's period ("e.g.", "i.e.", "Jan.") does not end a
//     grant's clause, so a negation before it still withdraws the grant.
//     Decided as a class: a period ends the clause only after a word of five
//     letters or more ("... no warranty. It is released under ...");
//   - a title heading above a body ("The LibTom license", libtommath's
//     LICENSE, the exact Unlicense body) is the body's own heading, as a
//     bare heading is for the GPL body; a heading naming another license is
//     still a second statement.
func TestWorklist61Review3(t *testing.T) {
	prefix := "Copyright (c) 2020-2024 Jane Q. Doe and the Example Project Contributors. "
	for _, text := range []string{
		prefix + "Nothing in this repository (e.g. the sprites) is released under CC0.",
		prefix + "Nothing in this repository (e.g. the sprites) is released under the Unlicense.",
		prefix + "None of this code (i.e. the engine) is released under the Unlicense.",
		prefix + "This is not (e.g. per the FAQ) released under CC0.",
		prefix + "Not (as of Jan. 2020) released under the Unlicense.",
		prefix + "Nothing made by Acme Corp. is released under the Unlicense.",
		"The MIT License\n\n" + readLicenseFixture(t, "Unlicense.txt"),
	} {
		assertKeptAsText(t, text, text)
	}
	if got := NormalizeLicenseToSPDX("The LibTom license\n\n" + readLicenseFixture(t, "Unlicense.txt")); got != "Unlicense" {
		t.Errorf("libtommath's LICENSE = %q, want Unlicense", got)
	}
}

// TestWorklist61Review4 — worklist-61 review round 4: the body's own heading
// is a whole title line ("The LibTom license"), not the last words of a
// sentence, and never a GPL name: stripping "license" from "... used under
// the Lesser General Public License" hid the LGPL, which HEAD's GPL-name
// guard had kept (CGAL, round 42).
func TestWorklist61Review4(t *testing.T) {
	unl := readLicenseFixture(t, "Unlicense.txt")
	cc0 := readLicenseFixture(t, "CC0-1.0.txt")
	for name, text := range map[string]string{
		"LGPL sentence above Unlicense": "Alternatively, this code may be used under the Lesser General Public License\n\n" + unl,
		"AGPL sentence above Unlicense": "Or, at your option, under the Affero General Public License\n" + unl,
		"GPL sentence above CC0":        "This library may also be used under the General Public License\n\n" + cc0,
		"GPL heading above CC0":         "The General Public License\n\n" + cc0,
		"a different license":           "Some files in this repository use a different license\n\n" + unl,
		"heading with an article":       "A different license\n\n" + unl,
	} {
		assertKeptAsText(t, name, text)
	}
}

// TestWorklist61Review5 — worklist-61 review round 5:
//   - a bare "License" heading (chakrit/gossip's and dret/webconcepts'
//     LICENSE.md: "# LICENSE" above the exact Unlicense body) is the body's
//     own heading; HEAD read them;
//   - mozilla.org is the MPL's publisher, not a reference back for the GPL or
//     Apache notice readers: their notices next to a mozilla.org licensing
//     URL keep the text, as HEAD did.
func TestWorklist61Review5(t *testing.T) {
	for name, c := range map[string]struct{ text, want string }{
		"# LICENSE above the Unlicense": {"# LICENSE\n\n" + readLicenseFixture(t, "Unlicense.txt"), "Unlicense"},
		"License above CC0":             {"License\n\n" + readLicenseFixture(t, "CC0-1.0.txt"), "CC0-1.0"},
		"Licence above MPL":             {"Licence\n\n" + readLicenseFixture(t, "MPL-2.0.txt"), "MPL-2.0"},
	} {
		if got := NormalizeLicenseToSPDX(c.text); got != c.want {
			t.Errorf("%s = %q, want %q", name, got, c.want)
		}
	}
	for _, text := range []string{
		"This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License version 2 as published by the Free Software Foundation. The Firefox logo is used per https://www.mozilla.org/en-US/foundation/licensing/website-content/",
		`Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. The icons are used per https://www.mozilla.org/en-US/foundation/licensing/website-content/`,
	} {
		assertKeptAsText(t, text, text)
	}
}

// TestWorklist61Review6 — worklist-61 review round 6:
//   - an MPL-2.0 body under an MPL-2.0 notice that agrees with it reads
//     MPL-2.0, as a GPL or Apache body under its own notice does (real
//     LICENSE.md files: JuMP.jl's "**[MPL]** version 2.0:", SDDP.jl's
//     sentence, Tulip.jl's Markdown link and "## License" heading, the
//     nanoporetech repos' standard notice and © line). A notice that states
//     a range, the no-copyleft exhibit or another license keeps the text;
//   - outside a Mozilla license block, any GPL-family name and any listed
//     license named with its version are seen.
func TestWorklist61Review6(t *testing.T) {
	mpl := readLicenseFixture(t, "MPL-2.0.txt")
	for name, prefix := range map[string]string{
		"JuMP":     "The JuMP Julia module is licensed under the **[MPL]** version 2.0:\n\n[MPL]: https://www.mozilla.org/MPL/2.0/\n\n",
		"SDDP":     "SDDP.jl is licensed under the Mozilla Public License, Version 2.0. Copyright (c) 2017-2026: Oscar Dowson and contributors.\n\n",
		"Tulip":    "Copyright (c) 2018-2019: Mathieu Tanneau\n\nTulip.jl is licensed under the [MPL version 2.0](https://www.mozilla.org/MPL/2.0/).\n\n## License\n\n",
		"nanopore": "This Source Code Form is subject to the terms of the Mozilla Public\nLicense, v. 2.0. If a copy of the MPL was not distributed with this\nfile, You can obtain one at http://mozilla.org/MPL/2.0/.\n\n(c) 2017 Oxford Nanopore Technologies Ltd.\n\n",
	} {
		if got := NormalizeLicenseToSPDX(prefix + mpl); got != "MPL-2.0" {
			t.Errorf("%s: MPL body under its notice = %q, want MPL-2.0", name, got)
		}
	}
	quote := func(s string) string {
		lines := strings.Split(s, "\n")
		for i := range lines {
			lines[i] = "> " + lines[i]
		}
		return strings.Join(lines, "\n")
	}
	for name, text := range map[string]string{
		// Coluna.jl, Plasmo.jl: the body in a Markdown blockquote.
		"blockquoted body": "The Coluna.jl package is licensed under the Mozilla Public License, Version 2.0:\n\n" + quote("Copyright (c) 2019: Atoptima.\n\n"+mpl),
		// tbkeys: a Markdown link between the name and nothing else, then "Full license:".
		"link and Full license:": "Copyright 2019 Will Shanks. tbkeys is licensed under a [Mozilla Public License, v. 2.0](http://mozilla.org/MPL/2.0/) (full text below).\n\nFull license:\n\n" + mpl,
		// JuMP.jl with its real copyright line.
		"JuMP with copyright": "Copyright (c) 2017: Iain Dunning, Joey Huchette, Miles Lubin, and contributors\n\nThe JuMP Julia module is licensed under the **[MPL](https://www.mozilla.org/MPL/2.0/)** version 2.0:\n\n" + mpl,
		// Tab-Manager-Plus: a "Copyright & License Notice" heading.
		"license notice heading": "Copyright & License Notice\n=========================\n\nThis Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0. If a copy of the MPL was not distributed with this file, You can obtain one at http://mozilla.org/MPL/2.0/.\n\n" + mpl,
	} {
		if got := NormalizeLicenseToSPDX(text); got != "MPL-2.0" {
			t.Errorf("%s = %q, want MPL-2.0", name, got)
		}
	}
	for name, prefix := range map[string]string{
		"a range above":        "This library is licensed under the Mozilla Public License, Version 2.0 or (at your option) any later version.\n\n",
		"MIT above":            "This library is licensed under the MIT license and the Mozilla Public License, Version 2.0.\n\n",
		"no-copyleft above":    "This Source Code Form is \"Incompatible With Secondary Licenses\", as defined by the Mozilla Public License, v. 2.0.\n\n",
		"MPL 1.1 notice above": "The contents of this file are subject to the Mozilla Public License Version 1.1.\n\n",
	} {
		assertKeptAsText(t, name, prefix+mpl)
	}
	block := "\n\n***** BEGIN LICENSE BLOCK *****\nThis Source Code Form is subject to the terms of the Mozilla Public License,\nv. 2.0. If a copy of the MPL was not distributed with this file, You can obtain\none at http://mozilla.org/MPL/2.0/.\n\n***** END LICENSE BLOCK *****"
	for _, outside := range []string{
		"The documentation in docs/ is CC-BY-4.0.",
		"The helper scripts here are GNU LGPLv3.",
		"The helper scripts here are under the GNU FDL 1.3.",
		"The fixtures are EPL-2.0 and BSD-3-Clause.",
	} {
		assertKeptAsText(t, outside, outside+block)
	}
}

// TestWorklist61Review6CC0 — real CC0 texts in the review-6 GitHub sample
// that HEAD read: the legal code without its "CC0 1.0 Universal" title,
// starting at its disclaimer (ValyriaTear's COPYING.CC0) or at "Statement of
// Purpose" under a CC0 badge (woop/awesome-quantified-self,
// greggman/better-unity-webgl-template), and a grant spelled with the
// license word ("licensed under a Creative Commons CC0 license",
// mercedes-benz's FOSS manifesto).
func TestWorklist61Review6CC0(t *testing.T) {
	cc0 := readLicenseFixture(t, "CC0-1.0.txt")
	disclaimer := cc0[strings.Index(cc0, "CREATIVE COMMONS CORPORATION"):]
	purpose := cc0[strings.Index(cc0, "Statement of Purpose"):]
	for name, c := range map[string]struct{ text, want string }{
		"legal code from its disclaimer": {disclaimer, "CC0-1.0"},
		"legal code under a CC0 badge":   {"[![CC0](https://i.creativecommons.org/p/zero/1.0/88x31.png)](https://creativecommons.org/publicdomain/zero/1.0/)\n\n" + purpose, "CC0-1.0"},
		// A Markdown copy: "### _Statement of Purpose_" (review 7).
		"Markdown legal code":          {"# Creative Commons Legal Code\n\n## CC0 1.0 Universal\n\n### _Statement of Purpose_\n\n" + purpose[len("Statement of Purpose"):], "CC0-1.0"},
		"licensed under a CC0 license": {"The Example Manifesto is licensed under a Creative Commons CC0 license. Thus, it is released into the public domain.", "CC0-1.0"},
		"licensed under the Unlicense": {"This project, its code and its documentation, is licensed under the Unlicense. See the UNLICENSE file.", "Unlicense"},
	} {
		if got := NormalizeLicenseToSPDX(c.text); got != c.want {
			t.Errorf("%s = %q, want %q", name, got, c.want)
		}
	}
	for _, text := range []string{
		"The Example Manifesto is licensed under a Creative Commons CC0 license, and the code is licensed under the MIT license.",
		"This project is not licensed under the Unlicense; all of it is proprietary to Acme Corp and its licensors worldwide.",
	} {
		assertKeptAsText(t, text, text)
	}
}

// TestWorklist61Review7 — worklist-61 review round 7:
//   - the CC0 legal code starts at its own "Statement of Purpose" heading;
//     a notice above it that names "CC0 1.0 Universal" is read as the prefix
//     it is, not swallowed into the body;
//   - "license notice" is an MPL reference only as the "Copyright & License
//     Notice" heading, not "a different license notice";
//   - only the MPL's own link target (mozilla.org) is dropped above an MPL
//     body; another license's link stays a statement.
func TestWorklist61Review7(t *testing.T) {
	cc0 := readLicenseFixture(t, "CC0-1.0.txt")
	mpl := readLicenseFixture(t, "MPL-2.0.txt")
	for name, text := range map[string]string{
		"CC0 notice + GPL above the legal code": "The artwork is CC0 1.0 Universal; the code is GPL-3.0-or-later.\n\n" + cc0,
		"CC0 notice + MIT above the legal code": "The artwork is CC0 1.0 Universal; the source code is under the MIT license, see LICENSE-MIT.\n\n" + cc0,
		"CC0 notice + proprietary above":        "The artwork is released under CC0 1.0 Universal (below). The source code is proprietary to Acme Corp and all rights reserved.\n\n" + cc0,
		"a different license notice (MPL 1.1)":  "The contents of this file are subject to the Mozilla Public License Version 1.1. Some files in this directory carry a different license notice.",
		"a separate license notice (MPL 2.0)":   "This project is under the MPL 2.0. The files under vendor/ carry a separate license notice from their authors.",
		"CC-BY link above the MPL body":         "This project is licensed under the [MPL](https://www.mozilla.org/MPL/2.0/) version 2.0; the artwork, see [here](https://creativecommons.org/licenses/by-sa/4.0/).\n\n" + mpl,
	} {
		assertKeptAsText(t, name, text)
	}
}

// TestWorklist61Review8 — worklist-61 review round 8:
//   - the CC0 "Statement of Purpose" heading in its real layouts (kragen's
//     "### Statement of Purpose ###", setext underlines, a colon, bold);
//   - a URL naming the CC0 text between its title lines is the legal code's
//     own source line (an AsciiDoc copy: "= Creative Commons Legal Code",
//     "http://repository.jboss.org/licenses/cc0-1.0.txt", "CC0 1.0 Universal");
//   - "copyright and license notice" is a reference only as the heading that
//     opens the text, not "Files in vendor/ carry a different copyright and
//     license notice";
//   - only the disclaimer's exact wording is the legal code's own text: a
//     statement between "not a law firm" and "provided hereunder" is read.
func TestWorklist61Review8(t *testing.T) {
	cc0 := readLicenseFixture(t, "CC0-1.0.txt")
	for _, head := range []string{"### Statement of Purpose ###", "Statement of Purpose\n====================", "Statement of Purpose:", "**Statement of Purpose:**"} {
		text := strings.Replace(cc0, "Statement of Purpose", head, 1)
		if text == cc0 && head != "Statement of Purpose" {
			t.Fatal("the fixture lost its Statement of Purpose heading")
		}
		if got := NormalizeLicenseToSPDX(text); got != "CC0-1.0" {
			t.Errorf("heading %q: %q, want CC0-1.0", head, got)
		}
	}
	adoc := strings.Replace(cc0, "Creative Commons Legal Code", "= Creative Commons Legal Code\n\nhttp://repository.jboss.org/licenses/cc0-1.0.txt", 1)
	if got := NormalizeLicenseToSPDX(adoc); got != "CC0-1.0" {
		t.Errorf("AsciiDoc copy with its source URL = %q, want CC0-1.0", got)
	}
	purpose := cc0[strings.Index(cc0, "Statement of Purpose"):]
	for name, text := range map[string]string{
		"copyright and license notice in a sentence": "Copyright 2020 Foo Inc. This file is subject to the terms of the Mozilla Public License, version 1.1. Files in vendor/ carry a different copyright and license notice.",
		"GPL inside a disclaimer-shaped span":        "Creative Commons Corporation is not a law firm. The code is licensed under the GNU GPL v3, the artwork is provided hereunder.\n\n" + purpose,
	} {
		assertKeptAsText(t, name, text)
	}
}

// TestWorklist61Review8Corpus — real CC0 layouts from 514 GitHub copies of the
// legal code that 689acc7 read and the first round-8 fix kept as text:
// setext headings above the body ("License\n=====", "Licensing\n====="),
// copies without the "Statement of Purpose" heading (seven files), and an
// agreeing grant above the body with a "## creative commons" heading between
// (BlakeRMills/MetBrewer). A grant above that disagrees still keeps the text.
func TestWorklist61Review8Corpus(t *testing.T) {
	cc0 := readLicenseFixture(t, "CC0-1.0.txt")
	noHead := strings.Replace(cc0, "Statement of Purpose", "", 1)
	for name, text := range map[string]string{
		"License setext above":         "License\n=======\n\n" + cc0,
		"Licensing setext above":       "Licensing\n=========\n\n" + cc0,
		"Documentation License setext": "Documentation License\n=====================\n\n" + cc0,
		"no Statement of Purpose head": noHead,
		"MetBrewer grant above":        "BlakeRMills/MetBrewer is licensed under the Creative Commons Zero v1.0 Universal\n\n## creative commons\n\n# " + cc0[strings.Index(cc0, "CC0 1.0 Universal"):],
		"released under CC0 above":     "This dataset is released under CC0 1.0 Universal.\n\n" + cc0,
		"US-government template above": "As a work of the United States Government, this package is in the public domain within the United States. Additionally, we waive copyright and related rights in the work worldwide through the CC0 1.0 Universal public domain dedication.\n\n" + cc0,
		"CC mirrors badge above":       "[![CC0-1.0](http://mirrors.creativecommons.org/presskit/buttons/88x31/svg/cc-zero.svg)](http://creativecommons.org/publicdomain/zero/1.0/)\n\n### " + cc0[strings.Index(cc0, "CC0 1.0 Universal"):],
		"AsciiDoc attribute line":      "= Creative Commons Legal Code\n\nhttp://repository.jboss.org/licenses/cc0-1.0.txt\n\n:sectnums!:\n\n" + cc0[strings.Index(cc0, "CC0 1.0 Universal"):],
		"official translations line":   strings.Replace(cc0, "CC0 1.0 Universal", "CC0 1.0 Universal\n\nOfficial translations of this legal tool are available", 1),
	} {
		if got := NormalizeLicenseToSPDX(text); got != "CC0-1.0" {
			t.Errorf("%s = %q, want CC0-1.0", name, got)
		}
	}
	for name, text := range map[string]string{
		"negated grant above":   "This dataset is not released under CC0 1.0 Universal.\n\n" + cc0,
		"grant + MIT above":     "This dataset is released under CC0 1.0 Universal. The code is under the MIT license.\n\n" + cc0,
		"US-gov waiver of part": "As a work of the United States Government, the font software modifications made by GSA are not subject to copyright within the United States. Additionally, GSA waives copyright and related rights in the font software modifications worldwide through the CC0 1.0 Universal public domain dedication.\n\n" + cc0,
	} {
		assertKeptAsText(t, name, text)
	}
}

// TestWorklist61Review9 — worklist-61 review round 9: the CC0 opening block's
// parts carry no statement. An AsciiDoc or reStructuredText field line with a
// value (":License: GPL-3.0") is not a part (only a bare attribute such as
// ":sectnums!:" is); a creativecommons.org URL is a part only when it is
// CC0's own (not CC's hosted GPL); and the opening sentence is anchored with
// its continuation, so a notice that happens to begin "The laws of most
// jurisdictions throughout the world ..." is read as the prefix it is.
func TestWorklist61Review9(t *testing.T) {
	cc0 := readLicenseFixture(t, "CC0-1.0.txt")
	for name, text := range map[string]string{
		"rst license field":        ":license: GPL-3.0\n\n" + cc0,
		"rst note field":           ":note: parts of this repository are covered by the GNU General Public License\n\n" + cc0,
		"field inside the chain":   strings.Replace(cc0, "CC0 1.0 Universal", "CC0 1.0 Universal\n:note: the code is under the GNU GPL v3", 1),
		"CC-GNU GPL URL":           "http://creativecommons.org/licenses/GPL/2.0/\n\n" + cc0,
		"CC-BY-SA URL":             "<https://creativecommons.org/licenses/by-sa/4.0/>\n\n" + cc0,
		"notice opening like body": "The laws of most jurisdictions throughout the world do not apply here: the code is GPL-3.0.\n\n" + cc0,
	} {
		assertKeptAsText(t, name, text)
	}
}

// TestWorklist61Review10 — worklist-61 review round 10, real shapes:
//   - "the MPL-2.0 license" / "the MPL 2.0 license" is the MPL's name, as
//     "the Apache 2.0 license" is Apache's (publicsuffix2's shipped
//     mpl-2.0.LICENSE, kanaka/miniMAL's LICENSE, CoCube's LICENSE);
//   - an Unlicense body under an agreeing "X is licensed under the
//     Unlicense:" line reads (TrackingHeaps.jl, tamper-api), as MPL and CC0
//     bodies under their notices do;
//   - a URL in the CC0 opening block is CC0's own only by its host path or
//     its CC0 file name, not by "cc0" anywhere in it.
func TestWorklist61Review10(t *testing.T) {
	mpl := readLicenseFixture(t, "MPL-2.0.txt")
	unl := readLicenseFixture(t, "Unlicense.txt")
	cc0 := readLicenseFixture(t, "CC0-1.0.txt")
	for name, c := range map[string]struct{ text, want string }{
		"publicsuffix2":         {"This data file is licensed under the MPL-2.0 license.\n\n" + mpl, "MPL-2.0"},
		"miniMAL":               {"miniMAL is licensed under the MPL 2.0 license. The text of the MPL 2.0 license is included below.\n\n" + mpl, "MPL-2.0"},
		"MPL-2.0 license alone": {"This Android application and its build scripts are licensed under the MPL-2.0 license.", "MPL-2.0"},
		"TrackingHeaps":         {"The TrackingHeap.jl package is licensed under the Unlicense:\n\n" + unl, "Unlicense"},
		"tamper-api":            {"The Tamper-Api project is licensed under the Unlicense.\n\n" + unl, "Unlicense"},
	} {
		if got := NormalizeLicenseToSPDX(c.text); got != c.want {
			t.Errorf("%s = %q, want %q", name, got, c.want)
		}
	}
	for name, text := range map[string]string{
		"GPL URL with a cc0 fragment":   strings.Replace(cc0, "Statement of Purpose", "https://www.gnu.org/licenses/gpl-3.0.html#cc0\n\nStatement of Purpose", 1),
		"repo URL naming cc0":           "https://github.com/acme/gpl-3.0-code-with-cc0-assets\n\n" + cc0,
		"negated Unlicense above":       "The Tamper-Api project is not licensed under the Unlicense.\n\n" + unl,
		"Unlicense grant + CC-BY above": "The Tamper-Api project is licensed under the Unlicense; the icons are licensed under CC-BY-4.0.\n\n" + unl,
		"MPL-2.0 license + MIT":         "This data file is licensed under the MPL-2.0 license, and the scripts under the MIT license.",
	} {
		assertKeptAsText(t, name, text)
	}
}

// TestWorklist61Review11 — worklist-61 review round 11, one rule for all three
// body readers: a prefix that names the family's own license must be an
// agreeing notice or a name-only heading. So a heading naming CC0 above the
// legal code reads (jupyter/governance's "# Creative Commons CC0 License",
// BartMassey/feed-icons' copyright line and heading, a quoted grant in
// BartMassey/name-lists), and a negated or passing mention of the bare name
// above an Unlicense or MPL body keeps the text, as it already did for CC0.
func TestWorklist61Review11(t *testing.T) {
	cc0 := readLicenseFixture(t, "CC0-1.0.txt")
	unl := readLicenseFixture(t, "Unlicense.txt")
	mpl := readLicenseFixture(t, "MPL-2.0.txt")
	purpose := cc0[strings.Index(cc0, "Statement of Purpose"):]
	for name, c := range map[string]struct{ text, want string }{
		"jupyter heading":                  {"# Creative Commons CC0 License\n\n## " + purpose, "CC0-1.0"},
		"feed-icons":                       {"Copyright © 2022 Bart Massey\n\nCreative Commons CC0 License\n\n" + purpose, "CC0-1.0"},
		"# CC0 License":                    {"# CC0 License\n\n" + cc0, "CC0-1.0"},
		"CC0 GitHub name":                  {"Creative Commons Zero v1.0 Universal\n\n" + cc0, "CC0-1.0"},
		"quoted grant":                     {"[This program is licensed under the \"Creative Commons CC0 License\"]\n\n" + cc0, "CC0-1.0"},
		"The Unlicense heading":            {"# The Unlicense\n\n" + unl, "Unlicense"},
		"Unlicense (Public Domain) setext": {"Unlicense (Public Domain)\n============================\n\n" + unl, "Unlicense"},
		"linked grant":                     {"This code is released under [the Unlicense](http://unlicense.org)\n\n" + unl, "Unlicense"},
		"MPL 2.0 License heading":          {"# MPL 2.0 License\n\n" + mpl, "MPL-2.0"},
	} {
		if got := NormalizeLicenseToSPDX(c.text); got != c.want {
			t.Errorf("%s = %q, want %q", name, got, c.want)
		}
	}
	for name, text := range map[string]string{
		"not released under the Unlicense":  "This project is not released under the Unlicense.\n\n" + unl,
		"the Unlicense does not apply":      "The Unlicense does not apply to this project.\n\n" + unl,
		"considered the Unlicense":          "We considered the Unlicense for this project, but the author keeps copyright.\n\n" + unl,
		"not released under the MPL":        "This project is not released under the MPL.\n\n" + mpl,
		"heading, body, then CC-BY":         "# The Unlicense\n\n" + unl + "\n\nThe icons in this repository are licensed under CC-BY-4.0.",
		"grant linked to another license":   "This code is released under [the Unlicense](https://opensource.org/licenses/MIT)\n\n" + unl,
		"copyright line with GPL above CC0": "Copyright 2020 Foo Inc. Licensed under the GNU GPL v3.\n\nCreative Commons CC0 License\n\n" + purpose,
	} {
		assertKeptAsText(t, name, text)
	}
}

// TestWorklist61Review12 — worklist-61 review round 12:
//   - "n't" is a negation: a grant's quote marks are removed, but not the
//     apostrophe ("aren't released under the Unlicense");
//   - "All rights reserved." on a copyright line above the CC0 legal code is a
//     second statement, not the holder's line;
//   - a Markdown link wrapping a grant ("[Released under CC0 1.0 Universal](
//     https://example.com/our-terms)") ends in a link, not a clause;
//   - the family's own names in their other spellings ("Unlicence", "public
//     domain", "Creative Commons", "CC-Zero") route a prefix to the grant
//     check, so their negations are read.
func TestWorklist61Review12(t *testing.T) {
	cc0 := readLicenseFixture(t, "CC0-1.0.txt")
	unl := readLicenseFixture(t, "Unlicense.txt")
	for name, text := range map[string]string{
		"aren't released (notice)":  "The source code, documentation and examples in this repository aren't released under the Unlicense.",
		"haven't been dedicated":    "The data files in this repository haven't been dedicated to the public domain under CC0 1.0 Universal.",
		"isn't released above body": "The artwork isn't released under CC0 1.0 Universal.\n\n" + cc0,
		"all rights reserved line":  "Copyright (c) 2020 Acme Corp. All rights reserved.\n\n" + cc0,
		"link-wrapped grant":        "[Released under CC0 1.0 Universal](https://example.com/our-terms)\n\n" + cc0,
		"not the Unlicence":         "This project is not released under the Unlicence.\n\n" + unl,
		"not public domain":         "This software is not dedicated to the public domain.\n\n" + unl,
		"not Creative Commons":      "This dataset is not Creative Commons.\n\n" + cc0,
		"not CC-Zero":               "This dataset is not released under CC-Zero.\n\n" + cc0,
	} {
		assertKeptAsText(t, name, text)
	}
	// The own-URL link and a plain quoted name still read.
	if got := NormalizeLicenseToSPDX("[Released under CC0 1.0 Universal](https://creativecommons.org/publicdomain/zero/1.0/)\n\n" + cc0); got != "CC0-1.0" {
		t.Errorf("own-link grant = %q, want CC0-1.0", got)
	}
}

// TestWorklist61Review13 — worklist-61 reviews 13 and 14:
//   - the MPL body ends at the first Exhibit B statement after Exhibit A, so
//     text appended after the body, before another Exhibit B sentence, is
//     read as the suffix it is;
//   - a notice's negation words stay not/no/never/without/cannot/n't. Review
//     13 added "none", "nothing", "neither", "nor", "formerly" and
//     "previously" to them; review 14 found a real Apache header under
//     "Copyright (c) 2016-2018 Lightbend Inc. (formerly Typesafe Inc.)"
//     (ekrich/sconfig's LICENSE.md) that then kept its text. Those words
//     withdraw grants only; "None of the files ... are licensed under the MPL
//     2.0" reads MPL-2.0, a decided limit in the docs.
func TestWorklist61Review13(t *testing.T) {
	mpl := readLicenseFixture(t, "MPL-2.0.txt")
	exB := "\n\nThis Source Code Form is \"Incompatible With Secondary Licenses\", as defined by the Mozilla Public License, v. 2.0."
	assertKeptAsText(t, "appended MIT/GPL then ExB", mpl+"\n\nThe files in vendor/ are licensed under the MIT License, and the files in gpl/ under the GNU General Public License version 3."+exB)
	assertKeptAsText(t, "once released (grant)", "This project was once released under the Unlicense; the current terms are in the TERMS file.")
	assertKeptAsText(t, "formerly released (grant)", "This project was formerly released under the Unlicense. The current terms are in the TERMS file of the repo.")
	for name, c := range map[string]struct{ text, want string }{
		"sconfig": {"_Copyright (c) 2011-2016 Typesafe Inc._\\\n_Copyright (c) 2016-2018 Lightbend Inc. (formerly Typesafe Inc.)_\\\n_Copyright (c) 2018-2026 Eric K Richardson_\n\nLicensed under the Apache License, Version 2.0 (the \"License\"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0", "Apache-2.0"},
		"Novell":  {"Copyright (C) 2008 Novell, Inc. (formerly Ximian, Inc.)\n\nThis program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.", "GPL-3.0-or-later"},
	} {
		if got := NormalizeLicenseToSPDX(c.text); got != c.want {
			t.Errorf("%s = %q, want %q", name, got, c.want)
		}
	}
}

// TestWorklist61Review15 — worklist-61 review round 15: a badge's image is not
// a statement. A shields.io badge above a body (KBPsystem777/JSexercises'
// LICENSE.md: [![License: CC0-1.0](https://img.shields.io/...)](CC0 deed))
// reads by its alt text and its link target; the image URL is ignored. The
// link target must still be the license's own page (OSI's and SPDX's pages
// count), and the alt text must still be the license's name.
func TestWorklist61Review15(t *testing.T) {
	cc0 := readLicenseFixture(t, "CC0-1.0.txt")
	mpl := readLicenseFixture(t, "MPL-2.0.txt")
	unl := readLicenseFixture(t, "Unlicense.txt")
	for name, c := range map[string]struct{ text, want string }{
		"CC0 shields badge":       {"[![License: CC0-1.0](https://img.shields.io/badge/License-CC0%201.0-lightgrey.svg)](http://creativecommons.org/publicdomain/zero/1.0/)\n\n" + cc0, "CC0-1.0"},
		"MPL shields badge":       {"[![License: MPL 2.0](https://img.shields.io/badge/License-MPL_2.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)\n\n" + mpl, "MPL-2.0"},
		"Unlicense shields badge": {"[![License: Unlicense](https://img.shields.io/badge/license-Unlicense-blue.svg)](http://unlicense.org/)\n\n" + unl, "Unlicense"},
	} {
		if got := NormalizeLicenseToSPDX(c.text); got != c.want {
			t.Errorf("%s = %q, want %q", name, got, c.want)
		}
	}
	for name, text := range map[string]string{
		"badge alt names MIT":      "[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](http://creativecommons.org/publicdomain/zero/1.0/)\n\n" + cc0,
		"badge links another page": "[![License: CC0-1.0](https://img.shields.io/badge/License-CC0%201.0-lightgrey.svg)](https://opensource.org/licenses/MIT)\n\n" + cc0,
	} {
		assertKeptAsText(t, name, text)
	}
}

// TestWorklist61Review16 — worklist-61 review round 16:
//   - a shields.io badge renders its path, so a badge image URL that names
//     another license ("/badge/License-GPL_3.0-blue.svg") is a statement,
//     whatever its alt text; one that names only the license itself is not;
//   - MPL's OSI and SPDX pages are MPL references in a notice and in a grant
//     link above the body, as mozilla.org is (elkarte/themes' license notice).
func TestWorklist61Review16(t *testing.T) {
	cc0 := readLicenseFixture(t, "CC0-1.0.txt")
	unl := readLicenseFixture(t, "Unlicense.txt")
	mpl := readLicenseFixture(t, "MPL-2.0.txt")
	for name, text := range map[string]string{
		"CC0 + GPL badge":                "The CC0 1.0 Universal ![License](https://img.shields.io/badge/License-GPL_v3-blue.svg) applies to the data in this repository.",
		"Unlicense + Apache badge":       "The Unlicense ![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg) for this project and its examples here.",
		"empty alt GPL badge":            "Unlicense ![](https://img.shields.io/badge/License-GPL_3.0-blue.svg?style=for-the-badge&logo=opensourceinitiative)",
		"GPL badge linked to own":        "[![License](https://img.shields.io/badge/license-GPL-blue.svg?style=for-the-badge)](https://unlicense.org)\n\n" + unl,
		"MIT badge alt CC0":              "[![CC0](https://img.shields.io/badge/license-MIT-blue.svg?style=for-the-badge)](https://creativecommons.org/publicdomain/zero/1.0/)\n\n" + cc0,
		"Apache_2.0 badge linked to own": "[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://unlicense.org)\n\n" + unl,
	} {
		assertKeptAsText(t, name, text)
	}
	for name, c := range map[string]struct{ text, want string }{
		"OSI URL in notice":       {"This project is licensed under the Mozilla Public License 2.0 (https://opensource.org/licenses/MPL-2.0).", "MPL-2.0"},
		"SPDX URL in notice":      {"Licensed under the Mozilla Public License, Version 2.0 (https://spdx.org/licenses/MPL-2.0.html). See LICENSE.", "MPL-2.0"},
		"elkarte":                 {"This ElkArte Addon is subject to the terms of the Mozilla Public License 2.0 (the \"License\"). You can obtain a copy of the License at https://opensource.org/licenses/MPL-2.0", "MPL-2.0"},
		"OSI link above the body": {"Foo is licensed under the [MPL](https://opensource.org/licenses/MPL-2.0) version 2.0:\n\n" + mpl, "MPL-2.0"},
		"own CC0 shields badge":   {"[![License: CC0-1.0](https://img.shields.io/badge/License-CC0_1.0-lightgrey.svg)](http://creativecommons.org/publicdomain/zero/1.0/)\n\n" + cc0, "CC0-1.0"},
	} {
		if got := NormalizeLicenseToSPDX(c.text); got != c.want {
			t.Errorf("%s = %q, want %q", name, got, c.want)
		}
	}
	// An OSI URL of another MPL version disagrees with the notice.
	assertKeptAsText(t, "MPL 1.1 notice with the 2.0 OSI URL", "The contents of this file are subject to the Mozilla Public License Version 1.1; see https://opensource.org/licenses/MPL-2.0 for details.")
}

// TestWorklist61Review17 — worklist-61 review round 17: a badge path glues a
// version to a name ("LGPLv3", "MPL2", "BSD3", "EUPLv1.2") and writes a
// literal dash as "--" ("CC--BY--4.0"); the image check reads both.
func TestWorklist61Review17(t *testing.T) {
	cc0 := readLicenseFixture(t, "CC0-1.0.txt")
	unl := readLicenseFixture(t, "Unlicense.txt")
	for _, name := range []string{"AGPLv3", "MPL2", "MPLv2", "BSD3", "EUPLv1.2"} {
		text := "[![License](https://img.shields.io/badge/license-" + name + "-blue.svg)](https://unlicense.org)\n\n" + unl
		assertKeptAsText(t, name+" badge over the Unlicense", text)
	}
	for _, name := range []string{"LGPLv3", "CC--BY--4.0", "CC--BY--SA--4.0"} {
		text := "[![License](https://img.shields.io/badge/license-" + name + "-blue.svg)](http://creativecommons.org/publicdomain/zero/1.0/)\n\n" + cc0
		assertKeptAsText(t, name+" badge over CC0", text)
	}
	assertKeptAsText(t, "CC0 badge over the Unlicense", "[![License](https://img.shields.io/badge/license-CC0-blue.svg)](https://unlicense.org)\n\n"+unl)
}

// TestWorklist61Review18 — worklist-61 review round 18: a badge on a line of
// its own ("![License: Unlicense](https://img.shields.io/...)" above the
// body) reads as the same badge mid-line does. The shared comment-marker
// strip took a line-opening "!" for a Fortran comment and left a plain link.
func TestWorklist61Review18(t *testing.T) {
	cc0 := readLicenseFixture(t, "CC0-1.0.txt")
	unl := readLicenseFixture(t, "Unlicense.txt")
	mpl := readLicenseFixture(t, "MPL-2.0.txt")
	for name, c := range map[string]struct{ text, want string }{
		"Unlicense badge line": {"![License: Unlicense](https://img.shields.io/badge/license-Unlicense-blue.svg)\n\n" + unl, "Unlicense"},
		"CC0 badge line":       {"![License: CC0-1.0](https://img.shields.io/badge/License-CC0_1.0-lightgrey.svg)\n\n" + cc0, "CC0-1.0"},
		"MPL badge line":       {"![License: MPL 2.0](https://img.shields.io/badge/License-MPL_2.0-brightgreen.svg)\n\n" + mpl, "MPL-2.0"},
	} {
		if got := NormalizeLicenseToSPDX(c.text); got != c.want {
			t.Errorf("%s = %q, want %q", name, got, c.want)
		}
	}
	assertKeptAsText(t, "GPL badge line", "![License: Unlicense](https://img.shields.io/badge/license-GPL_3.0-blue.svg)\n\n"+unl)
}

// TestWorklist61Review19 — worklist-61 review round 19:
//   - only the image guards are removed from the plain views; a U+200B the
//     text already had still separates words ("MIT\u200blicensed");
//   - another license written with its version glued on ("LGPLv3",
//     "Apache2", "BSD3", "EPLv2") next to a grant is seen, as the spaced and
//     dashed spellings already were: the other-license check reads the text
//     with glued versions split too (the badge path's rule, review 17).
func TestWorklist61Review19(t *testing.T) {
	for name, text := range map[string]string{
		"MIT zero-width licensed": "This project is released under the Unlicense. The bundled test fixtures are MIT\u200blicensed and stay that way.",
		"OFL zero-width licensed": "Everything in this repository is dedicated to the public domain under CC0 1.0 Universal. Fonts are OFL\u200blicensed.",
	} {
		assertKeptAsText(t, name, text)
	}
	for _, name := range []string{"LGPLv3", "AGPLv3", "Apache2", "BSD3", "EPLv2"} {
		assertKeptAsText(t, name+" beside an Unlicense grant", "Everything here is released under the Unlicense. Portions: "+name+" as the headers say in the tree.")
		assertKeptAsText(t, name+" beside a CC0 grant", "Everything here is dedicated to the public domain under CC0 1.0 Universal. Portions: "+name+" as the headers say.")
	}
}
