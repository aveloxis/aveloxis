// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package a_test

import (
	"context"
	"testing"

	"github.com/aveloxis/aveloxis/scripts/testdata/xtestvariant/a"
	"github.com/aveloxis/aveloxis/scripts/testdata/xtestvariant/b"
	"github.com/aveloxis/aveloxis/scripts/testdata/xtestvariant/c"
	"github.com/jackc/pgx/v5/pgxpool"
)

var _ = a.ExportedForXTest

var _ = b.Use(a.New())

// c reaches a only through b: a direct-only rebuild splits *a.K here, and a
// rebuild without the variant cache splits b.W (two copies of b).
var _ *a.K = c.Get().K

var _ b.W = c.Get()

// The one deferred connection close the check must find in this package.
func TestX(t *testing.T) {
	raw, _ := pgxpool.New(context.Background(), "")
	defer raw.Close()
}
