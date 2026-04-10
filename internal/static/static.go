// Package static embeds shared static assets (icons, images) for use by
// the web GUI, monitor dashboard, and API server.
package static

import _ "embed"

//go:embed icon.png
var IconPNG []byte
