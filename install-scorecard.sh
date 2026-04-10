install_scorecard() {
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')   # darwin or linux
    ARCH=$(uname -m)

    case "$ARCH" in
        x86_64)  ARCH="amd64" ;;
        arm64|aarch64) ARCH="arm64" ;;
        *) echo "Unsupported architecture: $ARCH"; return 1 ;;
    esac

    VERSION=$(curl -s https://api.github.com/repos/ossf/scorecard/releases/latest \
        | grep '"tag_name"' | cut -d'"' -f4)

    FILENAME="scorecard_${OS}_${ARCH}"
    URL="https://github.com/ossf/scorecard/releases/download/${VERSION}/${FILENAME}"

    TMP=$(mktemp)
    curl -sSfL "$URL" -o "$TMP" || { echo "Download failed"; return 1; }
    chmod +x "$TMP"
    mv "$TMP" "$HOME/go/bin/scorecard"   # or wherever you put scc
}
