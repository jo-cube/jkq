#!/usr/bin/env sh

set -eu

ROOT="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT INT TERM

mkdir -p "$TMP_DIR/fixtures" "$TMP_DIR/mock-bin" "$TMP_DIR/install"
printf '#!/usr/bin/env sh\nprintf "jkq fixture\\n"\n' > "$TMP_DIR/fixtures/jkq"
chmod +x "$TMP_DIR/fixtures/jkq"
tar -czf "$TMP_DIR/fixtures/jkq_linux_amd64.tar.gz" \
	-C "$TMP_DIR/fixtures" jkq

if command -v sha256sum >/dev/null 2>&1; then
	(cd "$TMP_DIR/fixtures" && \
		sha256sum jkq_linux_amd64.tar.gz > jkq_linux_amd64.tar.gz.sha256)
else
	(cd "$TMP_DIR/fixtures" && \
		shasum -a 256 jkq_linux_amd64.tar.gz > jkq_linux_amd64.tar.gz.sha256)
fi

cat > "$TMP_DIR/mock-bin/uname" <<'EOF'
#!/usr/bin/env sh
case "$1" in
	-s) printf 'Linux\n' ;;
	-m) printf 'x86_64\n' ;;
esac
EOF

cat > "$TMP_DIR/mock-bin/curl" <<'EOF'
#!/usr/bin/env sh
set -eu
while [ "$#" -gt 0 ]; do
	case "$1" in
		-o) OUTPUT="$2"; shift 2 ;;
		-*) shift ;;
		*) URL="$1"; shift ;;
	esac
done
printf '%s\n' "$URL" >> "$CURL_LOG"
case "$URL" in
	*.sha256) FILE='jkq_linux_amd64.tar.gz.sha256' ;;
	*) FILE='jkq_linux_amd64.tar.gz' ;;
esac
cp "$FIXTURES/$FILE" "$OUTPUT"
EOF
chmod +x "$TMP_DIR/mock-bin/uname" "$TMP_DIR/mock-bin/curl"

PATH="$TMP_DIR/mock-bin:$PATH" \
	FIXTURES="$TMP_DIR/fixtures" \
	CURL_LOG="$TMP_DIR/curl.log" \
	INSTALL_DIR="$TMP_DIR/install" \
	VERSION='v9.9.9' \
	sh "$ROOT/scripts/install.sh" >/dev/null

test "$("$TMP_DIR/install/jkq")" = 'jkq fixture'
test "$(sed -n '1p' "$TMP_DIR/curl.log")" = \
	'https://github.com/jo-cube/jkq/releases/download/v9.9.9/jkq_linux_amd64.tar.gz'
test "$(sed -n '2p' "$TMP_DIR/curl.log")" = \
	'https://github.com/jo-cube/jkq/releases/download/v9.9.9/jkq_linux_amd64.tar.gz.sha256'

printf '%064d  jkq_linux_amd64.tar.gz\n' 0 > \
	"$TMP_DIR/fixtures/jkq_linux_amd64.tar.gz.sha256"
if PATH="$TMP_DIR/mock-bin:$PATH" \
	FIXTURES="$TMP_DIR/fixtures" \
	CURL_LOG="$TMP_DIR/curl.log" \
	INSTALL_DIR="$TMP_DIR/rejected" \
	sh "$ROOT/scripts/install.sh" >/dev/null 2>&1; then
	printf 'installer accepted an invalid checksum\n' >&2
	exit 1
fi
test ! -e "$TMP_DIR/rejected/jkq"
