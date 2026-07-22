#!/usr/bin/env sh

set -eu

REPO="${GITHUB_REPOSITORY:-jo-cube/jkq}"
INSTALL_DIR="${INSTALL_DIR:-${HOME}/.local/bin}"
VERSION="${VERSION:-latest}"
CLI_NAME="jkq"

usage() {
	cat <<EOF
Usage: $0 [jkq] [destination]

Installs a prebuilt jkq release binary from ${REPO}.

Arguments:
  jkq          Optional binary name, accepted for compatibility
  destination Optional install directory (default: ${INSTALL_DIR})

Environment:
  VERSION            Release tag to install (default: latest)
  INSTALL_DIR        Destination directory (default: ~/.local/bin)
  GITHUB_REPOSITORY  GitHub repo in owner/name form (default: jo-cube/jkq)
EOF
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
	usage
	exit 0
fi

case "$#" in
	0)
		;;
	1)
		if [ "$1" = "$CLI_NAME" ]; then
			:
		else
			INSTALL_DIR="$1"
		fi
		;;
	2)
		if [ "$1" != "$CLI_NAME" ]; then
			usage >&2
			exit 1
		fi
		INSTALL_DIR="$2"
		;;
	*)
		usage >&2
		exit 1
		;;
esac

case "$REPO" in
	*/*/*|/*|*/|*'..'*|*[!A-Za-z0-9._/-]*)
		printf 'error: invalid GITHUB_REPOSITORY: %s\n' "$REPO" >&2
		exit 1
		;;
	*/*)
		;;
	*)
		printf 'error: invalid GITHUB_REPOSITORY: %s\n' "$REPO" >&2
		exit 1
		;;
esac

case "$VERSION" in
	*[!A-Za-z0-9._-]*)
		printf 'error: invalid VERSION: %s\n' "$VERSION" >&2
		exit 1
		;;
	*)
		;;
esac

OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS" in
	Darwin)
		RELEASE_OS="darwin"
		;;
	Linux)
		RELEASE_OS="linux"
		;;
	*)
		printf 'error: unsupported operating system: %s\n' "$OS" >&2
		exit 1
		;;
esac

case "$ARCH" in
	arm64|aarch64)
		RELEASE_ARCH="arm64"
		;;
	x86_64)
		RELEASE_ARCH="amd64"
		;;
	*)
		printf 'error: unsupported architecture: %s\n' "$ARCH" >&2
		exit 1
		;;
esac

ASSET_NAME="${CLI_NAME}_${RELEASE_OS}_${RELEASE_ARCH}.tar.gz"
TMP_DIR="$(mktemp -d)"
ARCHIVE_PATH="${TMP_DIR}/${ASSET_NAME}"
CHECKSUM_PATH="${ARCHIVE_PATH}.sha256"

cleanup() {
	rm -rf "$TMP_DIR"
}

trap cleanup EXIT INT TERM

if command -v curl >/dev/null 2>&1; then
	DOWNLOAD_TOOL='curl'
elif command -v wget >/dev/null 2>&1; then
	DOWNLOAD_TOOL='wget'
else
	printf 'error: curl or wget is required to download release assets\n' >&2
	exit 1
fi

case "$VERSION" in
	latest)
		DOWNLOAD_URL="https://github.com/${REPO}/releases/latest/download/${ASSET_NAME}"
		;;
	*)
		DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${VERSION}/${ASSET_NAME}"
		;;
esac

printf 'Downloading %s from %s\n' "$ASSET_NAME" "$DOWNLOAD_URL"
case "$DOWNLOAD_TOOL" in
	curl)
		curl -fsSL "$DOWNLOAD_URL" -o "$ARCHIVE_PATH"
		curl -fsSL "${DOWNLOAD_URL}.sha256" -o "$CHECKSUM_PATH"
		;;
	wget)
		wget -qO "$ARCHIVE_PATH" "$DOWNLOAD_URL"
		wget -qO "$CHECKSUM_PATH" "${DOWNLOAD_URL}.sha256"
		;;
esac

if command -v sha256sum >/dev/null 2>&1; then
	(cd "$TMP_DIR" && sha256sum -c "${ASSET_NAME}.sha256" >/dev/null)
elif command -v shasum >/dev/null 2>&1; then
	set -- $(cat "$CHECKSUM_PATH")
	EXPECTED="$1"
	ACTUAL="$(shasum -a 256 "$ARCHIVE_PATH" | awk '{print $1}')"
	if [ "$ACTUAL" != "$EXPECTED" ]; then
		printf 'error: checksum mismatch for %s\n' "$ASSET_NAME" >&2
		exit 1
	fi
else
	printf 'error: sha256sum or shasum is required to verify release assets\n' >&2
	exit 1
fi

mkdir -p "$INSTALL_DIR"
if [ "$(tar -tzf "$ARCHIVE_PATH")" != "$CLI_NAME" ]; then
	printf 'error: release archive must contain only %s\n' "$CLI_NAME" >&2
	exit 1
fi
tar -xzf "$ARCHIVE_PATH" -C "$TMP_DIR"
install -m 0755 "${TMP_DIR}/${CLI_NAME}" "${INSTALL_DIR}/${CLI_NAME}"

printf 'Installed %s to %s\n' "$CLI_NAME" "${INSTALL_DIR}/${CLI_NAME}"

case ":$PATH:" in
	*":${INSTALL_DIR}:"*)
		;;
	*)
		printf 'Add %s to your PATH if it is not already available.\n' "$INSTALL_DIR"
		;;
esac
