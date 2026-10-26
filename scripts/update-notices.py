#!/usr/bin/env python3
"""Collect license texts from the locked release graph and bundled native sources."""
import argparse
import hashlib
import json
import re
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--manifest-path', type=Path, default=root / 'Cargo.toml')
args = parser.parse_args()
packages = {}
seen = set()
targets = re.findall(r"target: ([a-z0-9_-]+)", (root / '.github/workflows/release.yml').read_text())
if not targets:
    raise SystemExit('Release platform inventory is empty')
for target in targets:
    metadata = json.loads(subprocess.check_output([
        'cargo', 'metadata', '--locked', '--offline', '--format-version', '1',
        '--manifest-path', str(args.manifest_path), '--filter-platform', target,
    ]))
    packages.update((p['id'], p) for p in metadata['packages'])
    nodes = {n['id']: n for n in metadata['resolve']['nodes']}
    pending = [metadata['resolve']['root']]
    local = set()
    while pending:
        identity = pending.pop()
        if identity in local:
            continue
        local.add(identity)
        pending.extend(d['pkg'] for d in nodes[identity]['deps']
                       if any(k['kind'] != 'dev' for k in d['dep_kinds']))
    seen.update(local)

texts = []
shared = {}
for package in sorted((packages[i] for i in seen if packages[i]['name'] != 'jkq'),
                      key=lambda p: (p['name'], p['version'])):
    directory = Path(package['manifest_path']).parent
    files = [p for p in directory.iterdir() if p.is_file()
             and p.name.lower().startswith(('license', 'copying', 'notice', 'unlicense'))]
    if package['name'] == 'jx':
        files.append(directory.parents[1] / 'THIRD_PARTY_LICENSES')
    if package['name'] in {'openssl-src', 'rdkafka-sys', 'zstd-sys', 'libz-sys'}:
        files.extend(p for p in directory.rglob('*') if p.is_file()
                     and p.name.lower().startswith(('license', 'copying', 'notice')))
    files = sorted(set(files))
    if not files:
        raise SystemExit(f"Review missing license text: {package['name']}")
    texts.append(f"\n{'=' * 72}\n{package['name']} {package['version']} ({package['license']})\n")
    for path in files:
        body = path.read_text(encoding='utf-8', errors='strict')
        # Preserve wording while normalizing upstream trailing spaces for review.
        body = '\n'.join(line.rstrip(' \t') for line in body.split('\n'))
        digest = hashlib.sha256(body.encode()).hexdigest()
        label = str(path.relative_to(directory)) if path.is_relative_to(directory) else 'jx/Rust notices'
        texts.append(f'\n{label}\n')
        if digest in shared:
            texts.append(f'License text reproduced above: {shared[digest]}\n')
        else:
            shared[digest] = f"{package['name']} {package['version']}/{label}"
            texts.append(body.rstrip() + '\n')
(root / 'THIRD_PARTY_LICENSES').write_text(
    'Third-party licenses and notices\n\n'
    'Dependency license metadata and source license texts for the locked release graph.\n'
    'Bundled native-source notices and the jx/Rust standard-library notices are included.\n'
    'Alternative source licenses remain alternatives; this inventory does not change them.\n'
    + ''.join(texts), encoding='utf-8')
