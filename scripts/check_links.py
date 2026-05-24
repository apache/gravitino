#!/usr/bin/env python3
"""Validate intra-repo Markdown links and anchors."""
import os
import re
import sys
from pathlib import Path
from collections import defaultdict

LINK_RE = re.compile(r'\[([^\]]*)\]\(([^)]+)\)')
HEADING_RE = re.compile(r'^(#{1,6})\s+(.+)$', re.MULTILINE)
EXTERNAL_PREFIXES = ('http://', 'https://', 'mailto:', 'tel:', 'ftp://', 'pathname://')

# Docusaurus auto-generated / plugin-resolved paths that don't exist as files
# in docs/ but resolve at build time. These are not real broken links.
DOCUSAURUS_BUILD_PATHS = ('api/rest/', 'api/java-api', 'api/python-api')


def slugify(text):
    """Docusaurus-style heading slug."""
    # Strip inline code backticks, bold/italic markers, leading/trailing whitespace
    text = re.sub(r'`([^`]+)`', r'\1', text)
    text = re.sub(r'[*_]+', '', text)
    text = text.strip()
    text = text.lower()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text)
    return text.strip('-')


def extract_anchors(content):
    """Return set of valid anchors in a Markdown file (from headings).

    Mirrors Docusaurus's duplicate-heading handling: if the same slug appears
    multiple times, the second gets ``-1``, the third ``-2``, and so on.
    """
    anchors = set()
    seen = {}
    for m in HEADING_RE.finditer(content):
        slug = slugify(m.group(2))
        if slug in seen:
            anchors.add(f"{slug}-{seen[slug]}")
            seen[slug] += 1
        else:
            anchors.add(slug)
            seen[slug] = 1
    return anchors


def extract_links(path):
    """Return list of (line_no, link_text, link_target) tuples from a file."""
    links = []
    with path.open(encoding='utf-8') as f:
        for line_no, line in enumerate(f, 1):
            for m in LINK_RE.finditer(line):
                text, target = m.group(1), m.group(2)
                links.append((line_no, text, target))
    return links


def resolve_target(target, source_file, docs_root):
    """Resolve a link target to (file_path, anchor) or (None, anchor) for same-file refs."""
    # Strip query strings (rare in docs but possible)
    target = target.split('?', 1)[0]

    if '#' in target:
        file_part, anchor = target.split('#', 1)
    else:
        file_part, anchor = target, None

    if not file_part:
        # Same-file anchor reference: [text](#section)
        return source_file, anchor

    # Resolve relative or absolute-from-docs-root paths
    if file_part.startswith('/'):
        candidate = docs_root / file_part.lstrip('/')
    else:
        candidate = (source_file.parent / file_part).resolve()

    # Append .md if it has no extension and the bare path doesn't exist
    if not candidate.exists() and not candidate.suffix:
        for ext in ('.md', '.mdx'):
            if (candidate.parent / (candidate.name + ext)).exists():
                candidate = candidate.parent / (candidate.name + ext)
                break

    return candidate, anchor


def validate_docs(docs_root):
    docs_root = Path(docs_root).resolve()
    md_files = sorted(docs_root.rglob('*.md'))
    broken = []

    for md in md_files:
        for line_no, text, target in extract_links(md):
            if target.startswith(EXTERNAL_PREFIXES):
                continue
            if any(target.startswith(p) or target.startswith('./' + p) for p in DOCUSAURUS_BUILD_PATHS):
                continue
            if target.startswith('#'):
                # Same-file anchor
                target_file, anchor = md, target.lstrip('#')
            else:
                target_file, anchor = resolve_target(target, md, docs_root)

            # Image-only links inside parens (rare); skip if target is clearly an asset
            if target_file and target_file.suffix.lower() in (
                '.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', '.pdf'
            ):
                if not target_file.exists():
                    broken.append({
                        'file': md.relative_to(docs_root),
                        'line': line_no,
                        'text': text,
                        'target': target,
                        'error': f"Asset not found: {target_file.relative_to(docs_root) if docs_root in target_file.parents else target_file}",
                    })
                continue

            if target_file is None or not target_file.exists():
                broken.append({
                    'file': md.relative_to(docs_root),
                    'line': line_no,
                    'text': text,
                    'target': target,
                    'error': f"File not found: {target}",
                })
                continue

            if anchor:
                try:
                    content = target_file.read_text(encoding='utf-8')
                except Exception as e:
                    broken.append({
                        'file': md.relative_to(docs_root),
                        'line': line_no,
                        'text': text,
                        'target': target,
                        'error': f"Could not read target: {e}",
                    })
                    continue

                anchors = extract_anchors(content)
                if anchor not in anchors:
                    # Provide a short hint of available anchors
                    sample = sorted(anchors)[:8]
                    broken.append({
                        'file': md.relative_to(docs_root),
                        'line': line_no,
                        'text': text,
                        'target': target,
                        'error': f"Anchor '#{anchor}' not found in {target_file.name}",
                        'hint': sample,
                    })

    return broken


def main():
    if len(sys.argv) < 2:
        print("Usage: check_links.py <docs_directory>")
        sys.exit(2)

    docs_root = sys.argv[1]
    if not os.path.exists(docs_root):
        print(f"Error: '{docs_root}' not found")
        sys.exit(2)

    print(f"Validating Markdown links under {docs_root}...")
    broken = validate_docs(docs_root)

    if not broken:
        print("\nAll links and anchors valid.")
        return 0

    print(f"\nFound {len(broken)} broken link(s):\n")
    by_file = defaultdict(list)
    for item in broken:
        by_file[str(item['file'])].append(item)

    for filepath in sorted(by_file):
        print(f"\n{filepath}")
        for item in by_file[filepath]:
            print(f"  L{item['line']}: [{item['text']}]({item['target']})")
            print(f"    -> {item['error']}")
            if 'hint' in item and item['hint']:
                print(f"    available anchors (sample): {', '.join(item['hint'])}")

    return 1


if __name__ == '__main__':
    sys.exit(main())
