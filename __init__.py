import zipfile
import os
import re
import hashlib
import argparse
from pathlib import Path
from bs4 import BeautifulSoup
import tempfile
from collections import defaultdict

def detect_epub_type(temp_path: Path) -> str:
    """Detects if the ePub is a supported PressReader file and determines its type.
    Returns 'calibre', 'raw', or 'unsupported'."""
    # Search for a unique PressReader footprint in any XHTML file.
    # This confirms it's a PressReader ePub.
    is_pressreader = False
    all_xhtml_files = list(temp_path.glob("**/*.xhtml"))

    # Check a few files to avoid scanning the whole book if unnecessary
    for xhtml_file in all_xhtml_files[:5]:
        with open(xhtml_file, 'r', encoding='utf-8') as f:
            content = f.read()
            if "PressReader.com" in content or "NewspaperDirect" in content:
                is_pressreader = True
                break

    if not is_pressreader:
        return 'unsupported'

    # Now that we know it's a PressReader file, check if it was converted by Calibre.
    opf_file = next(temp_path.glob("**/content.opf"), None)
    if opf_file:
        with open(opf_file, 'r', encoding='utf-8') as f:
            content = f.read()
        if 'calibre' in content:
            print("Detection: Calibre-converted PressReader ePub found.")
            return 'calibre'
    print("Detection: Raw PressReader ePub found.")
    return 'raw'

def get_article_hash(article_tag):
    """Generates a SHA256 hash from the body text (<p> tags) of an article."""
    paragraphs = article_tag.find_all('p')
    body_text = "".join(p.get_text(strip=True) for p in paragraphs)
    return hashlib.sha256(body_text.encode('utf-8')).hexdigest()

def get_page_num_from_path(path: Path):
    """Extracts the page number from a file path using regex."""
    match = re.search(r'page-(\d+)', str(path))
    return int(match.group(1)) if match else None

def find_correct_version(path_list: list[Path]) -> Path:
    """Finds the correct version of an article to keep from a list of paths.
    Rule: Keep the first page of the last consecutive block of pages."""
    if not path_list:
        return None

    pages = sorted([(get_page_num_from_path(p), p) for p in path_list if get_page_num_from_path(p) is not None])

    if not pages:
        return path_list[0] # Fallback if no page numbers found

    correct_page_num, correct_path = pages[-1]

    for i in range(len(pages) - 2, -1, -1):
        page_num, path = pages[i]
        if page_num == correct_page_num - 1:
            correct_page_num, correct_path = page_num, path
        else:
            break

    return correct_path

def update_metadata_files(temp_path: Path, deleted_files: set, epub_type: str):
    """Parses OPF and NCX files to remove all references to deleted XHTML files."""
    # Determine correct base paths based on ePub type
    if epub_type == 'raw':
        # In raw files, opf and ncx are inside OEBPS
        metadata_base_path = next(temp_path.glob("**/OEBPS"), temp_path)
    else: # calibre
        # In Calibre files, they are in the root
        metadata_base_path = temp_path

    deleted_rel_paths = {f.relative_to(metadata_base_path).as_posix() for f in deleted_files}

    # Update content.opf
    opf_file = next(metadata_base_path.glob("*.opf"), None)
    if opf_file:
        print("Updating content.opf manifest...")
        with open(opf_file, 'r+', encoding='utf-8') as f:
            soup = BeautifulSoup(f, 'xml')
            ids_to_delete = set()

            for item in soup.select('manifest item[href$=".xhtml"]'):
                if item.get('href') in deleted_rel_paths:
                    ids_to_delete.add(item.get('id'))
                    item.decompose()

            for itemref in soup.select('spine itemref'):
                if itemref.get('idref') in ids_to_delete:
                    itemref.decompose()

            f.seek(0); f.write(str(soup)); f.truncate()

    # Update toc.ncx
    ncx_file = next(metadata_base_path.glob("*.ncx"), None)
    if ncx_file:
        print("Updating toc.ncx navigation...")
        with open(ncx_file, 'r+', encoding='utf-8') as f:
            soup = BeautifulSoup(f, 'xml')

            for navpoint in soup.select('navMap navPoint'):
                content_tag = navpoint.select_one('content')
                if content_tag and content_tag.get('src'):
                    src_path = content_tag['src'].split('#')[0]
                    if src_path in deleted_rel_paths:
                        navpoint.decompose()

            f.seek(0); f.write(str(soup)); f.truncate()

def clean_epub(epub_path: Path, keep_first: bool = False):
    if not epub_path.is_file():
        print(f"Error: File not found at {epub_path}"); return
    # Ensure the file is a ZIP archive before proceeding.
    if not zipfile.is_zipfile(epub_path):
        print(f"Error: '{epub_path.name}' is not a valid ePub or ZIP archive. Aborting.")
        return

    output_path = epub_path.with_name(f"{epub_path.stem}_clean.epub")
    print(f"Processing '{epub_path.name}'...")
    if keep_first:
        print("Mode: --keep-first flag detected. Keeping the first instance of each article.")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        try:
            with zipfile.ZipFile(epub_path, 'r') as zip_ref: zip_ref.extractall(temp_path)
        except zipfile.BadZipFile:
            print(f"Error: Failed to unzip '{epub_path.name}'. File corrupted or not a valid ePub. Aborting.")
            return

        # Step 1: Detect type and set the correct article marker
        epub_type = detect_epub_type(temp_path)
        if epub_type == 'unsupported':
            print(f"Error: '{epub_path.name}' does not appear to be a PressReader-generated ePub. Aborting.")
            return
        elif epub_type == 'calibre':
            article_selector_args = ('div', {'class': 'toc'})
        else: # raw
            article_selector_args = ('div', {'class': 'art-cnt'})

        oebps_path = next(temp_path.glob("**/OEBPS"), temp_path)
        all_xhtml_files = sorted(oebps_path.glob("page-*/**/*.xhtml"))

        # Stage 1: Gather information
        print("Stage 1: Analyzing article locations...")
        hashes_to_paths = defaultdict(list)
        for xhtml_file in all_xhtml_files:
            with open(xhtml_file, 'r', encoding='utf-8') as f:
                soup = BeautifulSoup(f, 'xml')
            for article in soup.find_all(*article_selector_args):
                article_hash = get_article_hash(article)
                hashes_to_paths[article_hash].append(xhtml_file)

        # Stage 2: Decide which single version of each article to keep
        print("Stage 2: Deciding which articles to keep...")
        articles_to_keep = set()
        for article_hash, path_list in hashes_to_paths.items():
            if len(path_list) == 1:
                articles_to_keep.add((article_hash, path_list[0]))
            else:
                if keep_first:
                    # Sort paths by page number and keep the very first one
                    sorted_paths = sorted(path_list, key=lambda p: get_page_num_from_path(p) or 0)
                    correct_path = sorted_paths[0]
                else:
                    # Use the default "first of the last consecutive block" rule
                    correct_path = find_correct_version(path_list)

                if correct_path:
                    articles_to_keep.add((article_hash, correct_path))

        # Stage 3: Clean the files
        print("Stage 3: Cleaning files...")
        files_to_delete = set()
        articles_removed_count = 0

        for xhtml_file in all_xhtml_files:
            with open(xhtml_file, 'r', encoding='utf-8') as f:
                soup = BeautifulSoup(f, 'xml')

            articles_in_file = soup.find_all(*article_selector_args)
            if not articles_in_file or not soup.body: continue

            articles_kept_in_file = 0
            for article in articles_in_file:
                article_hash = get_article_hash(article)
                if (article_hash, xhtml_file) in articles_to_keep:
                    articles_kept_in_file += 1
                else:
                    article.decompose()
                    articles_removed_count += 1

            if articles_kept_in_file == 0 and len(articles_in_file) > 0:
                files_to_delete.add(xhtml_file)

            with open(xhtml_file, 'w', encoding='utf-8') as f:
                f.write(str(soup))

        # Stage 4: Delete empty files and update metadata
        if files_to_delete:
            print(f"Stage 4: Removing {len(files_to_delete)} empty files and updating manifest...")
            update_metadata_files(temp_path, files_to_delete, epub_type)
            for f in files_to_delete:
                if f.is_file(): f.unlink()

        # Stage 5: Re-pack the ePub
        print("Stage 5: Re-packing the clean ePub...")
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            mimetype_path = temp_path / 'mimetype'
            if mimetype_path.exists():
                zipf.write(mimetype_path, 'mimetype', compress_type=zipfile.ZIP_STORED)

            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    if file == 'mimetype': continue
                    file_path = Path(root) / file
                    if file_path.is_file():
                      archive_name = file_path.relative_to(temp_path)
                      zipf.write(file_path, archive_name)

    print("\n--------------------")
    print("Cleaning Metrics:")
    print(f"- Total articles removed: {articles_removed_count}")
    print(f"- Empty pages deleted: {len(files_to_delete)}")
    print("--------------------")
    print(f"\n✨ Success! Cleaned file saved as '{output_path.name}'")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Clean PressReader ePub files by removing duplicate articles.")
    parser.add_argument("epub_file", type=str, help="The path to the .epub file to be cleaned.")
    parser.add_argument("--keep-first", action="store_true", help="Keep the very first instance of an article, useful for files without a 'Content' section.")
    args = parser.parse_args()
    clean_epub(Path(args.epub_file), keep_first=args.keep_first)
