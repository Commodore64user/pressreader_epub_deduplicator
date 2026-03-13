import hashlib
import os
import re
import tempfile
import zipfile
from bs4 import BeautifulSoup
from calibre.customize import FileTypePlugin
from calibre.customize.conversion import OptionRecommendation
from calibre.ebooks.conversion.plumber import Plumber
from calibre.utils.logging import default_log
from collections import defaultdict
from pathlib import Path

class PressReaderDeduplicator(FileTypePlugin):
    name                    = 'PressReader ePub deDuplicator'
    description             = 'Automatically removes duplicate articles from PressReader-generated ePubs.'
    supported_platforms     = ['windows', 'osx', 'linux']
    author                  = 'Commodore64user'
    version                 = (1, 1, 0)
    minimum_calibre_version = (5, 0, 0)  # Python 3.
    file_types              = {'epub'}
    on_import               = True # Runs when adding to library
    on_postimport           = True
    # on_preprocess           = True # sadly it interferes with Plumber and causes an infinite loop
    priority                = 100

    def run(self, path_to_ebook):
        epub_type = self.pre_check(path_to_ebook)
        if epub_type is None:
            return path_to_ebook

        # path_to_ebook is a path to a temporary copy of the file Calibre is importing.
        epub_path = Path(path_to_ebook)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            try:
                with zipfile.ZipFile(epub_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_path)

                # Configure selectors based on epub type
                if epub_type == 'calibre':
                    article_selector_args = ('div', {'class': 'toc'})
                else: # raw
                    article_selector_args = ('div', {'class': 'art-cnt'})

                oebps_path = next(temp_path.glob("**/OEBPS"), temp_path)
                all_xhtml_files = sorted(oebps_path.glob("page-*/**/*.xhtml"))

                # Stage 1: Parse all files, strip nav chrome, build article hash map
                hashes_to_paths = defaultdict(list)
                soup_cache = {}
                hash_cache = {}  # {xhtml_file: {article_id: hash}}
                for xhtml_file in all_xhtml_files:
                    with open(xhtml_file, 'r', encoding='utf-8') as f:
                        soup = BeautifulSoup(f, 'xml')
                    # Strip page header navigation divs
                    for cls in ('page-header', 'art-header'):
                        for header in soup.find_all('div', attrs={'class': cls}):
                            header.decompose()
                    soup_cache[xhtml_file] = soup
                    file_hashes = {}
                    for article in soup.find_all(*article_selector_args):
                        article_hash = self.get_article_hash(article)
                        file_hashes[id(article)] = article_hash
                        hashes_to_paths[article_hash].append(xhtml_file)
                    hash_cache[xhtml_file] = file_hashes

                # Stage 2: Decide which version of each duplicate to keep
                articles_to_keep = set()
                for article_hash, path_list in hashes_to_paths.items():
                    if len(path_list) == 1:
                        articles_to_keep.add((article_hash, path_list[0]))
                    else:
                        correct_path = self.find_correct_version(path_list)
                        if correct_path:
                            articles_to_keep.add((article_hash, correct_path))

                # Stage 3: Decompose duplicate articles, track removed anchors
                anchors_deleted = set()
                files_to_delete = set()
                metadata_base_path = next(temp_path.glob("**/OEBPS"), temp_path)

                for xhtml_file in all_xhtml_files:
                    soup = soup_cache[xhtml_file]
                    articles_in_file = soup.find_all(*article_selector_args)
                    for article in articles_in_file:
                        article_hash = hash_cache[xhtml_file][id(article)]
                        if (article_hash, xhtml_file) not in articles_to_keep:
                            anchor_id = article.get('id')
                            if anchor_id:
                                rel = xhtml_file.relative_to(metadata_base_path).as_posix()
                                anchors_deleted.add((rel, anchor_id))
                            article.decompose()
                    # Track files that are now empty (calibre type only needs this)
                    articles_kept = sum(1 for a in articles_in_file
                                        if (hash_cache[xhtml_file][id(a)], xhtml_file) in articles_to_keep)
                    if len(articles_in_file) > 0 and articles_kept == 0:
                        files_to_delete.add(xhtml_file)
                    with open(xhtml_file, 'w', encoding='utf-8') as f:
                        f.write(str(soup))

                dupe_count = sum(len(v) - 1 for v in hashes_to_paths.values() if len(v) > 1)
                print(f"deDuplicator: Found {len(all_xhtml_files)} xhtml files")
                print(f"deDuplicator: {len(hashes_to_paths)} unique article hashes")
                print(f"deDuplicator: {sum(1 for v in hashes_to_paths.values() if len(v) > 1)} hashes with duplicates")
                print(f"deDuplicator: {dupe_count} duplicate articles removed")

                # Stage 4: Clean dead anchor references from nav files
                deleted_rel_paths = None
                if epub_type == 'calibre' and files_to_delete:
                    deleted_rel_paths = {re.sub(r'^OEBPS/', '', f.relative_to(temp_path).as_posix()) for f in files_to_delete}

                if anchors_deleted or deleted_rel_paths:
                    self.clean_nav_files(temp_path, epub_type, anchors_deleted, deleted_rel_paths)
                    # Stage 4.5: For calibre epubs, clean manifest and delete empty files
                    # Plumber splits articles into individual xhtml files; when all articles
                    # in a split file are removed, the file becomes an empty (blank) page.
                    if deleted_rel_paths:
                        self._clean_calibre_manifest(temp_path, files_to_delete)

                # Stage 5: Re-pack the ePub back into the original temporary path
                tmp = self.temporary_file('.epub')
                tmp.close()
                repack_tmp = Path(tmp.name)
                try:
                    with zipfile.ZipFile(repack_tmp, 'w', zipfile.ZIP_DEFLATED) as zipf:
                        mimetype_path = temp_path / 'mimetype'
                        if mimetype_path.exists():
                            zipf.write(mimetype_path, 'mimetype', compress_type=zipfile.ZIP_STORED)

                        for root, _, files in os.walk(temp_dir):
                            for file in files:
                                if file == 'mimetype': continue
                                file_path = Path(root) / file
                                archive_name = file_path.relative_to(temp_path)
                                zipf.write(file_path, archive_name)

                    os.replace(repack_tmp, path_to_ebook)
                except Exception:
                    repack_tmp.unlink(missing_ok=True)
                    raise

                if epub_type == 'raw':
                    self._convert_epub(path_to_ebook)

                return path_to_ebook

                # Note: At this point, calibre runs postimport(), which in our case renames
                #       the file, so it can be sorted by year, i.e., (2026-03-13)
            except Exception as e:
                print(f"deDuplicator: Processing failed: {e}")
                return path_to_ebook

    def pre_check(self, path_to_ebook):
        """Detects if the ePub is a supported PressReader file and determines its type.
            Returns 'calibre', 'raw', or None."""
        try:
            with zipfile.ZipFile(path_to_ebook, 'r') as z:
                opf_path = next((f for f in z.namelist() if f.endswith('.opf')), None)
                if not opf_path:
                    return None
                with z.open(opf_path) as f:
                    opf_content = f.read().decode('utf-8', errors='ignore')

                pattern = r"<(?:dc:creator|dc:publisher)[^>]*>(.*?)</(?:dc:creator|dc:publisher)>"
                matches = re.findall(pattern, opf_content, re.IGNORECASE)
                if not any("NewspaperDirect" in match for match in matches):
                    print('deDuplicator: Not a PressReader ePub, aborting')
                    return None
                print('\ndeDuplicator: PressReader ePub confirmed via metadata')

                meta_match = re.search(r"<metadata(.*?)>", opf_content, re.DOTALL | re.IGNORECASE)
                if meta_match and 'calibre.kovidgoyal.net' in meta_match.group(1).lower():
                    print('deDuplicator: Previously converted by calibre')
                    return 'calibre'

                print(f"deDuplicator: Raw PressReader ePub detected")
                return 'raw'
        except Exception as e:
            print(f"deDuplicator: Pre-check failed: {e}")
            return None

    def get_article_hash(self, article_tag):
        """Generates a SHA256 hash from the body text (<p> tags) of an article."""
        paragraphs = article_tag.find_all('p')
        body_text = "".join(p.get_text(strip=True) for p in paragraphs)
        return hashlib.sha256(body_text.encode('utf-8')).hexdigest()

    def get_page_num_from_path(self, path):
        """Extracts the page number from a file path using regex."""
        match = re.search(r'page-(\d+)', str(path))
        return int(match.group(1)) if match else None

    def find_correct_version(self, path_list):
        """Finds the correct version of an article to keep from a list of duplicate paths.
        Rule: Keep the first page of the last consecutive block of pages.

        Articles spanning multiple pages appear once per physical page they occupy. We keep
        the first page of the last consecutive block, which corresponds to where the
        article actually starts in the issue.

        The 'last consecutive block' logic was originally needed because Contents
        sections also carried full articles, creating an early isolated duplicate
        (e.g., page 3) separate from the main run (e.g., pages 17-18). Upstream
        behaviour appears to have changed and Contents pages no longer duplicate
        articles in full, but the logic remains correct and handles both cases."""

        pages = sorted([(self.get_page_num_from_path(p), p) for p in path_list if self.get_page_num_from_path(p) is not None])
        if not pages:
            return path_list[0]
        candidate_page_num, candidate_path = pages[-1]
        for i in range(len(pages) - 2, -1, -1):
            page_num, path = pages[i]
            # we build some tolerence (<=2) so articles split by ads are still considered one single cluster
            if candidate_page_num - page_num <= 2:
                candidate_page_num, candidate_path = page_num, path
            else:
                break
        return candidate_path

    def clean_nav_files(self, temp_path, epub_type, anchors_deleted, deleted_rel_paths=None):
        """Removes dead anchor references from NCX and nav.xhtml files."""
        if epub_type == 'raw':
            # In raw files, opf and ncx are inside OEBPS
            metadata_base_path = next(temp_path.glob("**/OEBPS"), temp_path)
        else:
            # In Calibre files, they are in the root
            metadata_base_path = temp_path

        def _get_ncx_nodes(soup):
            for navpoint in soup.find_all('navPoint'):
                if navpoint.parent is None:
                    continue
                content = navpoint.find('content')
                if content and content.get('src'):
                    yield navpoint, re.sub(r'^OEBPS/', '', content['src'])

        def _get_nav_nodes(soup):
            for li in soup.find_all('li'):
                a = li.find('a', href=True)
                if a:
                    yield li, re.sub(r'^OEBPS/', '', a['href'])

        ncx_file = next(metadata_base_path.glob("**/*.ncx"), None)
        if ncx_file:
            self._clean_nav(ncx_file, _get_ncx_nodes, anchors_deleted, deleted_rel_paths)

        nav_file = next(metadata_base_path.glob("**/nav.xhtml"), None)
        if nav_file:
            self._clean_nav(nav_file, _get_nav_nodes, anchors_deleted, deleted_rel_paths)

    def _safe_write_xml(self, target_file, soup):
        dir_ = target_file.parent
        with tempfile.NamedTemporaryFile('w', encoding='utf-8', dir=dir_, delete=False, suffix='.tmp') as tmp:
            tmp_path = Path(tmp.name)
        try:
            tmp_path.write_text(str(soup), encoding='utf-8')
            os.replace(tmp_path, target_file)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

    def postimport(self, book_id, book_format, db):
        if book_format.lower() != 'epub':
            return
        mi = db.get_metadata(book_id, index_is_id=True)
        new_title = self._reformat_title(mi.title)
        if new_title:
            mi.title = new_title
            mi.title_sort = None # force calibre to update the sort-title using our new_title
            db.set_metadata(book_id, mi)
            print(f"deDuplicator: Title reformatted to '{new_title}'")

    def _convert_epub(self, path_to_ebook):
        tmp = self.temporary_file('.epub')
        tmp.close()
        tmp_path = tmp.name
        try:
            plumber = Plumber(path_to_ebook, tmp_path, default_log)
            plumber.merge_ui_recommendations([
                ('output_profile', 'kindle_oasis', OptionRecommendation.HIGH),
                ('epub_version', '3', OptionRecommendation.HIGH),
            ])
            plumber.run()
            os.replace(tmp_path, path_to_ebook)
            print(f"deDuplicator: ePub conversion complete")
        except Exception as e:
            print(f"deDuplicator: Conversion failed: {e}")
            Path(tmp_path).unlink(missing_ok=True)

    def _reformat_title(self, title_str):
        months = {
            'Jan':'01', 'Feb':'02', 'Mar':'03', 'Apr':'04', 'May':'05', 'Jun':'06',
            'Jul':'07', 'Aug':'08', 'Sep':'09', 'Oct':'10', 'Nov':'11', 'Dec':'12'
        }
        match = re.search(r'^(.*?)\((\d{1,2})\s+(\w{3})\s+(\d{4})\)$', title_str.strip())
        if not match:
            return None
        name, day, month, year = match.group(1).strip(), match.group(2), match.group(3), match.group(4)
        month_to_num = months.get(month)
        if not month_to_num:
            return None
        return f"{name} ({year}-{month_to_num}-{int(day):02d})"


    def _clean_nav(self, nav_file, get_nodes, anchors_deleted, deleted_rel_paths=None):
        with open(nav_file, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f, 'xml')
        for node, src in list(get_nodes(soup)):
            rel = src.split('#')[0]
            anchor = src.split('#')[1] if '#' in src else None
            if deleted_rel_paths and rel in deleted_rel_paths:
                print(f"deDuplicator: Removing from {nav_file.name}: {src}")
                node.decompose()
            elif anchor and (rel, anchor) in anchors_deleted:
                print(f"deDuplicator: Removing dead anchor from {nav_file.name}: {src}")
                node.decompose()
        self._safe_write_xml(nav_file, soup)

    def _clean_calibre_manifest(self, temp_path, files_to_delete):
        metadata_base_path = temp_path
        deleted_rel_paths = {f.relative_to(metadata_base_path).as_posix() for f in files_to_delete}

        opf_file = next(metadata_base_path.glob("*.opf"), None)
        if opf_file:
            with open(opf_file, 'r', encoding='utf-8') as f:
                soup = BeautifulSoup(f, 'xml')

            ids_to_delete = set()
            manifest = soup.find('manifest')
            if manifest:
                for item in manifest.find_all('item'):
                    if item.get('href', '') in deleted_rel_paths:
                        print(f"deDuplicator: Removing from manifest: {item.get('href')}")
                        ids_to_delete.add(item.get('id'))
                        item.decompose()

            spine = soup.find('spine')
            if spine:
                for itemref in spine.find_all('itemref'):
                    if itemref.get('idref') in ids_to_delete:
                        itemref.decompose()

            self._safe_write_xml(opf_file, soup)

        for f in files_to_delete:
            if f.is_file():
                f.unlink()
        print(f"deDuplicator: Removed {len(files_to_delete)} empty files from calibre epub")
