"""
AdTech Company Data Analyst Exam - Python Section
------------------------------------------
Description:
    This script identifies potential publisher leads by analyzing competitor runtime domains.
    It fetches lists of sites working with competitors via an internal API, checks if they
    are existing clients, and scrapes their pages to extract contact information (emails, social links).

Key Features:
    1. Parallel Processing: Uses ProcessPoolExecutor to handle multiple competitors concurrently.
    2. Robust Scraping: Includes fallback mechanisms (HTTP/HTTPS) and validation logic.
    3. Contact Extraction: Uses Regex to identify emails and social media profiles.

IMPORTANT NOTE ON DATA:
    The provided API endpoint (`/similar_get_domains`) returns a simple list of domain strings
    rather than objects containing traffic data (as implied in the instructions).
    Consequently, the 'monthly_visitors' field is unavailable in the source response
    and defaults to 0 for all discovered sites.
"""

import csv
import json
import re
import requests
import time
from datetime import date
from dataclasses import dataclass
from typing import List, Set, Any, Dict, Tuple
from urllib.parse import urljoin
from concurrent.futures import ProcessPoolExecutor, as_completed

import logging
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - PID:%(process)d - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


api_url =  "http://leads-management.ad-maven.com:9777/similar_get_domains"
api_key = "alo12220@gmail.com"
request_timeout = 5
max_workers = 5 # parallel

@dataclass
class CompetitorRecord:
    """
    Data structure representing a single row from the input CSV.
    Using a dataclass ensures type safety and cleaner code when passing data between functions.
    """
    competitor_name: str
    run_time_domain: str

def normalize_domain(domain: str) -> str:
    """
    Standardizes domain names to a clean 'domain.com' format.

    Logic:
    1. Removes whitespace and converts to lowercase.
    2. Strips protocol prefixes (http://, https://).
    3. Removes 'www.' subdomain.
    4. Drops any trailing paths or queries (e.g., domain.com/page -> domain.com).

    Returns:
        A clean domain string ready for API querying or deduplication.
    """
    if not domain:
        return ""
    d = domain.strip().lower()
    d = d.replace("https://", "")
    d = d.replace("http://", "")
    d = re.sub(r"^https?://", "", d)
    d = re.sub(r"^www\.", "", d)
    d = d.split("/")[0]
    return d

def load_competitors(path: str) -> List[CompetitorRecord]:
    """
    Parses the competitor CSV file and converts valid rows into CompetitorRecord objects.

    Key Logic:
    - Skips empty rows or rows missing required fields.
    - Applies normalization immediately upon loading to ensure consistency.
    """
    try:
        with open(path, encoding="utf-8") as f:
            return [
                CompetitorRecord(
                    competitor_name=row["competitor"].strip(),
                    run_time_domain=normalize_domain(row["run_time_domain"].strip())
                )
                for row in csv.DictReader(f)
                if row.get("competitor") and row.get("run_time_domain")
            ]
    except FileNotFoundError:
        logger.error(f"Could not find file: {path}")
        return []

def load_cur_clients(path: str) -> Set[str]:
    """
    Loads the list of existing clients to create an exclusion list (Blacklist).

    - This is crucial for performance when checking thousands of discovered sites
      against the existing client base.

    - Handles potential variations in column headers.
    """
    try:
        with open(path, encoding="utf-8") as f:
            return {
                normalize_domain(raw)
                for row in csv.DictReader(f)
                if (raw := (row.get("domain") or row.get("domains") or "").strip())
            }
    except FileNotFoundError:
        logger.warning(f"Client file {path} not found. Continuing without excluding clients.")
        return set()

# Call SimilarWeb-style API
def fetch_sites_for_runtime_domain(runtime_domain: str) -> List[Dict[str, Any]]:
    params = {
        "api_key": api_key,
        "domain": runtime_domain,
    }

    try:
        response = requests.get(api_url, params=params, timeout=request_timeout)
        response.raise_for_status()

        try:
            data = response.json()
        except Exception as je:
            logger.error(f"Failed to parse JSON for {runtime_domain}: {je}")
            return []

        final_list = []
        raw_list = []

        if isinstance(data, list):
            raw_list = data
        else:
            raw_list = data.get("domain_list", [])

        for item in raw_list:
            if isinstance(item, str):
                final_list.append({"site_name": item, "monthly_visitors": 0})
            elif isinstance(item, dict):
                final_list.append(item)

        return final_list

    except Exception as e:
        logger.error(f"API request failed for runtime_domain={runtime_domain}: {e}")
    return []

def fetch_html_content(url: str) -> Tuple[str, bool]:
    """
    Performs the low-level HTTP GET request with anti-scraping measures.

    Key Features:
    1. **User-Agent Spoofing:** Simulates a real browser to avoid immediate rejection by basic firewalls.
    2. **Blocking Detection:** Checks for both 'Hard Blocks' (HTTP Status codes 403, 429)
       and 'Soft Blocks' (CAPTCHA challenges embedded in the HTML text).

    Returns:
        Tuple (html_content, is_blocked)
    """
    time.sleep(0.5)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        resp = requests.get(url, timeout=request_timeout, headers=headers)

        if resp.status_code in [403, 429]:
            return "", True

        if 200 <= resp.status_code < 300:
            text_lower = resp.text.lower()
            if "captcha" in text_lower or "access denied" in text_lower or "verify you are human" in text_lower:
                return "", True
            return resp.text, False

    except requests.RequestException:
        pass  # not block only error

    return "", False


def fetch_site_html_robust(site_domain: str) -> Tuple[str, bool]:
    """
    Implements a protocol fallback strategy to maximize reachability.

    Logic:
    1. Attempt HTTPS first (Standard for modern web).
    2. If HTTPS fails (e.g., SSL handshake error, timeout), fallback to HTTP.
       This is crucial for discovering older or misconfigured publisher sites.
    """
    # Try Https
    html, blocked = fetch_html_content(f"https://{site_domain}")
    if html or blocked:
        return html, blocked

    # Fallback to HTTP
    return fetch_html_content(f"http://{site_domain}")

contact_keywords = ["contact", "about", "support"]
max_contact_pages = 2  # limit pages to scan per site

# Crawling helper: find internal contact pages
def find_potential_contact_urls(html: str, base_domain: str) -> List[str]:
    """
    Implements a Heuristic Crawling strategy to discover internal 'Contact' pages.

    Logic:
    1. Scans all 'href' tags in the HTML.
    2. Filters links based on semantic keywords (contact, about, support).
    3. Handles relative URLs (e.g., '/contact') by joining them with the base domain.
    """
    urls: Set[str] = set()
    for match in re.findall(r'href=[\'"]([^\'"]+)[\'"]', html, flags=re.IGNORECASE):
        href = match.strip()
        if not href or href.startswith("#") or href.lower().startswith("javascript:"):
            continue

        lower_href = href.lower()
        if not any(keyword in lower_href for keyword in contact_keywords):
            continue

        if href.startswith("http://") or href.startswith("https://"):
            full_url = href
        else:
            full_url = urljoin(f"https://{base_domain}/", href)

        urls.add(full_url)

    return list(urls)[:max_contact_pages]


# Parsing
def extract_contacts(html: str) -> Dict[str, Any]:
    """
    Parses HTML content to extract structured contact information (Emails & Social Links).
    Returns: A dictionary containing lists of found emails and social profiles.
    """
    if not html: return {}

    contacts = {}

    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'

    social_patterns = {
        "facebook": r"facebook\.com/[a-zA-Z0-9_.-]+",
        "instagram": r"instagram\.com/[a-zA-Z0-9_.-]+",
        "twitter": r"(twitter\.com|x\.com)/[a-zA-Z0-9_.-]+",
        "linkedin": r"linkedin\.com/(in|company)/[a-zA-Z0-9_.-]+",
        "telegram": r"t\.me/[a-zA-Z0-9_]+",
        "youtube": r"youtube\.com/(@|channel/|user/)?[a-zA-Z0-9_.-]+"
    }

    # Emails
    emails = list(set(re.findall(email_pattern, html)))
    if emails:
        contacts["emails"] = emails

    # Socials
    social_found = {}
    for platform, pattern in social_patterns.items():
        matches = list(set(re.findall(pattern, html, re.IGNORECASE)))
        if matches:
            social_found[platform] = matches

    if social_found:
        contacts["social"] = social_found

    return contacts if contacts else {}

# finding ads
def check_for_ads(html: str, competitor_runtime: str) -> Tuple[bool, str]:
    """
    Logic:
    1. **Direct Competitor Match:** Searches for the specific runtime domain string
       within the HTML source. If found, captures a code snippet as evidence.
    2. **Generic Ad Network Match:** If the competitor is not found, checks for
       footprints of major ad networks (Google AdSense, Taboola, Outbrain, etc.).

    Returns:
        Tuple (is_running_ads: bool, evidence: str)
    """
    if not html:
        return False, ""
    # finding the code of the competitor
    if competitor_runtime in html:
        idx = html.find(competitor_runtime)
        start = max(0, idx - 50)
        end = min(len(html), idx + 100)
        snippet = html[start:end].replace("\n", " ").replace("\r", "")
        return True, f"Found competitor domain: ...{snippet}..."

    generic_keywords = ["adsbygoogle", "googlesyndication", "criteo", "outbrain", "taboola", "/ads/"]
    for kw in generic_keywords:
        if kw in html:
            return True, f"Found generic ad keyword: {kw}"

    return False, ""

#Parallel logic
def process_competitor_workflow(competitor: CompetitorRecord, clients_set: Set[str]) -> List[Dict[str, Any]]:
    """
    Executes the full data enrichment pipeline for a single competitor.
    Running this function in a separate process allows for high-throughput parallel scanning.
    """
    logger.info(f"Starting scan for {competitor.competitor_name}")

    sites_data = fetch_sites_for_runtime_domain(competitor.run_time_domain)
    results = []

    for site_obj in sites_data:
        raw_site = site_obj.get("site_name") or site_obj.get("domain") or ""
        site_domain = normalize_domain(raw_site)

        if not site_domain:
            continue

        monthly_visitors = site_obj.get("monthly_visitors")
        if monthly_visitors is None:
            monthly_visitors = 0

        already_working = site_domain in clients_set

        # 1. Scrape main page
        html, got_blocked = fetch_site_html_robust(site_domain)
        contacts_dict = extract_contacts(html)

        # 2. Deep crawling: if no contacts found, try inner pages
        if not contacts_dict and html and not got_blocked:
            potential_urls = find_potential_contact_urls(html, site_domain)
            for inner_url in potential_urls:
                inner_html, _ = fetch_html_content(inner_url)
                if inner_html:
                    more_contacts = extract_contacts(inner_html)
                    # Merge results
                    if "emails" in more_contacts:
                        existing = set(contacts_dict.get("emails", []))
                        existing.update(more_contacts["emails"])
                        contacts_dict["emails"] = list(existing)

                    if "social" in more_contacts:
                        social_existing = contacts_dict.get("social", {})
                        for k, v in more_contacts["social"].items():
                            existing_list = set(social_existing.get(k, []))
                            existing_list.update(v)
                            social_existing[k] = list(existing_list)
                        contacts_dict["social"] = social_existing

        # Convert final dict to JSON for CSV
        contacts_json = json.dumps(contacts_dict) if contacts_dict else "{}"

        is_running_ads, ad_evidence = check_for_ads(html, competitor.run_time_domain)

        row = {
            "scan_date": date.today().isoformat(),
            "site_domain": site_domain,
            "competitor_name": competitor.competitor_name,
            "run_time_domain": competitor.run_time_domain,
            "monthly_visitors": monthly_visitors,
            "contacts_json": contacts_json,
            "got_blocked": got_blocked,
            "already_working": already_working,
            "is_running_ads": is_running_ads,
            "ad_evidence": ad_evidence,
        }
        results.append(row)

    logger.info(f"Finished {competitor.competitor_name}. Found {len(results)} sites.")
    return results


def main() -> None:
    logger.info("=" * 60)
    logger.info("AdTech Company Lead Discovery Script - Starting Execution")
    logger.info("=" * 60)

    # 1. Load Inputs
    comp_run_time_domains = "comp_run_time_domains.csv"
    competitors = load_competitors(comp_run_time_domains)
    logger.info(f"Found {len(competitors)} competitors in {comp_run_time_domains}")

    our_clients = "our_clients.csv"
    clients = load_cur_clients(our_clients)
    logger.info(f"Found {len(clients)} client domains in {our_clients}")

    if not competitors:
        logger.warning("No competitors found. Exiting.")
        return

    all_rows: List[Dict[str, Any]] = []
    logger.info(f"Starting parallel execution with {max_workers} workers...")

    start_time = time.time()

    # 2. Parallel Execution
    # Using ProcessPoolExecutor is ideal for this task as it involves significant network I/O
    # and CPU processing (Regex/Parsing) across independent data chunks.
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Map each competitor to a future task
        futures = {
            executor.submit(process_competitor_workflow, comp, clients): comp
            for comp in competitors
        }

        # Collect results as they finish (as_completed)
        for future in as_completed(futures):
            comp = futures[future]
            try:
                data = future.result()
                all_rows.extend(data)
            except Exception as exc:
                # Fault Tolerance: Ensure one worker crash doesn't kill the entire script
                logger.error(f"Exception in worker for {comp.competitor_name}: {exc}", exc_info=True)

    elapsed = time.time() - start_time
    logger.info(f"Scan completed in {elapsed:.2f} seconds.")

    if not all_rows:
        logger.warning("No data collected.")
        return

    # 3. Export Results
    output_path = "discovered_sites.csv"
    fieldnames = [
        "scan_date",
        "site_domain",
        "competitor_name",
        "run_time_domain",
        "monthly_visitors",
        "contacts_json",
        "got_blocked",
        "already_working",
        "is_running_ads",
        "ad_evidence",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    logger.info(f"Results written to: {output_path}")
    logger.info(f"Total rows: {len(all_rows)}")

    logger.info("=" * 60)
    logger.info("Execution completed successfully!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

