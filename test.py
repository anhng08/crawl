import asyncio
import json
import random
import re
from typing import List, Optional
import httpx
from bs4 import BeautifulSoup, NavigableString, Tag
from supabase import create_client

BASE_URL = "https://trangvangvietnam.com"
SUPABASE_URL = ""
SUPABASE_KEY = ""

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)",
]

DETAIL_WORKERS = 10
BATCH_SIZE = 100

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "vi-VN,vi;q=0.9",
    }


def clean(text) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def make_client():
    return httpx.AsyncClient(
        timeout=15,
        limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
    )


async def fetch(client: httpx.AsyncClient, url: str) -> Optional[str]:
    try:
        resp = await client.get(url, headers=get_headers())
        resp.raise_for_status()
        print(f"GET {url} → {resp.status_code} ({len(resp.text)} bytes)")
        return resp.text
    except Exception as e:
        print(f"FETCH ERROR {url}: {e}")
        return None


def extract_company_links(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/listings/" in href:
            if href.startswith("/"):
                href = BASE_URL + href
            links.add(href)
    return list(links)


def extract_json_ld(soup: BeautifulSoup) -> dict:
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, dict):
                return data
        except Exception:
            continue
    return {}


def extract_district(address: str) -> str:
    if not address:
        return ""
    match = re.search(r"(Q\.|H\.|TP\.|Quận|Huyện|Thành phố)\s*[^,]+", address)
    return match.group(0).strip() if match else ""


def normalize_employee_size(text: str) -> str:
    if not text:
        return ""
    text_lower = text.lower()
    if "ít hơn" in text_lower:
        return "0-10"
    if "trên" in text_lower:
        return "500+"
    match = re.search(r"(\d+)\s*-\s*(\d+)", text)
    if match:
        return f"{match.group(1)}-{match.group(2)}"
    return clean(text)


def extract_label_block(soup: BeautifulSoup, label: str) -> str:
    for block in soup.select("div.p-2.clearfix"):
        span = block.find("span")
        if span and label.lower() in span.get_text(strip=True).lower():
            full = clean(block.get_text(" ", strip=True))
            label_text = clean(span.get_text(strip=True))
            return clean(full.replace(label_text, "").lstrip(":").strip())
    return ""


def get_industry(soup: BeautifulSoup) -> List[str]:
    for block in soup.select("div.p-2.clearfix"):
        span = block.find("span")
        if span and "ngành nghề" in span.get_text(strip=True).lower():
            return [a.get_text(strip=True) for a in block.find_all("a")]
    return []


def get_name(soup: BeautifulSoup) -> str:
    el = soup.select_one(".noidung_chantrang span")
    if el:
        return clean(el.get_text())
    h1 = soup.find("h1")
    return clean(h1.get_text()) if h1 else ""


def get_address(soup: BeautifulSoup) -> str:
    label = soup.find(string=lambda t: t and "đ/c" in t.lower())
    if not label:
        return ""
    node = label.next_sibling
    while node and not isinstance(node, NavigableString):
        node = node.next_sibling
    return clean(str(node)) if node else ""


def get_tax_code(soup: BeautifulSoup) -> str:
    return extract_label_block(soup, "MÃ SỐ THUẾ")


def parse_company(html: str) -> Optional[dict]:
    soup = BeautifulSoup(html, "html.parser")

    schema = extract_json_ld(soup)
    if schema:
        address_obj = schema.get("address", {}) if isinstance(schema.get("address"), dict) else {}
        street = address_obj.get("streetAddress", "")
        region = address_obj.get("addressRegion", "")
        full_address = clean(f"{street} {region}")

        raw_emp = schema.get("numberOfEmployees", {})
        emp_raw = raw_emp.get("value", "") if isinstance(raw_emp, dict) else str(raw_emp)

        industries = schema.get("knowsAbout", [])
        if isinstance(industries, list):
            industry_str = ", ".join(industries)
        else:
            industry_str = str(industries)

        tax_code = schema.get("taxID", "") or get_tax_code(soup)
        name = schema.get("name", "") or get_name(soup)

        if not tax_code and not name:
            return None

        return {
            "tax_code": tax_code,
            "name": name,
            "founded_year": schema.get("foundingDate", "") or extract_label_block(soup, "NĂM THÀNH LẬP"),
            "address": full_address or get_address(soup),
            "district": extract_district(street),
            "employee_size": normalize_employee_size(emp_raw) or normalize_employee_size(extract_label_block(soup, "SỐ LƯỢNG NHÂN VIÊN")),
            "industry": industry_str or ", ".join(get_industry(soup)),
        }

    name = get_name(soup)
    tax_code = get_tax_code(soup)
    address = get_address(soup)
    industries = get_industry(soup)

    if not name and not tax_code:
        print("Could not parse company — no name or tax_code found")
        return None

    return {
        "tax_code": tax_code,
        "name": name,
        "founded_year": extract_label_block(soup, "NĂM THÀNH LẬP"),
        "address": address,
        "district": extract_district(address),
        "employee_size": normalize_employee_size(extract_label_block(soup, "SỐ LƯỢNG NHÂN VIÊN")),
        "industry": ", ".join(industries),
    }


async def process_detail(url: str, client: httpx.AsyncClient, result_buffer: list, sem: asyncio.Semaphore):
    async with sem:
        html = await fetch(client, url)

    if not html:
        return

    data = parse_company(html)
    print(f"PARSED: {data}")
    if data:
        result_buffer.append(data)


async def flush_to_db(buffer: list):
    if not buffer:
        return

    total_inserted = 0
    total_skipped = 0

    global_seen = set()
    deduped = []
    for r in buffer:
        tax_code = (r.get("tax_code") or "").strip()
        if not tax_code or tax_code in global_seen:
            total_skipped += 1
            continue
        global_seen.add(tax_code)
        r["tax_code"] = tax_code
        deduped.append(r)
    buffer.clear()

    for i in range(0, len(deduped), BATCH_SIZE):
        batch = deduped[i:i + BATCH_SIZE]
        try:
            supabase.table("companies").upsert(batch, on_conflict="tax_code").execute()
            total_inserted += len(batch)
            print(f"Inserted batch {i//BATCH_SIZE + 1}: {len(batch)} records")
        except Exception as e:
            print(f"DB error batch {i//BATCH_SIZE + 1}: {e}")

    print(f"\Total: {total_inserted} inserted, {total_skipped} skipped (no tax_code / duplicated)")


async def fetch_all_links(client, start_url: str) -> List[str]:
    all_links = set()
    page = 1

    while True:
        url = f"{start_url}?page={page}" if page > 1 else start_url
        html = await fetch(client, url)

        if not html:
            print(f"Page {page}: fetch failed, stopping")
            break

        links = extract_company_links(html)
        print(f"Page {page}: {len(links)} links")

        if not links:
            print(f"Page {page}: link not found")
            break

        new_links = set(links) - all_links
        if not new_links:
            print(f"Page {page}: duplicated link")
            break

        all_links.update(new_links)
        page += 1
        await asyncio.sleep(0.5)

    print(f"\nTotal: {len(all_links)} links from page {page-1}")
    return list(all_links)

async def fetch_all_category_urls(client) -> List[str]:
    import re as _re
    r = await client.get("https://trangvangvietnam.com", headers=get_headers())
    if r.status_code != 200:
        print(f"Homepage {r.status_code}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    seen = set()
    urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/categories/" in href:
            if href.startswith("/"):
                href = BASE_URL + href
            href = href.split("?")[0]
            if href not in seen:
                seen.add(href)
                urls.append(href)

    print(f"Found {len(urls)} categories in the homepage")
    return urls


# ================= MAIN =================
async def main():
    async with make_client() as client:
        category_urls = await fetch_all_category_urls(client)

        if not category_urls:
            category_urls = [
                "https://trangvangvietnam.com/cateprovinces/92210/xây-dựng-dân-dụng-ở-tại-tp.-hồ-chí-minh-(tphcm).html"
            ]

        all_links = set()
        START_INDEX = 1  # 0 = cat 1, 1 = cat 2, ...
        for i, cat_url in enumerate(category_urls[START_INDEX:], START_INDEX + 1):            
            print(f"\n[{i}/{len(category_urls)}] {cat_url}")
            links = await fetch_all_links(client, cat_url)
            new = set(links) - all_links
            all_links.update(new)
            print(f"  +{len(new)} new links | Total: {len(all_links)}")
            break

        links = list(all_links)
        if not links:
            print("No listings found")
            return

        queue = asyncio.Queue()
        for link in links:
            await queue.put(link)

        result_buffer = []
        sem = asyncio.Semaphore(DETAIL_WORKERS)

        async def worker():
            while True:
                url = await queue.get()
                if url is None:          
                    queue.task_done()
                    break
                try:
                    await process_detail(url, client, result_buffer, sem)
                finally:
                    queue.task_done()

        tasks = [asyncio.create_task(worker()) for _ in range(DETAIL_WORKERS)]

        await queue.join()               

        for _ in range(DETAIL_WORKERS):  
            await queue.put(None)

        await asyncio.gather(*tasks)

        await flush_to_db(result_buffer)

        print(f"\nDONE — {len(result_buffer)} companies remaining in buffer")


if __name__ == "__main__":
    asyncio.run(main())