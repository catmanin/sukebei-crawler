#!/usr/bin/env python3
import asyncio
import json
import re
import random
import argparse
from pathlib import Path
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

# --- Configuration ---
H = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"
}
BASE_URL = "https://sukebei.nyaa.si/view/{}"
STATE_FILE = "crawler_state.json"

DEFAULT_START = 12970
DEFAULT_END = 4172147


def parse_html(html, id_val):
    soup = BeautifulSoup(html, "html.parser")
    if soup.find("div", class_="alert-danger"):
        return None

    title_tag = soup.find("h3", class_="panel-title")
    if not title_tag:
        return None

    res = {"id": id_val, "title": title_tag.get_text(strip=True)}

    magnet_tag = soup.find("a", href=re.compile(r"^magnet:\?"))
    res["magnet"] = magnet_tag["href"] if magnet_tag else None
    if res["magnet"]:
        hash_match = re.search(r"btih:([a-fA-F0-9]{40})", res["magnet"])
        res["info_hash"] = hash_match.group(1).lower() if hash_match else None

    ts_tag = soup.find(attrs={"data-timestamp": True})
    if ts_tag:
        try:
            ts = int(ts_tag["data-timestamp"])
            res["uploaded_at"] = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        except:
            pass

    def get_int(id_attr):
        tag = soup.find(id=id_attr)
        try:
            return int(tag.get_text(strip=True)) if tag else 0
        except:
            return 0

    res["seeders"] = get_int("seeders")
    res["leechers"] = get_int("leechers")

    for row in soup.select(".panel-body .row"):
        cols = row.find_all("div", recursive=False)
        if len(cols) >= 2:
            key = cols[0].get_text(strip=True).rstrip(":")
            val = cols[1].get_text(strip=True)
            if key == "Category":
                res["category"] = val
            elif key == "Submitter":
                res["submitter"] = val
            elif key in ("File size", "Size"):
                res["size"] = val
            elif key == "Completed":
                try:
                    res["completed"] = int(val)
                except:
                    res["completed"] = 0
            elif key == "Information":
                info_link = cols[1].find("a")
                res["information"] = info_link["href"] if info_link else val

    desc_tag = soup.find(id="torrent-description")
    res["description"] = desc_tag.get_text(strip=True) if desc_tag else None

    return res


async def fetch_one(id_val, session, min_delay, max_delay):
    await asyncio.sleep(random.uniform(min_delay, max_delay))
    try:
        resp = await session.get(BASE_URL.format(id_val), timeout=15)
        if resp.status_code == 404:
            return id_val, None, "404"
        if resp.status_code == 429:
            return id_val, None, "429"
        resp.raise_for_status()
        data = parse_html(resp.text, id_val)
        return id_val, data, "ok" if data else "parse_fail"
    except Exception as e:
        return id_val, None, str(e)[:50]


def save_local_batch(results, fh):
    if not results:
        return
    for r in results:
        fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    fh.flush()


def save_state(progress, count, output_file):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump({
                "progress": progress,
                "count": count,
                "last_output": str(output_file)
            }, f)
    except Exception as e:
        print(f" [!] Failed to save state: {e}")


def load_state():
    try:
        if Path(STATE_FILE).exists():
            with open(STATE_FILE, "r") as f:
                return json.load(f)
    except:
        pass
    return {"progress": DEFAULT_START, "count": 0, "last_output": None}


def make_output_filename():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"nyaa_results_{ts}.jsonl"


async def run_crawler(start_id, end_id, workers, min_delay, max_delay, proxy, output_file, batch_size):
    state = load_state()

    # Resume from saved progress if using default start
    if start_id == DEFAULT_START and state["progress"] > DEFAULT_START:
        print(f"[*] Resuming from saved progress: {state['progress']}")
        start_id = state["progress"]

    current_count = state.get("count", 0)

    # Going upward: target_end = start + batch_size - 1, capped at end_id
    if batch_size > 0:
        target_end = min(end_id, start_id + batch_size - 1)
    else:
        target_end = end_id

    if start_id > end_id:
        print(f"[*] Crawler already reached the end ID ({end_id}). Nothing to do.")
        return

    print(f"[*] Starting crawler: {start_id} → {target_end} (global end: {end_id})")
    print(f"[*] Output file: {output_file}")
    print(f"[*] Config: workers={workers}, delay={min_delay}-{max_delay}s, batch_size={batch_size}")

    queue = asyncio.Queue(maxsize=workers * 2)
    stop_event = asyncio.Event()
    results_batch = []
    processed_count = 0
    found_count = 0
    lock = asyncio.Lock()

    fh = open(output_file, "w", encoding="utf-8")  # new file each run

    async def worker_task(session):
        nonlocal current_count, found_count, processed_count
        while not stop_event.is_set():
            try:
                id_val = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            id_res, data, status_msg = await fetch_one(id_val, session, min_delay, max_delay)

            async with lock:
                processed_count += 1
                if data:
                    found_count += 1
                    results_batch.append(data)

                if status_msg == "429":
                    print(f" [!] 429 on #{id_val}, sleeping 30s...")
                    await asyncio.sleep(30)

                if processed_count % 50 == 0:
                    print(f" [{processed_count}] id=#{id_val} | found={found_count} | status={status_msg}")

                if len(results_batch) >= 10 or processed_count % 50 == 0:
                    save_local_batch(results_batch, fh)
                    results_batch.clear()
                    save_state(id_val + 1, current_count + found_count, output_file)
                    # +1 so next run starts from the next unprocessed ID

            queue.task_done()

    proxies = {"http": proxy, "https": proxy} if proxy else None
    async with AsyncSession(headers=H, proxies=proxies, impersonate="chrome110") as session:
        worker_tasks = [asyncio.create_task(worker_task(session)) for _ in range(workers)]

        # Upward: range(start, target_end + 1)
        for i in range(start_id, target_end + 1):
            await queue.put(i)

        await queue.join()
        stop_event.set()

        if results_batch:
            save_local_batch(results_batch, fh)

        # Save next start = target_end + 1
        save_state(target_end + 1, current_count + found_count, output_file)

        for t in worker_tasks:
            t.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)

    fh.close()
    print(f"[*] Done. processed={processed_count}, found={found_count}, file={output_file}")
    print(f"[*] Next run will start from ID: {target_end + 1}")


if __name__ == "__main__":
    default_output = make_output_filename()

    parser = argparse.ArgumentParser(description="Sukebei Nyaa async crawler (ascending)")
    parser.add_argument("--start", type=int, default=DEFAULT_START, help=f"Start ID (default: {DEFAULT_START})")
    parser.add_argument("--end", type=int, default=DEFAULT_END, help=f"End ID (default: {DEFAULT_END})")
    parser.add_argument("--workers", type=int, default=6, help="Concurrent workers")
    parser.add_argument("--proxy", type=str, help="Proxy URL e.g. socks5://127.0.0.1:10808")
    parser.add_argument("--min-delay", type=float, default=0.6)
    parser.add_argument("--max-delay", type=float, default=0.8)
    parser.add_argument("--output", type=str, default=default_output, help="Output JSONL file (timestamped by default)")
    parser.add_argument("--batch-size", type=int, default=1100, help="IDs to process per run (0 = unlimited)")
    args = parser.parse_args()

    asyncio.run(run_crawler(
        args.start, args.end, args.workers,
        args.min_delay, args.max_delay,
        args.proxy, args.output, args.batch_size
    ))
