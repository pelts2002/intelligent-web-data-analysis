# Запуск программы
# python lab1.py --domain "pstu.ru" --limit 5 --show-text Пермь Политех
# python lab1.py --domain "pstu.ru" --show-text ИТАС
# python lab1.py --domain "https://msu.ru/" --limit 5 --show-text Путин
# python lab1.py новости события --domain "ria.ru" --show-text
# python lab1.py политика экономика --domain "tass.ru" --limit 5 --show-text

import argparse
import requests
import json
import io
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import time

# --------------------------------
# Все функции
# --------------------------------

def get_index(dom, lim=10):
    # Эта штука спрашивает индекс Common Crawl
    url_index = "https://index.commoncrawl.org/CC-MAIN-2025-30-index"
    params = {
        "url": f"{dom}/*",
        "output": "json",
        "fl": "original,timestamp,filename,offset,length"
    }
    try:
        r = requests.get(url_index, params=params, timeout=30)
        r.raise_for_status()
        lista = [json.loads(line) for line in r.text.splitlines()]
        return lista[:lim]
    except Exception as e:
        print("Проблемка с индексом:", e)
        return []

def get_warc(fileee, off, leng):
    # Берем сам файл WARC
    base = "https://data.commoncrawl.org/"
    full = base + fileee.lstrip("/")
    heds = {"Range": f"bytes={off}-{int(off)+int(leng)-1}"}
    try:
        r = requests.get(full, headers=heds, timeout=60)
        r.raise_for_status()
        return ArchiveIterator(io.BytesIO(r.content))
    except Exception as e:
        print("Ошибка с WARC:", e)
        return None

def get_text(warcc):
    # Извлекаем текст из WARC
    for x in warcc:
        try:
            if x.rec_type == "response":
                ctt = x.http_headers.get_header("Content-Type") or ""
                if "html" in ctt.lower() or "text" in ctt.lower():
                    rawr = x.content_stream().read()
                    try:
                        txtt = rawr.decode("utf-8", errors="replace")
                    except Exception:
                        txtt = rawr.decode("latin1", errors="replace")
                    soup = BeautifulSoup(txtt, "html.parser")
                    title = soup.title.string.strip() if soup.title and soup.title.string else None
                    body = soup.get_text(separator=" ", strip=True)
                    return title, body
        except Exception:
            pass
    return None, None

def find_kw_snip(texx, keys, ctx=120):
    # Находим кусок текста с ключевым словом
    if not texx:
        return None
    low = texx.lower()
    for k in keys:
        idx = low.find(k.lower())
        if idx != -1:
            s = max(0, idx - ctx)
            e = min(len(texx), idx + len(k) + ctx)
            return texx[s:e].replace("\n", " ")
    return None

# --------------------------------
# Главная фигня
# --------------------------------

def searchy(keys, dom, lim=10, show=False):
    print("Используем этот индекс: https://index.commoncrawl.org/CC-MAIN-2025-30-index")
    results = get_index(dom, lim*5)
    
    allrows = []
    for r in tqdm(results, desc="Читаем WARC"):
        url = r.get("original")
        date = r.get("timestamp")
        tit = None
        snipp = None
        if show:
            it = get_warc(r["filename"], r["offset"], r["length"])
            if it:
                t, body = get_text(it)
                if t:
                    tit = t
                snipp = find_kw_snip(body, keys)
        allrows.append({
            "URL": url,
            "Дата": date,
            "Заголовок": tit,
            "Фрагмент": snipp
        })
        if len(allrows) >= lim:
            break
        time.sleep(0.1)
    return pd.DataFrame(allrows, columns=["URL", "Дата", "Заголовок", "Фрагмент"])

# --------------------------------
# Запуск из консоли
# --------------------------------

def main():
    parser = argparse.ArgumentParser(description="Ищем в Common Crawl")
    parser.add_argument("keywords", nargs="+", help="Слова для поиска")
    parser.add_argument("--domain", type=str, required=True, help="Фильтр по домену")
    parser.add_argument("--limit", type=int, default=10, help="Сколько результатов")
    parser.add_argument("--show-text", action="store_true", help="Показывать кусок текста")
    args = parser.parse_args()

    df = searchy(args.keywords, args.domain, lim=args.limit, show=args.show_text)
    if df.empty:
        print("Ничего не найдено :(")
    else:
        pd.set_option("display.max_colwidth", 300)
        print(df.to_string(index=False))

if __name__ == "__main__":
    main()