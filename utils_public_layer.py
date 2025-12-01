import re, json, time, requests
from typing import Dict, List, Tuple

USER_AGENT = "Class-Project/1.0 (contact: example@class.edu)"


def get_json(url: str, params: Dict = None, method: str = "GET",
             data: Dict = None, timeout: int = 30):
  
    headers = {"User-Agent": USER_AGENT}
    if method == "GET":
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
    else:
        r = requests.post(url, data=data, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()


def pick_polygon_layer(service_root: str) -> Tuple[int, Dict]:
    """
    Return (layer_id, layer_info) for the polygon layer that best matches 'counties'.
    """
    root = get_json(f"{service_root}?f=json")
    layers = root.get("layers", [])
    candidates = []
    for lyr in layers:
        info = get_json(f"{service_root}/{lyr['id']}?f=json")
        if info.get("geometryType") == "esriGeometryPolygon":
            name = (info.get("name") or "").lower()
            score = 2 if re.search(r"\bcount(y|ies)\b", name) else 1
            candidates.append((score, lyr["id"], info))

    if not candidates:
        raise RuntimeError("No polygon layers found.")

    candidates.sort(key=lambda x: (-x[0], x[1]))
    _, layer_id, layer_info = candidates[0]
    return layer_id, layer_info


def layer_urls(service_root: str, layer_id: int) -> Dict[str, str]:
    base = f"{service_root}/{layer_id}"
    return {
        "layer": base,
        "query": f"{base}/query",
        "applyEdits": f"{base}/applyEdits",
    }


def query_all(layer_query_url: str, out_fields: str,
              where: str = "1=1", chunk: int = 5000):
    """
    Generator over all features that match 'where'.
    Does NOT set orderByFields to avoid invalid field issues.
    """
    offset = 0
    while True:
        params = {
            "f": "json",
            "where": where,
            "outFields": out_fields,
            "returnGeometry": "false",
            "resultOffset": offset,
            "resultRecordCount": chunk,
        }
        js = get_json(layer_query_url, params=params)
        feats = js.get("features", [])
        if not feats:
            return
        for f in feats:
            yield f
        offset += len(feats)


def batched(iterable, n: int):
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == n:
            yield batch
            batch = []
    if batch:
        yield batch


def apply_updates(apply_url: str, updates: List[Dict],
   
    total = 0
    for b in batched(updates, batch):
        data = {"f": "json", "updates": json.dumps(b)}
        js = get_json(apply_url, method="POST", data=data)

        if "error" in js:
            print("applyEdits error:", js["error"])
            return total

        if "updateResults" in js:
            successes = [r for r in js["updateResults"] if r.get("success")]
            total += len(successes)
        else:
            print("applyEdits unexpected response:", js)
            return total

        time.sleep(sleep)

    return total
