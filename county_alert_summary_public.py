from datetime import datetime, timezone
from utils_public_layer import (
    get_json,
    pick_polygon_layer,
    layer_urls,
    query_all,
    apply_updates,
)

SERVICE_ROOT = (
    "https://services1.arcgis.com/qr14biwnHA6Vis6l/"
    "arcgis/rest/services/USA_Counties_EWD/FeatureServer"
)
BASE_ALERTS = "https://api.weather.gov/alerts/active"

# If this set is empty, ALL events are accepted.
# To restrict later, put exact event names in this set.
ALLOW_EVENTS = set()


def iter_active_alert_props():
    """
    Iterate over all active alerts from NWS.

    IMPORTANT: no 'limit' parameter – the alerts API can 400 if 'limit' is present.
    Follows pagination.next until there are no more pages.
    """
    url = f"{BASE_ALERTS}?status=actual&message_type=alert"
    while url:
        data = get_json(url)
        for feat in data.get("features", []):
            p = feat.get("properties", {})
            ev = p.get("event")
            # If ALLOW_EVENTS is empty, accept all events; otherwise filter.
            if not ALLOW_EVENTS or ev in ALLOW_EVENTS:
                yield p
        url = data.get("pagination", {}).get("next")


def ww_tag(event_text: str):
    """
    Classify an event name into 'warning', 'watch', or 'advisory'
    based on the presence of those words in the event text.
    """
    e = (event_text or "").lower()
    if "warning" in e:
        return "warning"
    if "watch" in e:
        return "watch"
    return "advisory"


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def main():
    # Find county polygon layer and its objectIdField
    layer_id, layer_info = pick_polygon_layer(SERVICE_ROOT)
    urls = layer_urls(SERVICE_ROOT, layer_id)
    oid_field = layer_info.get("objectIdField", "OBJECTID")

    # Aggregate alerts by county UGC code
    agg = {}  # ugc -> {"warn": set(), "watch": set(), "adv": set()}
    for p in iter_active_alert_props():
        event_name = p.get("event")
        tag = ww_tag(event_name)
        ugcs = (p.get("geocode") or {}).get("UGC") or []
        # Only use county UGCs (3rd char == 'C')
        county_ugcs = [u for u in ugcs if len(u) > 2 and u[2] == "C"]
        for ugc in county_ugcs:
            rec = agg.setdefault(ugc, {"warn": set(), "watch": set(), "adv": set()})
            if tag == "warning":
                rec["warn"].add(event_name)
            elif tag == "watch":
                rec["watch"].add(event_name)
            else:
                rec["adv"].add(event_name)

    print(f"Aggregated alerts for {len(agg)} county UGCs.")

    # Pull counties and build updates
    updates = []
    now = now_iso()
    county_count = 0

    for f in query_all(urls["query"], f"{oid_field},ugc,county_name"):
        a = f.get("attributes", {})
        oid = a.get(oid_field)
        ugc = a.get("ugc")

        county_count += 1

        rec = agg.get(ugc, {"warn": set(), "watch": set(), "adv": set()})
        warning_names = sorted(rec["warn"])
        watch_names = sorted(rec["watch"])
        adv_names = sorted(rec["adv"])

        warning_count = len(warning_names)
        watch_count = len(watch_names)
        advisory_count = len(adv_names)
        status_level = 2 if warning_count > 0 else (1 if watch_count > 0 else 0)

        updates.append(
            {
                "attributes": {
                    oid_field: oid,
                    "status_level": status_level,
                    "warning_count": warning_count,
                    "warning_names": "; ".join(warning_names)
                    if warning_names
                    else None,
                    "watch_count": watch_count,
                    "watch_names": "; ".join(watch_names)
                    if watch_names
                    else None,
                    "advisory_count": advisory_count,
                    "advisory_names": "; ".join(adv_names)
                    if adv_names
                    else None,
                    "last_updated": now,
                }
            }
        )

    print(f"Read {county_count} counties; prepared {len(updates)} updates.")

    if not updates:
        print("No summary updates required.")
        return

    total = apply_updates(urls["applyEdits"], updates, batch=500, sleep=0.2)
    print(f"Updated {total} counties (layer {layer_id}).")


if __name__ == "__main__":
    main()
