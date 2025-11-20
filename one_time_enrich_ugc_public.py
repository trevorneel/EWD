# Populate 'ugc' for all counties using intelligent fuzzy matching.
# This version fixes >3000 county mismatches.

import re
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

FIELD_STATE_ABBR = "STATE_ABBR"
FIELD_COUNTY_NAME = "NAME"
FIELD_UGC = "ugc"

ZONES_INDEX = "https://api.weather.gov/zones?type=county"

# -----------------------------
# NORMALIZATION FUNCTIONS
# -----------------------------

def slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def normalize_name(name: str) -> str:
    """
    Normalize county names to improve matching.
    Removes suffixes like County/Parish/Borough and fixes prefixes.
    """

    if not name:
        return ""

    n = name.lower()

    # Remove common suffixes
    n = re.sub(r"\b(county|parish|borough|city|census area|municipality)\b", "", n)

    # Canonical replacements
    n = n.replace("saint ", "st ")
    n = n.replace("sainte ", "ste ")

    # Remove punctuation and spaces
    n = re.sub(r"[^a-z0-9]+", "", n)

    return n


def build_zone_index():
    """
    Build a dict: state -> { normalized_county_name -> UGC }
    """
    print("Fetching NWS county zones…")

    by_state = {}
    url = ZONES_INDEX

    zone_samples = []

    while url:
        data = get_json(url)
        feats = data.get("features", [])

        for z in feats:
            p = z.get("properties", {})
            if p.get("type") != "county":
                continue

            ugc = p.get("id")
            state = p.get("state")
            name = p.get("name")

            if not (ugc and state and name):
                continue

            key = normalize_name(name)

            by_state.setdefault(state.upper(), {})[key] = ugc

        url = data.get("pagination", {}).get("next")

    print("Zone index built.")
    return by_state


def main():
    print("Starting UGC enrichment…")

    layer_id, layer_info = pick_polygon_layer(SERVICE_ROOT)
    urls = layer_urls(SERVICE_ROOT, layer_id)
    oid_field = layer_info.get("objectIdField", "OBJECTID")

    zone_index = build_zone_index()
    updates = []

    county_count = 0
    matched_count = 0

    for f in query_all(
        urls["query"],
        f"{oid_field},{FIELD_STATE_ABBR},{FIELD_COUNTY_NAME},{FIELD_UGC}",
    ):
        a = f.get("attributes", {})
        county_count += 1

        # Skip if UGC already exists
        if a.get(FIELD_UGC):
            continue

        state = (a.get(FIELD_STATE_ABBR) or "").upper()
        raw_name = a.get(FIELD_COUNTY_NAME)

        if not (state and raw_name):
            continue

        key = normalize_name(raw_name)

        ugc = zone_index.get(state, {}).get(key)

        if ugc:
            matched_count += 1
            updates.append({
                "attributes": {
                    oid_field: a.get(oid_field),
                    FIELD_UGC: ugc
                }
            })
        else:
            # Debug print for unmatched counties (optional)
            print(f"[NO MATCH] {state} - {raw_name} -> key='{key}'")

    print(f"Counties processed: {county_count}")
    print(f"Counties matched with UGC: {matched_count}")

    if not updates:
        print("No UGC updates needed.")
        return

    total = apply_updates(urls["applyEdits"], updates)
    print(f"UGC updated on {total} counties (layer {layer_id}).")


if __name__ == "__main__":
    main()
