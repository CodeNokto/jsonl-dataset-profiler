import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional


def detect_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    if isinstance(value, list):
        return "list"
    if isinstance(value, dict):
        return "object"
    return "other"


def merge_profile(dst: Dict[str, Any], src: Dict[str, Any]) -> Dict[str, Any]:
    for field, s in src.items():
        if field not in dst:
            dst[field] = s
            continue
        d = dst[field]
        d["count"] += s["count"]
        d["null_count"] += s["null_count"]
        for t, c in s["types"].items():
            d["types"][t] = d["types"].get(t, 0) + c
    return dst


def profile_batch(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    profile: Dict[str, Any] = {}
    for rec in records:
        if not isinstance(rec, dict):
            continue
        for k, v in rec.items():
            if k not in profile:
                profile[k] = {
                    "count": 0,
                    "null_count": 0,
                    "types": {},
                }
            p = profile[k]
            p["count"] += 1
            if v is None:
                p["null_count"] += 1
            t = detect_type(v)
            p["types"][t] = p["types"].get(t, 0) + 1
    return profile


def profile_jsonl(path: Path, max_rows: Optional[int] = None, batch_size: int = 1000) -> Dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Fant ikke fil: {path}")
    total_rows = 0
    profile: Dict[str, Any] = {}
    batch: List[Dict[str, Any]] = []

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                continue
            batch.append(obj)
            total_rows += 1
            if max_rows is not None and total_rows >= max_rows:
                break
            if len(batch) >= batch_size:
                batch_profile = profile_batch(batch)
                profile = merge_profile(profile, batch_profile)
                batch = []

    if batch:
        batch_profile = profile_batch(batch)
        profile = merge_profile(profile, batch_profile)  # type: ignore[name-defined]

    result = {"total_rows": total_rows, "fields": profile}
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Profiler JSONL-datasett.")
    parser.add_argument("--input", required=True, help="Sti til JSONL-fil.")
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Maks antall rader å lese (for sampling).",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Sti til JSON-fil med profil.",
    )

    args = parser.parse_args()
    path = Path(args.input)
    output = Path(args.output)
    result = profile_jsonl(path, max_rows=args.max_rows)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"Skrev profil til {output}")


if __name__ == "__main__":
    main()
