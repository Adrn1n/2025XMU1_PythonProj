import json
import os


async def write_to_file(data, file_path="cache/baidu_search_res.json"):
    cache_dir = os.path.dirname(file_path)
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)
    print(f"Baidu search results: {file_path}")
