# src/shared/cache.py

from diskcache import Cache

CACHE_DIR = "cache"

def get_cache():
    return Cache(CACHE_DIR)
