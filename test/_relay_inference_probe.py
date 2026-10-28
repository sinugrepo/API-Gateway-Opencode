"""Relay Vercel inference test: deepseek-v4.1-flash via USE_RELAY=true.
Relay menyimpan OPENCODE_API_KEY server-side -> tidak perlu key lokal.
"""
import os
import asyncio
import sys

os.environ['OPENCODE_API_KEY'] = 'public'
os.environ['USE_RELAY'] = 'true'
os.environ['USE_PROXY'] = 'false'
os.environ['BUN_SIDECAR_ENABLED'] = 'false'
os.environ['RELAY_FALLBACK'] = 'true'
os.environ['GATEWAY_API_KEYS'] = ''
os.environ['RATE_GUARD_ENABLED'] = 'false'
os.environ['REQUEST_TIMEOUT'] = '60'

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.upstream import call_upstream
from app.core.config import RELAY_URLS, OPENCODE_URL

payload = {
    'model': 'deepseek-v4.1-flash',
    'messages': [{'role': 'user', 'content': 'Say the word: pineapple in exactly 5 words.'}],
    'stream': False,
    'max_tokens': 50,
}


async def main():
    print(f'Upstream URL: {OPENCODE_URL}')
    print(f'Relay count: {len(RELAY_URLS)}')
    print('Relay list:')
    for u in RELAY_URLS:
        print(f'  - {u}')
    print('-' * 60)
    try:
        resp, used = await call_upstream(payload, stream=False, use_relay=True)
        print('STATUS:', resp.status_code)
        print('USED:', used)
        body = resp.text[:1500]
        print('BODY:')
        print(body)
    except Exception as exc:
        print(f'EXCEPTION: {type(exc).__name__}: {exc}')
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())
