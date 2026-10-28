"""Direct inference test: deepseek-v4.1-flash via call_upstream(use_relay=False).
Key public, relay/proxy/Bun OFF -> jalur direct httpx murni.
"""
import os
import asyncio
import sys

os.environ['OPENCODE_API_KEY'] = 'public'
os.environ['USE_RELAY'] = 'false'
os.environ['USE_PROXY'] = 'false'
os.environ['BUN_SIDECAR_ENABLED'] = 'false'
os.environ['RELAY_FALLBACK'] = 'false'
os.environ['GATEWAY_API_KEYS'] = ''
os.environ['RATE_GUARD_ENABLED'] = 'false'

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.upstream import call_upstream
from app.core.config import OPENCODE_URL, API_KEY

payload = {
    'model': 'deepseek-v4.1-flash',
    'messages': [{'role': 'user', 'content': 'Say the word: pineapple in exactly 5 words.'}],
    'stream': False,
    'max_tokens': 50,
}


async def main():
    print(f'Upstream URL: {OPENCODE_URL}')
    print(f'API_KEY: {API_KEY!r}')
    print('Payload model:', payload['model'])
    print('-' * 60)
    try:
        resp, used = await call_upstream(payload, stream=False, use_relay=False)
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
