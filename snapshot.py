#!/usr/bin/env python3
# -*- coding: utf-8 -*-

Traceback (most recent call last):
  File "/home/runner/work/LIvefeedformygptbro/LIvefeedformygptbro/snapshot.py", line 218, in main
    snap = build_snapshot()
  File "/home/runner/work/LIvefeedformygptbro/LIvefeedformygptbro/snapshot.py", line 185, in build_snapshot
    assets.append(block_for_symbol(sym))
                  ~~~~~~~~~~~~~~~~^^^^^
  File "/home/runner/work/LIvefeedformygptbro/LIvefeedformygptbro/snapshot.py", line 169, in block_for_symbol
    kl, src = fetch_klines_for_symbol(symbol, gran, lim)
              ~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/LIvefeedformygptbro/LIvefeedformygptbro/snapshot.py", line 151, in fetch_klines_for_symbol
    raise RuntimeError(f"All MEXC kline endpoints failed for {symbol}. Last: {last_err}")
RuntimeError: All MEXC kline endpoints failed for BTC_USDT. Last: https://contract.mexc.com/api/v1/contract/kline?symbol=BTC_USDT&interval=Min1&limit=60 => HTTP Error 404: Not Found
Traceback (most recent call last):
  File "/home/runner/work/LIvefeedformygptbro/LIvefeedformygptbro/snapshot.py", line 229, in <module>
    main()
    ~~~~^^
  File "/home/runner/work/LIvefeedformygptbro/LIvefeedformygptbro/snapshot.py", line 218, in main
    snap = build_snapshot()
  File "/home/runner/work/LIvefeedformygptbro/LIvefeedformygptbro/snapshot.py", line 185, in build_snapshot
    assets.append(block_for_symbol(sym))
                  ~~~~~~~~~~~~~~~~^^^^^
  File "/home/runner/work/LIvefeedformygptbro/LIvefeedformygptbro/snapshot.py", line 169, in block_for_symbol
    kl, src = fetch_klines_for_symbol(symbol, gran, lim)
              ~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/LIvefeedformygptbro/LIvefeedformygptbro/snapshot.py", line 151, in fetch_klines_for_symbol
    raise RuntimeError(f"All MEXC kline endpoints failed for {symbol}. Last: {last_err}")
RuntimeError: All MEXC kline endpoints failed for BTC_USDT. Last: https://contract.mexc.com/api/v1/contract/kline?symbol=BTC_USDT&interval=Min1&limit=60 => HTTP Error 404: Not Found
DEBUG fetched from: https://contract.mexc.com/api/v1/contract/kline/BTC_USDT?interval=Min1&limit=60
DEBUG raw preview: {'success': True, 'code': 0, 'data': {'time': [1756147140, 1756147200, 1756147260, 1756147320, 1756147380, 1756147440, 1756147500, 1756147560, 1756147620, 1756147680, 1756147740, 1756147800, 1756147860, 1756147920, 1756147980, 1756148040, 1756148100, 1756148160, 1756148220, 1756148280, 1756148340, 1756148400, 1756148460, 1756148520, 1756148580, 1756148640, 1756148700, 1756148760, 1756148820, 1756148880, 1756148940, 1756149000, 1756149060, 1756149120, 1756149180, 1756149240, 1756149300, 175614936
DEBUG payload head: {'success': True, 'code': 0, 'data': {'time': [1756147140, 1756147200, 1756147260, 1756147320, 1756147380, 1756147440, 1756147500, 1756147560, 1756147620, 1756147680, 1756147740, 1756147800, 1756147860, 1756147920, 1756147980, 1756148040, 1756148100, 1756148160, 1756148220, 1756148280, 1756148340, 1756148400, 1756148460, 1756148520, 1756148580, 1756148640, 1756148700, 1756148760, 1756148820, 1756148880, 1756148940, 1756149000, 1756149060, 1756149120, 1756149180, 1756149240, 1756149300, 175614936
DEBUG endpoint failed: https://contract.mexc.com/api/v1/contract/kline/BTC_USDT?interval=Min1&limit=60 => Unexpected kline payload structure
DEBUG fetched from: https://contract.mexc.com/api/v1/contract/kline/BTC_USDT?interval=Min1&limit=60&page_size=60
DEBUG raw preview: {'success': True, 'code': 0, 'data': {'time': [1756147140, 1756147200, 1756147260, 1756147320, 1756147380, 1756147440, 1756147500, 1756147560, 1756147620, 1756147680, 1756147740, 1756147800, 1756147860, 1756147920, 1756147980, 1756148040, 1756148100, 1756148160, 1756148220, 1756148280, 1756148340, 1756148400, 1756148460, 1756148520, 1756148580, 1756148640, 1756148700, 1756148760, 1756148820, 1756148880, 1756148940, 1756149000, 1756149060, 1756149120, 1756149180, 1756149240, 1756149300, 175614936
DEBUG payload head: {'success': True, 'code': 0, 'data': {'time': [1756147140, 1756147200, 1756147260, 1756147320, 1756147380, 1756147440, 1756147500, 1756147560, 1756147620, 1756147680, 1756147740, 1756147800, 1756147860, 1756147920, 1756147980, 1756148040, 1756148100, 1756148160, 1756148220, 1756148280, 1756148340, 1756148400, 1756148460, 1756148520, 1756148580, 1756148640, 1756148700, 1756148760, 1756148820, 1756148880, 1756148940, 1756149000, 1756149060, 1756149120, 1756149180, 1756149240, 1756149300, 175614936
DEBUG endpoint failed: https://contract.mexc.com/api/v1/contract/kline/BTC_USDT?interval=Min1&limit=60&page_size=60 => Unexpected kline payload structure
DEBUG endpoint failed: https://contract-api.mexc.com/api/v1/contract/kline/BTC_USDT?interval=Min1&limit=60 => <urlopen error [Errno -5] No address associated with hostname>
DEBUG endpoint failed: https://contract.mexc.com/api/v1/contract/kline?symbol=BTC_USDT&interval=Min1&limit=60 => HTTP Error 404: Not Found
FATAL: All MEXC kline endpoints failed for BTC_USDT. Last: https://contract.mexc.com/api/v1/contract/kline?symbol=BTC_USDT&interval=Min1&limit=60 => HTTP Error 404: Not Found
Error: Process completed with exit code 1.
