# -*- coding: utf-8 -*-
"""
数据层 v4 - A股日K线本地数据库管理
数据源: AKShare（免费，无需token，pip install akshare）
"""
import sqlite3
import sys, os, time
import numpy as np
from datetime import datetime, timedelta

sys.stdout.reconfigure(encoding='utf-8') if sys.stdout else None

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'stock_data.db')

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS daily_kline (
            date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL, volume REAL,
            PRIMARY KEY (date, symbol)
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS stock_meta (
            symbol TEXT PRIMARY KEY,
            name TEXT,
            exchange TEXT,
            list_date TEXT,
            delist_date TEXT,
            is_suspended INTEGER DEFAULT 0
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS update_log (
            update_time TEXT PRIMARY KEY,
            action TEXT,
            count INTEGER,
            note TEXT
        )
    ''')
    conn.commit()
    conn.close()

def _ak_get_stock_list():
    """用AKShare获取A股列表"""
    import akshare as ak
    try:
        # 沪深A股实时行情（获取所有代码）
        df = ak.stock_zh_a_spot_em()
        stocks = []
        for _, row in df.iterrows():
            code = str(row['代码'])
            name = str(row['名称'])
            if code.startswith('6'):
                sym = f'SHSE.{code}'
            else:
                sym = f'SZSE.{code}'
            stocks.append({
                'symbol': sym,
                'name': name,
                'exchange': sym[:4],
                'list_date': '',
            })
        return stocks
    except Exception as e:
        print(f'AKShare获取股票列表失败: {e}')
        return []

def _ak_download_history(symbols, start_date, end_date):
    """用AKShare批量下载历史K线"""
    import akshare as ak
    results = []
    for sym in symbols:
        try:
            code = sym.split('.')[1]
            df = ak.stock_zh_a_hist(
                symbol=code,
                period='daily',
                start_date=start_date.replace('-', ''),
                end_date=end_date.replace('-', ''),
                adjust='qfq'
            )
            if df is not None and len(df) > 0:
                for _, row in df.iterrows():
                    results.append({
                        'symbol': sym,
                        'date': str(row['日期'])[:10],
                        'open': float(row['开盘']),
                        'high': float(row['最高']),
                        'low': float(row['最低']),
                        'close': float(row['收盘']),
                        'volume': float(row['成交量']),
                    })
        except Exception:
            pass
        time.sleep(0.2)  # 防限流
    return results

def _efinance_download_history(symbols, start_date, end_date):
    """用 efinance 批量下载历史K线（替代掘金）"""
    try:
        import efinance as ef
    except ImportError:
        print('[efinance] 未安装，请运行: pip install efinance')
        return []
    
    results = []
    for sym in symbols:
        try:
            code = sym.split('.')[1]  # SHSE.600545 → 600545
            # klt=101=日K, fqt=1=前复权
            df = ef.stock.get_quote_history(code, klt=101, fqt=1)
            if df is None or len(df) == 0:
                continue
            
            for _, row in df.iterrows():
                date_str = str(row['日期'])[:10]
                if start_date <= date_str <= end_date:
                    results.append({
                        'symbol': sym,
                        'date': date_str,
                        'open': float(row['开盘']),
                        'high': float(row['最高']),
                        'low': float(row['最低']),
                        'close': float(row['收盘']),
                        'volume': float(row['成交量']),  # efinance 成交量已是股数
                    })
        except Exception as e:
            pass
        time.sleep(0.3)  # 防限流
    return results


def _efinance_get_realtime_quotes(symbols):
    """用 efinance 获取实时行情（用于监控）
    注意: efinance 实时接口只支持单只查询，需循环调用
    返回: {symbol: {price, change_pct, volume, ...}}
    """
    try:
        import efinance as ef
    except ImportError:
        return {}
    
    result = {}
    for sym in symbols:
        try:
            code = sym.split('.')[1]
            df = ef.stock.get_latest_quote(code)
            if df is None or len(df) == 0:
                continue
            row = df.iloc[0]
            result[sym] = {
                'price': float(row['最新价']),
                'change_pct': float(row['涨跌幅']),
                'volume': float(row['成交量']),
                'amount': float(row['成交额']),
                'high': float(row['最高']),
                'low': float(row['最低']),
                'open': float(row['今开']),
                'prev_close': float(row['昨收']),
            }
        except Exception:
            continue
        time.sleep(0.2)  # 防限流
    return result


def check_data_age():
    if not os.path.exists(DB_PATH):
        return True, '数据库不存在'
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT MAX(date) FROM daily_kline')
    row = c.fetchone()
    conn.close()
    if not row or not row[0]:
        return True, '数据库为空'
    last_date = datetime.strptime(row[0], '%Y-%m-%d').date()
    today = datetime.now().date()
    diff = (today - last_date).days
    if diff <= 1:
        return False, f'已最新 ({last_date})'
    return True, f'过期{diff}天 ({last_date})'

def full_download(use_efinance=True):
    src = 'efinance' if use_efinance else 'AKShare'
    print(f'\n=== 首次全量下载 ({src}) ===')
    init_db()
    stocks = _ak_get_stock_list()
    if not stocks:
        print('获取股票列表失败，使用备用方案')
        # 备用：手动生成常见代码
        stocks = []
        for i in range(600000, 601000):
            stocks.append({'symbol': f'SHSE.{i:06d}', 'name': '', 'exchange': 'SHSE', 'list_date': ''})
        for i in range(1, 5000):
            stocks.append({'symbol': f'SZSE.{i:06d}', 'name': '', 'exchange': 'SZSE', 'list_date': ''})

    print(f'  A股: {len(stocks)}只')

    conn = get_db()
    c = conn.cursor()
    for s in stocks:
        c.execute('INSERT OR REPLACE INTO stock_meta (symbol, name, exchange, list_date) VALUES (?,?,?,?)',
                  (s['symbol'], s['name'], s['exchange'], s['list_date']))
    conn.commit()
    print(f'  元数据已写入')

    total = len(stocks)
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')

    ok = 0; fail = 0; total_rows = 0
    t0 = time.time()

    # 数据下载：优先 efinance，fallback AKShare
    download_fn = _efinance_download_history if use_efinance else _ak_download_history
    batch_size = 50
    for i in range(0, total, batch_size):
        batch = stocks[i:i+batch_size]
        syms = [s['symbol'] for s in batch]
        
        try:
            rows_data = download_fn(syms, start_date, end_date)
            if rows_data:
                c.executemany(
                    'INSERT OR REPLACE INTO daily_kline (date,symbol,open,high,low,close,volume) VALUES (?,?,?,?,?,?,?)',
                    [(r['date'], r['symbol'], r['open'], r['high'], r['low'], r['close'], r['volume']) for r in rows_data]
                )
                total_rows += len(rows_data)
                ok += len(syms)
            else:
                fail += len(syms)
        except Exception as e:
            fail += len(syms)
            if fail <= 3:
                print(f'    ERR: {e}')

        elapsed = time.time() - t0
        done = min(i + batch_size, total)
        print(f'  [{done}/{total}] OK={ok} FAIL={fail} rows={total_rows} ({elapsed:.0f}s)', flush=True)
        time.sleep(0.5)

    conn.commit()
    conn.close()
    dt = time.time() - t0
    print(f'\n  完成! OK={ok} FAIL={fail} rows={total_rows} 耗时{dt:.0f}s ({dt/60:.1f}min)')

    conn = get_db()
    conn.execute("INSERT INTO update_log (update_time, action, count, note) VALUES (datetime('now','localtime'), 'full', ?, ?)",
                 (total_rows, f'ok={ok} fail={fail}'))
    conn.commit()
    conn.close()

def incremental_update(progress_callback=None, use_efinance=True):
    """增量更新数据库
    progress_callback: callable(current, total) 用于UI进度显示
    """
    src = 'efinance' if use_efinance else 'AKShare'
    print(f'\n=== 增量更新 ({src}) ===')
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT MAX(date) FROM daily_kline')
    row = c.fetchone()
    last = row[0] if row and row[0] else None
    conn.close()

    if not last:
        print('  空库，全量下载')
        full_download(use_efinance=use_efinance)
        return

    stocks = _ak_get_stock_list()
    if not stocks:
        print('获取股票列表失败')
        return

    total = len(stocks)
    end_date = datetime.now().strftime('%Y-%m-%d')
    
    # 如果数据库已有今天的数据，直接跳过
    if last >= end_date:
        print(f'  数据库已是最新 ({last})，无需更新')
        if progress_callback:
            try: progress_callback(total, total)
            except: pass
        return
    
    total_new = 0; ok = 0; fail = 0
    conn = get_db()
    c = conn.cursor()
    t0 = time.time()
    
    download_fn = _efinance_download_history if use_efinance else _ak_download_history
    batch_size = 50

    for i in range(0, total, batch_size):
        batch = stocks[i:i+batch_size]
        syms = [s['symbol'] for s in batch]
        try:
            rows_data = download_fn(syms, last, end_date)
            if rows_data:
                c.executemany(
                    'INSERT OR REPLACE INTO daily_kline (date,symbol,open,high,low,close,volume) VALUES (?,?,?,?,?,?,?)',
                    [(r['date'], r['symbol'], r['open'], r['high'], r['low'], r['close'], r['volume']) for r in rows_data])
                total_new += len(rows_data)
                ok += len(syms)
        except Exception as e:
            fail += len(syms)
            if fail <= 3:
                print(f'    批次失败: {e}', flush=True)

        current = min(i + batch_size, total)
        # 每批都回调进度
        if progress_callback:
            try: progress_callback(current, total)
            except: pass
        if current % 200 == 0 or current == total:
            print(f'  [{current}/{total}] new={total_new} ({time.time()-t0:.0f}s)', flush=True)
        time.sleep(0.3)

    conn.commit()
    conn.close()
    dt = time.time() - t0
    print(f'\n  完成! 新增{total_new}条 ok={ok} fail={fail} 耗时{dt:.0f}s')

def get_kline(symbol, start_date=None, end_date=None):
    import pandas as pd
    if not os.path.exists(DB_PATH):
        return None
    conn = get_db()
    q = 'SELECT date, open, high, low, close, volume FROM daily_kline WHERE symbol = ?'
    p = [symbol]
    if start_date:
        q += ' AND date >= ?'; p.append(start_date)
    if end_date:
        q += ' AND date <= ?'; p.append(end_date)
    q += ' ORDER BY date'
    df = pd.read_sql_query(q, conn, params=p)
    conn.close()
    return df if not df.empty else None

def sim_calc(ref_seg, cand_seg):
    """形态相似度计算"""
    try:
        ref_close = ref_seg['close'].values.astype(float)
        cand_close = cand_seg['close'].values.astype(float)
        n = min(len(ref_close), len(cand_close))
        if n < 5:
            return 0.0

        rc = ref_close[-n:]
        cc = cand_close[-n:]

        r_norm = rc / rc[0] if rc[0] > 0 else rc
        c_norm = cc / cc[0] if cc[0] > 0 else cc

        dot = np.dot(r_norm, c_norm)
        norm_r = np.linalg.norm(r_norm)
        norm_c = np.linalg.norm(c_norm)
        cosine_sim = dot / (norm_r * norm_c) if (norm_r > 0 and norm_c > 0) else 0

        r_pct = np.diff(rc) / rc[:-1] if len(rc) > 1 else np.array([0])
        c_pct = np.diff(cc) / cc[:-1] if len(cc) > 1 else np.array([0])
        r_pct_mean = np.mean(r_pct)
        c_pct_mean = np.mean(c_pct)
        r_pct_dev = r_pct - r_pct_mean
        c_pct_dev = c_pct - c_pct_mean
        denom = np.sqrt(np.sum(r_pct_dev**2) * np.sum(c_pct_dev**2))
        pearson = np.sum(r_pct_dev * c_pct_dev) / denom if denom > 0 else 0

        ref_range = (np.max(rc) - np.min(rc)) / rc[0] if rc[0] > 0 else 0
        cand_range = (np.max(cc) - np.min(cc)) / cc[0] if cc[0] > 0 else 0
        range_diff = abs(ref_range - cand_range)
        amp_sim = max(0, 1 - range_diff)

        sim = (cosine_sim * 0.35 + pearson * 0.35 + amp_sim * 0.3) * 100
        return round(max(0, min(100, sim)), 1)
    except Exception:
        return 0.0


def db_info():
    if not os.path.exists(DB_PATH):
        print('数据库不存在')
        return
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT COUNT(DISTINCT symbol) FROM daily_kline')
    print(f'  股票数: {c.fetchone()[0]}')
    c.execute('SELECT COUNT(*) FROM daily_kline')
    print(f'  K线数: {c.fetchone()[0]:,}')
    c.execute('SELECT MAX(date) FROM daily_kline')
    print(f'  最新: {c.fetchone()[0]}')
    conn.close()

if __name__ == '__main__':
    need, msg = check_data_age()
    print(f'数据状态: {msg}')
    if need:
        if not os.path.exists(DB_PATH):
            full_download()
        else:
            incremental_update()
    else:
        db_info()
