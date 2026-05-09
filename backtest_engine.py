"""
回测引擎 v3 - 完整 L1/L2/L3
1. L3: 风险过滤(ST/半年涨幅/筹码集中)
2. L2: 突破+连阳
3. L1: K线相似度匹配(DTW+cosine+pearson)
4. 预构建numpy索引, O(1)访问
"""

import sqlite3
import os
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Optional

# ── L1 K线相似度计算 ──
def _draw_mini_kline(closes, highs, lows, volumes, save_path):
    """用 matplotlib 从数据画 K 线图并保存，供 _extract_candle_centers 解析"""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.finance import candlestick_ohlc
    from matplotlib.dates import date2num
    import datetime
    
    n = len(closes)
    if n < 4:
        return False
    
    # 构建 OHLC 数据
    dates = [datetime.datetime(2026, 1, 1) + datetime.timedelta(days=i) for i in range(n)]
    ohlc = []
    for i in range(n):
        ohlc.append([
            date2num(dates[i]),
            float(highs[i]),
            float(lows[i]),
            float(closes[i]),
            float(volumes[i]) if volumes is not None else 1e6
        ])
    
    fig, ax = plt.subplots(figsize=(8, 3), dpi=100)
    fig.patch.set_facecolor('white')
    ax.set_facecolor('white')
    
    candlestick_ohlc(ax, ohlc, width=0.6, colorup='red', colordown='green')
    
    ax.set_xlim(0, n)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.spines['left'].set_visible(False)
    
    plt.tight_layout(pad=0)
    plt.savefig(save_path, dpi=100, facecolor='white', bbox_inches='tight', pad_inches=0.05)
    plt.close(fig)
    return True


def _calc_similarity(ul, cl):
    """
    计算K线相似度(DTW + cosine + pearson + range/std)
    ul, cl: numpy 1D arrays (蜡烛中心点归一化序列)
    返回: 0-100 相似度分数
    """
    from sklearn.metrics.pairwise import cosine_similarity
    from scipy.stats import pearsonr
    import numba

    @numba.jit(nopython=True)
    def _fast_dtw(s1, s2):
        n, m = len(s1), len(s2)
        if n == 0 or m == 0:
            return 0.0
        dt = np.full((n + 1, m + 1), np.inf)
        dt[0, 0] = 0
        for i in range(1, n + 1):
            for j in range(1, m + 1):
                dt[i, j] = abs(s1[i-1] - s2[j-1]) + min(dt[i-1, j], dt[i, j-1], dt[i-1, j-1])
        mpd = max(np.ptp(s1), np.ptp(s2)) * max(n, m)
        return max(0.0, min(100.0, 100.0 * (1.0 - dt[n, m] / mpd))) if mpd > 0 else 100.0

    try:
        uf, cf = ul.flatten(), cl.flatten()
        cs = cosine_similarity(uf.reshape(1, -1), cf.reshape(1, -1))[0][0]
        try:
            pc, _ = pearsonr(uf, cf)
            ps = ((pc + 1) / 2) * 100 if not np.isnan(pc) else 0
        except:
            ps = 0
        ur, cr2 = np.ptp(uf), np.ptp(cf)
        us, css = np.std(uf), np.std(cf)
        rs = 100 * (1 - abs(ur - cr2) / max(ur, cr2)) if max(ur, cr2) > 1e-6 else 100
        ss = 100 * (1 - abs(us - css) / max(us, css)) if max(us, css) > 1e-6 else 100
        return round(max(0, min(100, 0.3 * ((cs + 1) / 2) * 100 + 0.2 * ps + 0.25 * (rs + ss) / 2 + 0.25 * _fast_dtw(uf, cf))), 1)
    except:
        return 0.0


def _extract_candle_centers(image_path):
    """解析K线图片, 提取中心点归一化序列"""
    import cv2
    print(f"[L1] extract_candle_centers: {image_path}")
    img = cv2.imread(image_path)
    if img is None:
        print(f"[L1] cv2.imread returned None")
        return None, '无法读取图片'
    print(f"[L1] image loaded: {img.shape}")
    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
    lr1 = np.array([0, 30, 50]); ur1 = np.array([15, 255, 255])
    lr2 = np.array([155, 30, 50]); ur2 = np.array([180, 255, 255])
    lg = np.array([25, 30, 50]); ug = np.array([95, 255, 255])
    mr = cv2.add(cv2.inRange(hsv, lr1, ur1), cv2.inRange(hsv, lr2, ur2))
    mg = cv2.inRange(hsv, lg, ug)
    k = np.ones((3, 3), np.uint8)
    mr = cv2.morphologyEx(mr, cv2.MORPH_CLOSE, k)
    mr = cv2.morphologyEx(mr, cv2.MORPH_OPEN, k)
    mg = cv2.morphologyEx(mg, cv2.MORPH_CLOSE, k)
    mg = cv2.morphologyEx(mg, cv2.MORPH_OPEN, k)
    cr, _ = cv2.findContours(mr, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    cg, _ = cv2.findContours(mg, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    candles = []
    for cnt in cr + cg:
        if cv2.contourArea(cnt) > 5:
            x, y, w, h = cv2.boundingRect(cnt)
            if w > 0 and 0.1 < h / w < 10:
                candles.append((x + w / 2, y + h / 2))
    candles = sorted(candles, key=lambda c: c[0])
    print(f"[L1] found {len(candles)} candle centers")
    if len(candles) < 4:
        return None, f'只检测到{len(candles)}根K线'
    cy = np.array([c[1] for c in candles])
    yn, yx = cy.min(), cy.max()
    yr = yx - yn if yx != yn else 1e-6
    return np.array([(yx - c[1]) / yr for c in candles][-40:]), f'检测到{len(candles)}根K线'


class BacktestConfig:
    def __init__(self,
                 start_date='2025-01-01',
                 end_date='2026-05-06',
                 initial_capital=100000,
                 max_positions=5,
                 commission=0.0003,
                 stamp_tax=0.001,
                 slippage=0.001,
                 exclude_st=True,
                 half_year_limit=110,
                 concentration=75,
                 breakout_days=60,
                 breakout_threshold=0.98,
                 limit_up_days=5,
                 similarity_threshold=73,
                 ref_image=None,
                 stop_loss=0.05,
                 take_profit=0.20,
                 ):
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        self.max_positions = max_positions
        self.commission = commission
        self.stamp_tax = stamp_tax
        self.slippage = slippage
        self.exclude_st = exclude_st
        self.half_year_limit = half_year_limit
        self.concentration = concentration
        self.breakout_days = breakout_days
        self.breakout_threshold = breakout_threshold
        self.limit_up_days = limit_up_days
        self.similarity_threshold = similarity_threshold
        self.ref_image = ref_image
        self.stop_loss = stop_loss
        self.take_profit = take_profit


class Position:
    def __init__(self, symbol, entry_price, entry_date, shares):
        self.symbol = symbol
        self.entry_price = float(entry_price)
        self.entry_date = entry_date
        self.shares = shares


class Trade:
    def __init__(self, symbol, buy_date, buy_price, sell_date=None, sell_price=None, reason=''):
        self.symbol = symbol
        self.buy_date = buy_date
        self.buy_price = float(buy_price)
        self.sell_date = sell_date
        self.sell_price = float(sell_price) if sell_price is not None else None
        self.reason = reason
    
    @property
    def pnl_pct(self):
        if self.sell_price is None:
            return 0
        return (self.sell_price - self.buy_price) / self.buy_price * 100


class DailyRecord:
    def __init__(self, date, cash, market_value, total_value, position_count):
        self.date = date
        self.cash = cash
        self.market_value = market_value
        self.total_value = total_value
        self.position_count = position_count


class BacktestResult:
    def __init__(self):
        self.daily_records = []
        self.trades = []
        self.config = None
    
    def to_dict(self):
        """序列化回测结果为字典（供UI使用）"""
        return {
            'daily': [
                {'date': r.date, 'total_value': r.total_value, 'cash': r.cash,
                 'market_value': r.market_value, 'pos_count': r.position_count}
                for r in self.daily_records
            ],
            'trades': [
                {
                    'symbol': t.symbol,
                    'buy_date': str(t.buy_date),
                    'buy_price': float(t.buy_price),
                    'sell_date': str(t.sell_date) if t.sell_date else '',
                    'sell_price': float(t.sell_price) if t.sell_price else 0,
                    'pnl_pct': round(float(t.pnl_pct), 2),
                    'reason': t.reason
                }
                for t in self.trades
            ],
            'summary': self.summary()
        }
    
    @property
    def initial_capital(self):
        return self.daily_records[0].total_value if self.daily_records else 0
    
    @property
    def final_value(self):
        return self.daily_records[-1].total_value if self.daily_records else 0
    
    @property
    def total_return(self):
        if not self.daily_records:
            return 0
        return (self.final_value - self.initial_capital) / self.initial_capital * 100
    
    @property
    def trading_days(self):
        return len(self.daily_records)
    
    @property
    def annual_return(self):
        r = self.total_return / 100
        years = self.trading_days / 252
        if years <= 0:
            return 0
        return ((1 + r) ** (1 / years) - 1) * 100
    
    @property
    def max_drawdown(self):
        if not self.daily_records:
            return 0
        peak = self.daily_records[0].total_value
        max_dd = 0
        for r in self.daily_records:
            if r.total_value > peak:
                peak = r.total_value
            dd = (peak - r.total_value) / peak
            if dd > max_dd:
                max_dd = dd
        return max_dd * 100
    
    @property
    def win_rate(self):
        closed = [t for t in self.trades if t.sell_price is not None]
        if not closed:
            return 0
        wins = sum(1 for t in closed if t.pnl_pct > 0)
        return wins / len(closed) * 100
    
    @property
    def profit_loss_ratio(self):
        closed = [t for t in self.trades if t.sell_price is not None]
        if not closed:
            return 0
        wins = [t.pnl_pct for t in closed if t.pnl_pct > 0]
        losses = [abs(t.pnl_pct) for t in closed if t.pnl_pct < 0]
        avg_win = np.mean(wins) if wins else 0
        avg_loss = np.mean(losses) if losses else 1
        return avg_win / avg_loss if avg_loss > 0 else 0
    
    @property
    def sharpe_ratio(self):
        if len(self.daily_records) < 2:
            return 0
        daily_returns = []
        for i in range(1, len(self.daily_records)):
            r = (self.daily_records[i].total_value - self.daily_records[i-1].total_value) / self.daily_records[i-1].total_value
            daily_returns.append(r)
        if not daily_returns:
            return 0
        return np.mean(daily_returns) / (np.std(daily_returns) + 1e-10) * np.sqrt(252)
    
    def summary(self):
        return {
            'Total Return': f'{self.total_return:.2f}%',
            'Ann Return': f'{self.annual_return:.2f}%',
            'Max DD': f'{self.max_drawdown:.2f}%',
            'Sharpe': f'{self.sharpe_ratio:.2f}',
            'Win Rate': f'{self.win_rate:.1f}%',
            'P/L Ratio': f'{self.profit_loss_ratio:.2f}',
            'Final Value': f'{self.final_value:.0f}',
            'Trades': len(self.trades),
            'Days': self.trading_days,
        }


class StockData:
    """预构建的numpy数组, O(1)访问"""
    __slots__ = ['dates', 'opens', 'highs', 'lows', 'closes', 'volumes']
    
    def __init__(self, dates, opens, highs, lows, closes, volumes):
        self.dates = dates  # list of str
        self.opens = opens  # np.array
        self.highs = highs
        self.lows = lows
        self.closes = closes
        self.volumes = volumes
    
    @property
    def length(self):
        return len(self.dates)
    
    def slice_before(self, date):
        """Return index of date (exclusive)"""
        for i, d in enumerate(self.dates):
            if d >= date:
                return i
        return len(self.dates)


class BacktestEngine:
    
    def __init__(self, db_path, config: BacktestConfig, progress_callback=None):
        self.db_path = db_path
        self.config = config
        self.progress_callback = progress_callback
    
    def _emit_progress(self, pct, msg):
        if self.progress_callback:
            self.progress_callback(pct, msg)
    
    def load_data(self):
        """加载数据, 构建numpy数组"""
        self._emit_progress(0, '加载数据...')
        
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT symbol, date, open, high, low, close, volume FROM daily_kline ORDER BY symbol, date')
        rows = c.fetchall()
        conn.close()
        
        # Group by symbol
        by_symbol = defaultdict(list)
        for row in rows:
            by_symbol[row[0]].append(row[1:])
        
        # Build numpy arrays
        stock_arrays = {}
        all_dates_set = set()
        
        for sym, data in by_symbol.items():
            dates = [r[0] for r in data]
            opens = np.array([float(r[1]) for r in data])
            highs = np.array([float(r[2]) for r in data])
            lows = np.array([float(r[3]) for r in data])
            closes = np.array([float(r[4]) for r in data])
            volumes = np.array([float(r[5]) for r in data])
            
            stock_arrays[sym] = StockData(dates, opens, highs, lows, closes, volumes)
            all_dates_set.update(dates)
        
        # Get sorted trading dates
        all_dates = sorted(all_dates_set)
        
        # Filter to backtest range
        start_idx = 0
        end_idx = len(all_dates) - 1
        
        for i, d in enumerate(all_dates):
            if d >= self.config.start_date:
                start_idx = i
                break
        
        for i in range(len(all_dates) - 1, -1, -1):
            if all_dates[i] <= self.config.end_date:
                end_idx = i
                break
        
        trading_dates = all_dates[start_idx:end_idx + 1]
        
        # Build date->market dict: {date: {symbol: (open,high,low,close,volume)}}
        date_market = {}
        for sym, sd in stock_arrays.items():
            for i, dt in enumerate(sd.dates):
                if dt not in date_market:
                    date_market[dt] = {}
                date_market[dt][sym] = (sd.opens[i], sd.highs[i], sd.lows[i], sd.closes[i], sd.volumes[i])
        
        self._emit_progress(5, f'加载完成: {len(stock_arrays)} 只股票, {len(trading_dates)} 个交易日')
        
        return stock_arrays, date_market, trading_dates
    
    def _check_l3(self, sd: StockData, date_idx):
        """L3风险过滤（与选股 ScreenerWorker 完全一致）

        选股:
          1. 数据量 ≥ 120 天
          2. 排除 ST
          3. 半年涨幅 ≤ hy%（120日）
          4. 筹码集中度 cc%: (收盘价/MA60 - 0.9) / 0.2 * 100
        """
        if date_idx < 120:
            return False

        closes = sd.closes

        # 半年涨幅(120个交易日)
        current = closes[date_idx]
        half_year_ago = closes[date_idx - 120]
        if half_year_ago > 0:
            ret = (current - half_year_ago) / half_year_ago * 100
            if ret > self.config.half_year_limit:
                return False

        # 筹码集中度: 与选股完全一致的计算方式
        ma60 = closes[date_idx - 59: date_idx + 1].mean()
        if ma60 > 0:
            cc = min(100, max(0, (current / ma60 - 0.9) / 0.2 * 100))
        else:
            cc = 0
        if cc < self.config.concentration:
            return False

        return True
    
    def _check_l2(self, sd: StockData, date_idx, symbol: str = ''):
        """L2突破+涨停（与选股 ScreenerWorker 完全一致）

        选股:
          1. 当日必须涨 (today > yesterday)
          2. 收盘价突破 N 日高点
          3. N日内有天触及涨停板
          4. MA5 >= MA10
        """
        nd = self.config.breakout_days
        if date_idx < nd + 1:
            return False

        highs = sd.highs
        closes = sd.closes

        # 1. 当日必须涨（收盘价 > 昨日收盘价）
        if date_idx > 0:
            today_close = closes[date_idx]
            yesterday_close = closes[date_idx - 1]
            if today_close <= yesterday_close:
                return False

        # 2. 突破 N 日高点（前 N 日，不含今天）
        if date_idx >= nd:
            window_high = np.max(highs[date_idx - nd: date_idx])  # 不含今天
            if today_close < window_high * self.config.breakout_threshold:
                return False

        # 3. N日内有涨停（与选股一致的判断逻辑）
        zd_n = self.config.limit_up_days
        if zd_n > 0 and date_idx >= zd_n:
            has_limit_up = False
            for i in range(date_idx, max(date_idx - zd_n, 0), -1):
                prev_close = closes[i - 1] if i > 0 else closes[i]
                if prev_close <= 0:
                    continue
                pct = (closes[i] - prev_close) / prev_close
                limit_pct = self._get_limit_pct(symbol)
                if pct >= limit_pct:
                    has_limit_up = True
                    break
            if not has_limit_up:
                return False

        # 4. MA5 >= MA10
        if date_idx >= 9:
            ma5 = closes[date_idx - 4: date_idx + 1].mean()
            ma10 = closes[date_idx - 9: date_idx + 1].mean()
            if ma5 < ma10:
                return False

        return True
    
    def _get_stock_list_with_dates(self, stock_arrays):
        """Get symbols that have data in the backtest range"""
        symbols = list(stock_arrays.keys())
        return symbols
    
    def _is_st(self, symbol):
        return 'ST' in symbol.upper() or '*ST' in symbol.upper()

    def _get_limit_pct(self, symbol):
        """根据股票代码判断涨停阈值"""
        sym = symbol.upper().replace('SHSE.', '').replace('SZSE.', '')
        if sym.startswith('300') or sym.startswith('688'):
            return 0.1995  # 创业板/科创板 20%
        if 'ST' in sym:
            return 0.0495  # ST 5%
        return 0.0995  # 主板 10%
    
    def _draw_candidate_kline(self, closes, highs, lows, volumes, save_path):
        """从 OHLC 数据画 K 线图 (与主选股 sim_calc_image 一致的渲染风格)"""
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        from matplotlib.patches import Rectangle

        n = len(closes)
        if n < 4:
            return False

        fig, ax = plt.subplots(figsize=(8, 3), dpi=100)
        fig.patch.set_facecolor('#1a1a2e')
        ax.set_facecolor('#1a1a2e')

        x = np.arange(n)
        w = 0.6

        for i in range(n):
            o = closes[i-1] if i > 0 else closes[i]
            c = closes[i]
            h = highs[i]
            l = lows[i]

            is_up = c >= o
            color = '#ff4444' if is_up else '#00cc44'

            body_top = max(o, c)
            body_bot = min(o, c)
            body_h = max(body_top - body_bot, 0.01)
            rect = Rectangle((x[i] - w/2, body_bot), w, body_h,
                            facecolor=color, edgecolor=color, linewidth=0.5)
            ax.add_patch(rect)
            ax.plot([x[i], x[i]], [l, h], color=color, linewidth=0.8)

        ax.set_xlim(-1, n)
        ymin, ymax = min(lows) * 0.99, max(highs) * 1.01
        ax.set_ylim(ymin, ymax)
        ax.set_xticks([])
        ax.set_yticks([])
        for sp in ax.spines.values():
            sp.set_visible(False)

        plt.tight_layout(pad=0)
        plt.savefig(save_path, dpi=100, facecolor='#1a1a2e', bbox_inches='tight', pad_inches=0)
        plt.close(fig)
        return True

    def _check_l1(self, sd, date_idx):
        """L1 K线相似度匹配（与选股 ScreenerWorker 完全一致）

        选股逻辑: 取最近40天收盘价 → 归一化 → sim_calc_image(参考图蜡烛Y, 归一化收盘价)
        回测必须用相同逻辑, 否则 L1 结果永远对不上。
        """
        if not self.config.ref_image:
            return True

        # ── 加载参考图（缓存一次）──
        if not hasattr(self, '_ref_candles'):
            ref_path = self.config.ref_image
            if not os.path.isabs(ref_path):
                ws = os.path.dirname(os.path.abspath(self.db_path))
                ref_path = os.path.join(ws, ref_path)
                if not os.path.exists(ref_path):
                    ref_path = os.path.join(os.getcwd(), self.config.ref_image)
            if not os.path.exists(ref_path):
                print(f"[L1] 参考图不存在: {ref_path}")
                return True
            self._ref_candles, msg = _extract_candle_centers(ref_path)
            if self._ref_candles is None:
                print(f"[L1] 参考图解析失败: {msg}")
                return True
            print(f"[L1] 参考图加载成功: {msg}")
            self._l1_total = 0
            self._l1_pass = 0

        # ── 取该股票 date_idx 前 N 天收盘价（与选股一致：默认 40 天）──
        n_seg = min(40, date_idx + 1)
        if n_seg < 10:
            return False

        seg = sd.closes[date_idx - n_seg + 1: date_idx + 1]
        sn, sx = seg.min(), seg.max()
        sr = sx - sn if sx != sn else 1e-6
        seg_n = np.array([(s - sn) / sr for s in seg])

        # ── 直接用 sim_calc_image 相同的算法（参考图蜡烛Y vs 归一化收盘价）──
        sim = _calc_similarity(self._ref_candles, seg_n)

        self._l1_total += 1
        passed = sim >= self.config.similarity_threshold
        if passed:
            self._l1_pass += 1

        return passed


    def run(self) -> BacktestResult:
        self._emit_progress(0, '开始回测...')
        
        stock_arrays, date_market, trading_dates = self.load_data()
        symbols = self._get_stock_list_with_dates(stock_arrays)
        
        result = BacktestResult()
        result.config = self.config
        
        cash = self.config.initial_capital
        positions: Dict[str, Position] = {}
        trades = []
        
        total_days = len(trading_dates)
        
        # Precompute symbol date indexes
        symbol_date_map = {}
        for sym, sd in stock_arrays.items():
            date_to_idx = {}
            for i, d in enumerate(sd.dates):
                date_to_idx[d] = i
            symbol_date_map[sym] = date_to_idx
        
        pending_buys: list[tuple[str, str]] = []  # (symbol, signal_date)
        
        for day_idx, date in enumerate(trading_dates):
            progress_pct = 10 + int(day_idx / total_days * 80)
            self._emit_progress(progress_pct, f'回测中: {date} ({day_idx+1}/{total_days})')
            
            today_market = date_market.get(date, {})
            
            # ===== 0. 执行前一日的买入信号（次日开盘买入）=====
            next_buys = [b for b in pending_buys if b[1] == date]
            pending_buys = [b for b in pending_buys if b[1] != date]
            
            available_slots = self.config.max_positions - len(positions)
            for sym, sig_date in next_buys[:available_slots]:
                if sym in positions:  # 已持有，跳过
                    continue
                market_data = today_market.get(sym)
                if not market_data:
                    continue
                open_price, high, low, close, vol = market_data
                if vol == 0:
                    continue
                
                buy_price = open_price * (1 + self.config.slippage)
                buy_amount = cash / self.config.max_positions
                shares = int(buy_amount / buy_price / 100) * 100
                if shares <= 0:
                    continue
                cost = buy_price * shares * (1 + self.config.commission)
                if cost > cash:
                    continue
                
                cash -= cost
                positions[sym] = Position(sym, buy_price, date, shares)
            
            # ===== 1. 选股（收盘后生成信号）=====
            signals = []
            
            for sym in symbols:
                if sym not in stock_arrays:
                    continue
                
                sd = stock_arrays[sym]
                
                # Get index for today
                date_idx = symbol_date_map.get(sym, {}).get(date)
                if date_idx is None:
                    continue  # Stock not trading today
                
                # L3: ST check
                if self.config.exclude_st and self._is_st(sym):
                    continue
                
                # L3: half year limit
                if not self._check_l3(sd, date_idx):
                    continue
                
                # L2: breakout + consecutive
                if not self._check_l2(sd, date_idx, sym):
                    continue
                
                # L1: K线相似度匹配（完整版，最耗时）
                if not self._check_l1(sd, date_idx):
                    continue
                
                signals.append(sym)
            
            # ===== 2. 将信号加入次日买入队列 =====
            next_day_date = trading_dates[day_idx + 1] if day_idx + 1 < len(trading_dates) else None
            if next_day_date:
                available_slots = self.config.max_positions - len(positions)
                for sym in signals[:available_slots]:
                    if sym in positions:
                        continue
                    # 不在已排队持仓中才加入
                    if any(b[0] == sym for b in pending_buys):
                        continue
                    pending_buys.append((sym, next_day_date))
            
            # ===== 3. 止损/止盈 =====
            to_remove = []
            
            for sym, pos in positions.items():
                market_data = today_market.get(sym)
                if not market_data:
                    continue
                
                open_p, high, low, close, vol = market_data
                current_price = close
                
                pnl = (current_price - pos.entry_price) / pos.entry_price
                
                if pnl <= -self.config.stop_loss:
                    sell_price = current_price * (1 - self.config.slippage)
                    revenue = sell_price * pos.shares * (1 - self.config.commission - self.config.stamp_tax)
                    cash += revenue
                    
                    trades.append(Trade(sym, pos.entry_date, pos.entry_price, date, sell_price, '止损'))
                    to_remove.append(sym)
                    continue
                
                if pnl >= self.config.take_profit:
                    sell_price = current_price * (1 - self.config.slippage)
                    revenue = sell_price * pos.shares * (1 - self.config.commission - self.config.stamp_tax)
                    cash += revenue
                    
                    trades.append(Trade(sym, pos.entry_date, pos.entry_price, date, sell_price, '止盈'))
                    to_remove.append(sym)
                    continue
            
            for sym in to_remove:
                del positions[sym]
            
            # ===== 4. 净值 =====
            market_value = 0
            for sym, pos in positions.items():
                market_data = today_market.get(sym)
                if market_data:
                    market_value += market_data[3] * pos.shares  # close price
            
            total_value = cash + market_value
            
            result.daily_records.append(DailyRecord(
                date=date,
                cash=cash,
                market_value=market_value,
                total_value=total_value,
                position_count=len(positions)
            ))
        
        # ===== 5. 平仓 =====
        for sym, pos in positions.items():
            market_data = today_market.get(sym)
            last_price = market_data[3] if market_data else pos.entry_price
            
            sell_price = last_price * (1 - self.config.slippage)
            revenue = sell_price * pos.shares * (1 - self.config.commission - self.config.stamp_tax)
            cash += revenue
            
            last_date = trading_dates[-1] if trading_dates else ''
            trades.append(Trade(sym, pos.entry_date, pos.entry_price, last_date, sell_price, '回测结束'))
        
        result.trades = trades
        
        self._emit_progress(95, '计算指标...')
        self._emit_progress(100, f'回测完成! 总收益{result.total_return:.1f}%')
        
        return result
