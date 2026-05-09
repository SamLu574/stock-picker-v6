# -*- coding: utf-8 -*-
"""A股形态选股系统 v6.0 - 完整修复版"""
import sys, os, time, json, requests, numpy as np
from datetime import datetime
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *

# PyInstaller 打包后 sys.stdout 可能为 None，需要保护
if sys.stdout:
    try: sys.stdout.reconfigure(encoding='utf-8')
    except: pass
if sys.stderr:
    try: sys.stderr.reconfigure(encoding='utf-8')
    except: pass

def on_crash(et, ev, tb):
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'crash.log'), 'w', encoding='utf-8') as f:
        import traceback; traceback.print_exception(et, ev, tb, file=f)
sys.excepthook = on_crash

# ── 颜色常量 ──
DARK  = '#0d1117'
CARD  = '#161b22'
CARD2 = '#1c2128'
WHITE = '#c9d1d9'
GREEN = '#3fb950'
YELL  = '#d29922'
RED   = '#f85149'
BLUE  = '#58a6ff'

def mk(t, c):
    it = QTableWidgetItem(str(t))
    it.setForeground(QColor(c))
    it.setTextAlignment(Qt.AlignCenter)
    return it

# ── K线图片解析 ──
def extract_candle_centers(image_path):
    import cv2
    img = cv2.imread(image_path)
    if img is None: return None, '无法读取图片'
    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
    lr1=np.array([0,30,50]); ur1=np.array([15,255,255])
    lr2=np.array([155,30,50]); ur2=np.array([180,255,255])
    lg=np.array([25,30,50]); ug=np.array([95,255,255])
    mr=cv2.add(cv2.inRange(hsv,lr1,ur1),cv2.inRange(hsv,lr2,ur2))
    mg=cv2.inRange(hsv,lg,ug)
    k=np.ones((3,3),np.uint8)
    mr=cv2.morphologyEx(mr,cv2.MORPH_CLOSE,k); mr=cv2.morphologyEx(mr,cv2.MORPH_OPEN,k)
    mg=cv2.morphologyEx(mg,cv2.MORPH_CLOSE,k); mg=cv2.morphologyEx(mg,cv2.MORPH_OPEN,k)
    cr,_=cv2.findContours(mr,cv2.RETR_EXTERNAL,cv2.CHAIN_APPROX_SIMPLE)
    cg,_=cv2.findContours(mg,cv2.RETR_EXTERNAL,cv2.CHAIN_APPROX_SIMPLE)
    candles=[]
    for cnt in cr+cg:
        if cv2.contourArea(cnt)>5:
            x,y,w,h=cv2.boundingRect(cnt)
            if w>0 and 0.1<h/w<10: candles.append((x+w/2,y+h/2))
    candles=sorted(candles,key=lambda c:c[0])
    if len(candles)<4: return None,f'只检测到{len(candles)}根K线'
    cy=np.array([c[1] for c in candles]); yn,yx=cy.min(),cy.max(); yr=yx-yn if yx!=yn else 1e-6
    return np.array([(yx-c[1])/yr for c in candles][-40:]),f'检测到{len(candles)}根K线'

# ── 相似度计算 ──
def sim_calc_image(ul,cl):
    from sklearn.metrics.pairwise import cosine_similarity
    from scipy.stats import pearsonr
    import numba
    @numba.jit(nopython=True)
    def fast_dtw(s1,s2):
        n,m=len(s1),len(s2)
        if n==0 or m==0: return 0
        dt=np.full((n+1,m+1),np.inf); dt[0,0]=0
        for i in range(1,n+1):
            for j in range(1,m+1): dt[i,j]=abs(s1[i-1]-s2[j-1])+min(dt[i-1,j],dt[i,j-1],dt[i-1,j-1])
        mpd=max(np.ptp(s1),np.ptp(s2))*max(n,m)
        return max(0,min(100,100*(1-dt[n,m]/mpd))) if mpd>0 else 100
    try:
        uf,cf=ul.flatten(),cl.flatten()
        cs=cosine_similarity(uf.reshape(1,-1),cf.reshape(1,-1))[0][0]
        try: pc,_=pearsonr(uf,cf); ps=((pc+1)/2)*100 if not np.isnan(pc) else 0
        except: ps=0
        ur,cr2=np.ptp(uf),np.ptp(cf); us,css=np.std(uf),np.std(cf)
        rs=100*(1-abs(ur-cr2)/max(ur,cr2)) if max(ur,cr2)>1e-6 else 100
        ss=100*(1-abs(us-css)/max(us,css)) if max(us,css)>1e-6 else 100
        return round(max(0,min(100,0.3*((cs+1)/2)*100+0.2*ps+0.25*(rs+ss)/2+0.25*fast_dtw(uf,cf))),1)
    except: return 0.0

# ── 飞书通知 ──
def send_feishu_msg(text):
    p = os.path.join(os.path.expanduser('~'), '.stepclaw', 'openclaw.json')
    try:
        with open(p, encoding='utf-8') as f: cfg = json.load(f)
        ch = cfg.get('channels',{}).get('feishu',{})
        app_id = ch.get('appId','')
        app_secret = ch.get('appSecret','')
    except: print('无法读取飞书配置'); return False
    if not app_id or not app_secret: print('飞书配置不完整'); return False
    owner = 'ou_12830d174689493c4ddde8dc2af808a9'
    try:
        r=requests.post('https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal',json={'app_id':app_id,'app_secret':app_secret})
        token=r.json().get('tenant_access_token','')
        if not token: return False
        url='https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=open_id'
        h={'Authorization':'Bearer '+token,'Content-Type':'application/json'}
        r2=requests.post(url,json={'receive_id':owner,'msg_type':'text','content':json.dumps({'text':text})},headers=h)
        return r2.json().get('code',-1)==0
    except Exception as e: print(f'飞书发送失败: {e}'); return False


# ══════════════════════════════════════════
#  选股 Worker
# ══════════════════════════════════════════
class ScreenerWorker(QThread):
    sig = pyqtSignal(dict)
    data = pyqtSignal(dict)
    fin  = pyqtSignal(list, dict, str)

    def __init__(self, conds):
        super().__init__()
        self.conds = conds

    def run(self):
        from gm.api import set_token
        set_token('ee777a1377ca29bafb01cd02b4bb276ea92fa3be')
        res, fc = [], {
            'total':0,'data_ok':0,'rising':0,
            'l3_pass':0,'l2_pass':0,'l1_pass':0,
            'l3_fail':{},'l2_fail':{},'l1_fail':{}
        }
        try:
            l1t   = float(self.conds.get('l1t', 70))
            do_st = self.conds.get('st', True)
            ref_img = self.conds.get('ref_img')
            if not ref_img or not os.path.exists(ref_img):
                self.sig.emit({'p':0,'m':'请先上传K线图片!'}); self.fin.emit([],fc,''); return

            self.sig.emit({'p':2,'m':'解析K线图片...'})
            user_line, msg = extract_candle_centers(ref_img)
            if user_line is None:
                self.sig.emit({'p':0,'m':f'图片解析失败: {msg}'}); self.fin.emit([],fc,''); return
            self.sig.emit({'p':3,'m':f'{msg}, 开始筛选...'})

            # 批量加载
            import sqlite3
            db  = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'stock_data.db')
            conn = sqlite3.connect(db); c = conn.cursor()
            c.execute('SELECT symbol, date, open, high, low, close FROM daily_kline ORDER BY symbol, date')
            rows = c.fetchall(); conn.close()
            from collections import defaultdict
            sd = defaultdict(list)
            for sym, dt, op, hi, lo, cl in rows:
                sd[sym].append((dt, float(op), float(hi), float(lo), float(cl)))
            syms = sorted(sd.keys())
            fc['total'] = len(syms)
            self.sig.emit({'p':1,'m':f'共 {len(syms)} 只'})

            # L3
            a3 = []
            for i, sym in enumerate(syms):
                if i % 200 == 0:
                    self.sig.emit({'p':5+i*35//len(syms),'m':f'L3: {i}/{len(syms)}'})
                r = sd[sym]; cl = [x[4] for x in r]
                if len(cl) < 120: continue
                fc['data_ok'] += 1
                if do_st and ('ST' in sym or '*ST' in sym):
                    fc['l3_fail']['ST'] = fc['l3_fail'].get('ST',0)+1; continue
                hy = (cl[-1]-cl[-120])/cl[-120]*100
                if hy > float(self.conds.get('hy',30)):
                    fc['l3_fail']['半年涨幅超限'] = fc['l3_fail'].get('半年涨幅超限',0)+1; continue
                ma60 = sum(cl[-60:])/60.0
                cc = min(100,max(0,(cl[-1]/ma60-0.9)/0.2*100)) if ma60>0 else 0
                if cc < float(self.conds.get('cc',75)):
                    fc['l3_fail']['筹码集中度不足'] = fc['l3_fail'].get('筹码集中度不足',0)+1; continue
                fc['l3_pass'] += 1
                a3.append((sym, r, cl))
            self.data.emit({'layer':'l3','count':fc['l3_pass']})
            self.sig.emit({'p':40,'m':f'L3通过: {fc["l3_pass"]} 只'})

            # L2
            a2 = []
            for sym, r, cl in a3:
                op = [x[1] for x in r]; hi = [x[2] for x in r]
                pn, p2 = cl[-1], cl[-2]
                tp = (pn-p2)/p2*100 if p2>0 else 0
                if tp < 0: continue
                fc['rising'] += 1
                nd = min(int(self.conds.get('bh_d',45)), len(hi))
                hn = max(hi[-nd:])
                if pn < hn * float(self.conds.get('bh_t',0.98)):
                    fc['l2_fail']['未突破N日高'] = fc['l2_fail'].get('未突破N日高',0)+1; continue
                # N日内有涨停
                zd_n = int(self.conds.get('zd_n',5))
                has_limit_up = False
                for j in range(len(cl)-1, max(len(cl)-zd_n,0), -1):
                    prev = cl[j-1] if j > 0 else cl[j]
                    if prev <= 0: continue
                    pct = (cl[j] - prev) / prev
                    # 根据代码判断涨停阈值
                    sym_clean = sym.upper()
                    if sym_clean.startswith('SZSE.300') or sym_clean.startswith('SHSE.688'):
                        limit_pct = 0.1995  # 创业板/科创板 20%
                    elif sym_clean.startswith('SZSE.') or sym_clean.startswith('SHSE.'):
                        if sym_clean.endswith('*ST') or 'ST' in sym_clean:
                            limit_pct = 0.0495  # ST 5%
                        else:
                            limit_pct = 0.0995  # 主板 10%
                    else:
                        limit_pct = 0.0995
                    if pct >= limit_pct:
                        has_limit_up = True
                        break
                if not has_limit_up:
                    fc['l2_fail']['N日内无涨停'] = fc['l2_fail'].get('N日内无涨停',0)+1; continue
                ma5 = sum(cl[-5:])/5.0; ma10 = sum(cl[-10:])/10.0
                if ma5 < ma10:
                    fc['l2_fail']['MA5<MA10'] = fc['l2_fail'].get('MA5<MA10',0)+1; continue
                fc['l2_pass'] += 1
                a2.append((sym, r, cl, tp))
            self.data.emit({'layer':'l2','count':fc['l2_pass']})
            self.sig.emit({'p':60,'m':f'L2通过: {fc["l2_pass"]} 只'})

            # L1
            l1 = []
            for sym, r, cl, tp in a2:
                if len(r) >= 40:
                    seg = [x[4] for x in r[-40:]]
                    sn, sx = min(seg), max(seg); sr = sx-sn if sx!=sn else 1e-6
                    seg_n = np.array([(s-sn)/sr for s in seg])
                    sim = sim_calc_image(user_line, seg_n)
                    if sim < l1t:
                        fc['l1_fail'][f'相似度<{l1t:.0f}%'] = fc['l1_fail'].get(f'相似度<{l1t:.0f}%',0)+1; continue
                else: sim = 0
                fc['l1_pass'] += 1
                l1.append({
                    'code':sym, 'sim':round(sim,1), 'price':round(cl[-1],2),
                    'today':round(tp,1),
                    'g60':round((cl[-1]-cl[-60])/cl[-60]*100,1) if len(cl)>=60 else 0,
                    'g120':round((cl[-1]-cl[-120])/cl[-120]*100,1) if len(cl)>=120 else 0
                })
            l1.sort(key=lambda x: x['sim'], reverse=True)
            self.data.emit({'layer':'l1','stocks':list(l1),'total':len(l1)})
            self.sig.emit({'p':95,'m':f'最终: {len(l1)} 只'})
            res = l1
        except Exception as e:
            self.sig.emit({'p':0,'m':'错误: '+str(e)})
        self.fin.emit(res, fc, str(datetime.now())[:19])


# ══════════════════════════════════════════
#  监控系统 Worker
# ══════════════════════════════════════════
class MonitorWorker(QThread):
    log  = pyqtSignal(str)      # 日志行
    alert= pyqtSignal(str,str)  # 飞书通知: code, msg
    position_update = pyqtSignal(list)  # 持仓更新: [{code, clean, price, date, current, pnl}]
    def __init__(self):
        super().__init__()
        self.watchlist  = []
        self.triggered  = set()  # 已通知的 (code, date)
        self.running    = False
        # 监控条件
        self.breakout_days = 60   # 股价突破N日高点
        self.stop_loss = 5.0      # 亏损止损%
        self.take_profit = 20.0   # 盈利止盈%
        self.entry_prices = {}    # 持仓: code -> (price, date)
        self.positions = []       # 持仓列表 [{code, clean, price, date, ...}]
        self.poll_count = 0       # 轮询计数
        self.poll_count = 0       # 轮询计数

    def set_conditions(self, breakout_days=None, stop_loss=None, take_profit=None):
        if breakout_days is not None: self.breakout_days = breakout_days
        if stop_loss is not None: self.stop_loss = stop_loss
        if take_profit is not None: self.take_profit = take_profit
        self.log.emit(f'🔴 买入条件: 突破{self.breakout_days}日高点')
        self.log.emit(f'🔵 卖出条件: 亏损>{self.stop_loss}% / 盈利>{self.take_profit}%')

    def load_positions(self, pos_list):
        """加载持仓数据"""
        self.entry_prices = {}
        self.positions = []
        for p in pos_list:
            code = p.get('code','')
            price = float(p.get('price',0))
            date = p.get('date','')
            self.entry_prices[code] = (price, date)
            self.positions.append({'code':code, 'price':price, 'date':date})

    def add_watchlist(self, codes):
        """添加监控股票（代码列表，带 SHSE./SZSE. 前缀）"""
        self.watchlist = list(set(self.watchlist + codes))
        self.log.emit(f'📡 监控池: {len(self.watchlist)} 只')

    def clear_watchlist(self):
        self.watchlist.clear(); self.triggered.clear()
        self.log.emit('📡 监控池已清空')

    def start_mon(self):
        self.running = True; self.log.emit('📡 监控启动')

    def stop_mon(self):
        self.running = False; self.log.emit('📡 监控停止')

    def run(self):
        """监控循环：用 efinance 获取实时行情"""
        import sys
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from data_layer import _efinance_get_realtime_quotes, get_kline
        
        today = str(datetime.now().date())
        while self.running:
            if not self.watchlist:
                time.sleep(10); continue
            try:
                # 用 efinance 获取实时行情
                quotes = _efinance_get_realtime_quotes(self.watchlist)
                if not quotes:
                    self.log.emit('⏸ 非交易时段或网络异常, 轮询暂停...'); time.sleep(60); continue
                
                self.poll_count += 1
                self.log.emit(f'📡 第{self.poll_count}次轮询 · {len(quotes)} 只股票有数据')

                # 构建轮询数据：所有股票 + 实时价格
                pool_data = []
                for code, q in quotes.items():
                    clean = code.replace('SHSE.','').replace('SZSE.','')
                    price = q.get('price', 0)
                    if price <= 0: continue

                    key = f'{code}:{today}'
                    entry_key = code
                    nd = self.breakout_days

                    status = '⏳ 待触发'
                    n_high = 0

                    # 检查已持仓股票：止损/止盈
                    if entry_key in self.entry_prices:
                        entry_price, entry_date = self.entry_prices[entry_key]
                        pnl_pct = (price - entry_price) / entry_price * 100
                        # 止损
                        if pnl_pct <= -self.stop_loss:
                            del self.entry_prices[entry_key]
                            self.triggered.add(key)
                            msg = f'📤 {clean} 触发止损卖出! 买入价{entry_price:.2f} 现价{price:.2f} (亏损{abs(pnl_pct):.1f}%)'
                            self.log.emit(msg)
                            self.alert.emit(clean, msg)
                            # 从持仓中移除，恢复为待触发
                            for i, pos in enumerate(self.positions):
                                if pos.get('code') == code:
                                    self.positions.pop(i); break
                            status = '⏳ 待触发'
                        # 止盈
                        elif pnl_pct >= self.take_profit:
                            del self.entry_prices[entry_key]
                            self.triggered.add(key)
                            msg = f'📤 {clean} 触发止盈卖出! 买入价{entry_price:.2f} 现价{price:.2f} (盈利{pnl_pct:.1f}%)'
                            self.log.emit(msg)
                            self.alert.emit(clean, msg)
                            for i, pos in enumerate(self.positions):
                                if pos.get('code') == code:
                                    self.positions.pop(i); break
                            status = '⏳ 待触发'
                        else:
                            status = f' 已买入'
                        # 更新持仓现价
                        for pos in self.positions:
                            if pos.get('code') == code:
                                pos['current'] = price
                                pos['pnl'] = round(pnl_pct, 1)
                    else:
                        # 未持仓：检测买入条件（突破N日高点）
                        try:
                            # 从本地数据库读前 N 日历史数据
                            hist_df = get_kline(code, start_date=None, end_date=today)
                            if hist_df is not None and len(hist_df) > nd:
                                # 取前 N 日最高价（不含今天）
                                window = hist_df.tail(nd + 1).head(nd)
                                n_high = window['high'].max()
                                
                                if price > n_high:
                                    self.entry_prices[entry_key] = (price, today)
                                    self.positions.append({'code':code, 'clean':clean, 'price':price, 'date':today, 'current':price, 'pnl':0.0})
                                    self.log.emit(f'📥 {clean} 触发买入! 价格{price:.2f} (突破{nd}日高点{n_high:.2f})')
                                    msg = f'📥 {clean} 触发买入! 价格{price:.2f}'
                                    self.alert.emit(clean, msg)
                                    status = '🔴 已买入'
                                else:
                                    status = f'⏳ 待触发 (60日高{n_high:.2f})'
                        except Exception as e:
                            self.log.emit(f'⚠ {clean} 突破检测异常: {e}')

                    entry_info = None
                    if entry_key in self.entry_prices:
                        ep, ed = self.entry_prices[entry_key]
                        pnl_v = round((price - ep) / ep * 100, 1)
                        entry_info = {'entry_price': ep, 'pnl': pnl_v}

                    pool_data.append({
                        'code': code, 'clean': clean, 'price': price, 'status': status,
                        'is_bought': entry_key in self.entry_prices, 'n_high': n_high,
                        'entry_price': entry_info['entry_price'] if entry_info else None,
                        'pnl': entry_info['pnl'] if entry_info else 0
                    })

                # 更新持仓表（所有股票）
                self.position_update.emit(pool_data)
            except Exception as e:
                self.log.emit(f'⚠ 监控异常: {e}')
            time.sleep(180)  # 3分钟轮询


# ══════════════════════════════════════════
#  主窗口
# ══════════════════════════════════════════
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'stock_data.db')

class MainWin(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('📊 A股形态选股系统 v6.0')
        self.ref_image_path = None

        pal = QPalette()
        pal.setColor(QPalette.Window, QColor(DARK))
        pal.setColor(QPalette.WindowText, QColor(WHITE))
        pal.setColor(QPalette.Base, QColor(DARK))
        pal.setColor(QPalette.AlternateBase, QColor(CARD2))
        pal.setColor(QPalette.Text, QColor(WHITE))
        pal.setColor(QPalette.Button, QColor(CARD2))
        pal.setColor(QPalette.ButtonText, QColor(WHITE))
        pal.setColor(QPalette.Highlight, QColor(BLUE))
        self.setPalette(pal)

        # 全局字体统一设置为 14px
        font = QFont('Microsoft YaHei', 14)
        self.setFont(font)
        QApplication.setFont(font)

        cw = QWidget(); self.setCentralWidget(cw)
        main_h = QHBoxLayout(cw); main_h.setContentsMargins(8,8,8,8); main_h.setSpacing(8)

        # ━━━━━━━━━ 左侧 ━━━━━━━━━
        left = QWidget()
        left.setFixedWidth(520)
        left.setStyleSheet(f'background:{CARD};border-radius:8px')
        lv = QVBoxLayout(left); lv.setContentsMargins(14,14,14,14); lv.setSpacing(8)

        # 选股条件标题
        t = QLabel('▶ 选股条件')
        t.setStyleSheet(f'color:{BLUE};font-size:18px;font-weight:bold;padding-bottom:4px')
        lv.addWidget(t)

        # 数据库状态（最上面）
        card_db = self._card(lv, '📦 数据库状态')
        self.db_info = QLabel('正在读取...')
        self.db_info.setStyleSheet(f'color:#9399b2;font-size:17px'); card_db.addWidget(self.db_info)
        QTimer.singleShot(300, self._refresh_db)

        # 更新数据按钮（数据库状态下方）
        self.update_btn = QPushButton('🔄  更新数据')
        self.update_btn.setFixedHeight(36)
        self.update_btn.setFont(QFont('Microsoft YaHei', 14, QFont.Bold))
        self.update_btn.setStyleSheet(
            f'QPushButton{{background:{BLUE};color:{DARK};border:none;border-radius:6px;padding:6px;font-size:19px;font-weight:bold}}'
            f'QPushButton:hover{{background:#56d364}}')
        self.update_btn.clicked.connect(self._update_db); card_db.addWidget(self.update_btn)

        # L3 卡
        c3 = self._card(lv, 'L3  风险过滤')
        self.st  = QCheckBox('排除 ST'); self.st.setChecked(True)
        self.st.setStyleSheet(f'color:#bac2de;font-size:17px'); c3.addWidget(self.st)
        self.hy  = self._spin(c3, '半年涨幅上限 (%)', 110, 10, 200, 10)
        self.cc  = self._spin(c3, '筹码集中度 (%)', 75, 50, 95, 5)

        # L2 卡
        c2 = self._card(lv, 'L2  突破涨停')
        self.bh_d = self._spin(c2, '突破 N 日高点', 45, 10, 250, 5)
        self.bh_t = self._dspin(c2, '突破阈值', 0.98, 0.80, 1.00, 0.01)
        self.zd_n = self._spin(c2, 'N日内涨停', 5, 1, 20, 1)

        # L1 卡
        c1 = self._card(lv, 'L1  K线形态相似度')
        self.img_preview = QLabel()
        self.img_preview.setAlignment(Qt.AlignCenter)
        self.img_preview.setMinimumHeight(100); self.img_preview.setMaximumHeight(140)
        self.img_preview.setStyleSheet(
            'background:#1e1e2e;border:2px dashed #45475a;border-radius:6px;'
            f'color:#8b949e;font-size:17px')
        self.img_preview.setText('点击上传K线图片'); c1.addWidget(self.img_preview)
        self.upload_btn = QPushButton('📂  上传K线图')
        self.upload_btn.setStyleSheet(
            f'QPushButton{{background:{BLUE};color:{DARK};border:none;border-radius:6px;padding:6px;font-size:17px;font-weight:bold}}'
            f'QPushButton:hover{{background:#79c0ff}}')
        self.upload_btn.clicked.connect(self._upload_image); c1.addWidget(self.upload_btn)
        self.img_label = QLabel('未上传')
        self.img_label.setStyleSheet(f'color:#8b949e;font-size:17px'); c1.addWidget(self.img_label)
        self.l1t = self._spin(c1, '相似度阈值 (%)', 73, 0, 100, 5)

        # 执行按钮
        self.run_btn = QPushButton('▶  执行选股')
        self.run_btn.setFixedHeight(40)
        self.run_btn.setFont(QFont('Microsoft YaHei', 15, QFont.Bold))
        self.run_btn.setStyleSheet(
            f'QPushButton{{background:{BLUE};color:{DARK};border:none;border-radius:6px;padding:6px;font-size:17px;font-weight:bold}}'
            f'QPushButton:hover{{background:#79c0ff}}'
            f'QPushButton:disabled{{background:#484f58;color:#8b949e}}')
        self.run_btn.clicked.connect(self._run); lv.addWidget(self.run_btn)

        # 进度
        self.pbar = QProgressBar()
        self.pbar.setStyleSheet(
            f'QProgressBar{{background:#1e1e2e;border:1px solid #45475a;border-radius:4px;height:16px;text-align:center;color:#cdd6f4;font-size:15px}}'
            f'QProgressBar::chunk{{background:{BLUE};border-radius:4px}}')
        self.pmsg = QLabel(''); self.pmsg.setStyleSheet(f'color:#8b949e;font-size:17px')
        lv.addWidget(self.pmsg); lv.addWidget(self.pbar)

        # 漏斗
        self.l3c = QLabel('L3 风险过滤: 0'); self.l3c.setStyleSheet(f'color:{BLUE};font-size:17px;font-weight:bold'); lv.addWidget(self.l3c)
        self.l2c = QLabel('L2 突破连阳: 0'); self.l2c.setStyleSheet(f'color:{BLUE};font-size:17px;font-weight:bold'); lv.addWidget(self.l2c)
        self.l1c = QLabel('L1 K线相似度: 0'); self.l1c.setStyleSheet(f'color:{YELL};font-size:17px;font-weight:bold'); lv.addWidget(self.l1c)

        # 结果表
        res_lbl = QLabel('📋 选股结果')
        res_lbl.setStyleSheet(f'color:{BLUE};font-size:19px;font-weight:bold')
        lv.addWidget(res_lbl)
        self.result_tbl = QTableWidget(0, 4)
        self.result_tbl.setHorizontalHeaderLabels(['代码','相似度%','现价','今日%'])
        for i,w in enumerate([80,80,80,80]):
            self.result_tbl.setColumnWidth(i, w)
        self.result_tbl.horizontalHeader().setSectionResizeMode(QHeaderView.Fixed)
        self.result_tbl.setSelectionBehavior(QTableWidget.SelectRows)
        self.result_tbl.verticalHeader().setVisible(False)
        self.result_tbl.setStyleSheet(
            f'QTableWidget{{background:{DARK};color:#cdd6f4;gridline-color:#313244;font-size:17px;border:none}}'
            f'QHeaderView::section{{background:{CARD2};color:#74c7ec;padding:4px;font-size:17px;font-weight:bold;border:none}}'
            f'QTableWidget::item{{padding:4px}}')
        lv.addWidget(self.result_tbl, 1)

        main_h.addWidget(left, 0)

        # ━━━━━━━━━ 右侧 ━━━━━━━━━
        right = QWidget()
        right.setStyleSheet(f'background:{CARD};border-radius:8px')
        rh = QHBoxLayout(right); rh.setContentsMargins(8,8,8,8); rh.setSpacing(8)

        # 监控系统
        mon_frame = QFrame()
        mon_frame.setStyleSheet(f'background:{CARD2};border-radius:6px;border:1px solid #30363d')
        mon_v = QVBoxLayout(mon_frame); mon_v.setContentsMargins(10,8,10,8); mon_v.setSpacing(6)
        mt = QLabel('📡 监控系统'); mt.setStyleSheet('color:#74c7ec;font-size:19px;font-weight:bold'); mon_v.addWidget(mt)

        # 买入条件模块
        mon_buy = self._card(mon_v, '🔴 买入条件', RED)
        self.mon_breakout = self._spin(mon_buy, '股价突破 N 日高点', 60, 5, 250, 5)

        # 卖出条件模块
        mon_sell = self._card(mon_v, '🔵 卖出条件', BLUE)
        self.mon_stop_loss = self._dspin(mon_sell, '亏损止损 (%)', 5.0, 1.0, 50.0, 1.0)
        self.mon_take_profit = self._dspin(mon_sell, '盈利止盈 (%)', 20.0, 1.0, 100.0, 1.0)

        # 监控控制
        mc = QHBoxLayout()
        self.mon_start_btn = QPushButton('▶ 启动')
        self.mon_start_btn.setFixedHeight(28)
        self.mon_start_btn.setStyleSheet(
            f'QPushButton{{background:{BLUE};color:{DARK};border:none;border-radius:4px;padding:4px;font-size:15px}}')
        self.mon_start_btn.clicked.connect(self._mon_toggle); mc.addWidget(self.mon_start_btn)
        mon_v.addLayout(mc)

        self.mon_status = QLabel('⏹ 未启动 · 监控池: 0 只')
        self.mon_status.setStyleSheet(f'color:#9399b2;font-size:19px'); mon_v.addWidget(self.mon_status)

        # 持仓表
        self.pos_tbl = QTableWidget(0, 5)
        self.pos_tbl.setHorizontalHeaderLabels(['代码','现价','60日高','差距','状态'])
        self.pos_tbl.verticalHeader().setVisible(False)
        self.pos_tbl.setColumnWidth(0, 56)
        self.pos_tbl.setColumnWidth(1, 48)
        self.pos_tbl.setColumnWidth(2, 56)
        self.pos_tbl.setColumnWidth(3, 50)
        self.pos_tbl.setColumnWidth(4, 72)
        self.pos_tbl.horizontalHeader().setStretchLastSection(True)
        self.pos_tbl.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.pos_tbl.verticalHeader().setDefaultSectionSize(30)
        self.pos_tbl.setMinimumHeight(235)
        self.pos_tbl.setStyleSheet(
            f'QTableWidget{{background:#0d1117;color:#cdd6f4;gridline-color:#30363d;font-size:15px;border:none}}'
            f'QHeaderView::section{{background:#21262d;color:#74c7ec;padding:3px;font-size:15px;font-weight:bold;border:none}}'
            f'QTableWidget::item{{padding:2px}}')
        mon_v.addWidget(self.pos_tbl, 2)

        self.mon_log = QTextEdit()
        self.mon_log.setReadOnly(True)
        self.mon_log.setStyleSheet(f'background:#0d1117;color:#cdd6f4;border:1px solid #30363d;border-radius:4px;font-size:19px;font-family:Consolas')
        self.mon_log.setMaximumHeight(150)
        mon_v.addWidget(self.mon_log, 1)

        rh.addWidget(mon_frame, 1)

        # 回测系统
        from backtest_tab import BacktestTab
        self.backtest_tab = BacktestTab(DB_PATH, self._get_bt_params)
        rh.addWidget(self.backtest_tab, 1)

        main_h.addWidget(right, 1)
        main_h.addStretch()

        # ━━━━━━━━━ 监控 Worker ━━━━━━━━━
        self.monitor = MonitorWorker()
        self.monitor.log.connect(self._mon_log_append)
        self.monitor.alert.connect(self._mon_alert)
        self.monitor.position_update.connect(self._mon_pos_update)

        # 启动时读取上次选股结果
        QTimer.singleShot(500, self._load_monitor_pool)

    def _get_bt_params(self):
        """读取左侧选股条件（供回测引擎使用）"""
        ref_img_path = None
        if hasattr(self, 'ref_image_path') and self.ref_image_path:
            ref_img_path = self.ref_image_path
        print(f"[DEBUG] _get_bt_params ref_image_path = {ref_img_path!r}")
        import os
        if ref_img_path:
            print(f"[DEBUG] file exists = {os.path.exists(ref_img_path)}")
        return {
            'st': self.st.isChecked(),
            'hy': self.hy.value(),
            'cc': self.cc.value(),
            'bh_d': self.bh_d.value(),
            'bh_t': self.bh_t.value(),
            'zd_n': self.zd_n.value(),
            'l1t': self.l1t.value(),
            'ref_img': ref_img_path,
        }

    # ── 辅助 ──
    def _load_monitor_pool(self):
        """启动时加载上次选股结果到监控池"""
        pool_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'monitor_pool.json')
        if os.path.exists(pool_path):
            try:
                with open(pool_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                codes = data.get('codes', [])
                stocks = data.get('stocks', [])
                ts = data.get('time', '')
                if codes:
                    self._last_codes = codes
                    self.monitor.watchlist = list(codes)
                    self.mon_status.setText(f'⏹ 未启动 · 监控池: {len(codes)} 只')
                    # 如果选股结果存在，也显示到结果表
                    if stocks:
                        self.result_tbl.setRowCount(len(stocks))
                        for i, row in enumerate(stocks):
                            code = row.get('code','').replace('SHSE.','').replace('SZSE.','')
                            self.result_tbl.setItem(i, 0, mk(code, WHITE))
                            self.result_tbl.setItem(i, 1, mk(row.get('sim',''), BLUE))
                            self.result_tbl.setItem(i, 2, mk(row.get('price',''), WHITE))
                            tp = row.get('today',0)
                            self.result_tbl.setItem(i, 3, mk(tp, RED if tp >= 0 else GREEN))
                    self.monitor.log.emit(f'📂 已加载上次选股结果: {ts} · {len(codes)} 只')
            except Exception as e:
                self.monitor.log.emit(f'⚠ 加载监控池失败: {e}')
    def _card(self, parent, title, color='#74c7ec'):
        f = QFrame()
        f.setStyleSheet(f'background:{CARD2};border-radius:6px;border:1px solid #30363d')
        v = QVBoxLayout(f); v.setContentsMargins(10,6,10,6); v.setSpacing(4)
        t = QLabel(title); t.setStyleSheet(f'color:{color};font-size:17px;font-weight:bold'); v.addWidget(t)
        parent.addWidget(f)
        return v

    def _spin(self, p, label, val, mn, mx, step):
        r = QHBoxLayout()
        lb = QLabel(label); lb.setStyleSheet(f'color:#9399b2;font-size:19px')
        sp = QSpinBox(); sp.setRange(mn,mx); sp.setSingleStep(step); sp.setValue(val)
        sp.setStyleSheet(f'background:#1e1e2e;color:#cdd6f4;border:1px solid #45475a;border-radius:4px;padding:3px;font-size:19px')
        sp.setFixedHeight(26); r.addWidget(lb); r.addWidget(sp); p.addLayout(r); return sp

    def _dspin(self, p, label, val, mn, mx, step):
        r = QHBoxLayout()
        lb = QLabel(label); lb.setStyleSheet(f'color:#9399b2;font-size:19px')
        sp = QDoubleSpinBox(); sp.setRange(mn,mx); sp.setSingleStep(step); sp.setDecimals(2); sp.setValue(val)
        sp.setStyleSheet(f'background:#1e1e2e;color:#cdd6f4;border:1px solid #45475a;border-radius:4px;padding:3px;font-size:19px')
        sp.setFixedHeight(26); r.addWidget(lb); r.addWidget(sp); p.addLayout(r); return sp

    # ── 数据库状态 ──
    def _refresh_db(self):
        try:
            import sqlite3
            conn = sqlite3.connect(DB_PATH); c = conn.cursor()
            c.execute('SELECT COUNT(DISTINCT symbol), MAX(date) FROM daily_kline')
            cnt, last = c.fetchone(); conn.close()
            import os as _os
            size_mb = _os.path.getsize(DB_PATH) / 1024 / 1024
            self.db_info.setText(f'{cnt} 只股票 | 最新: {last} | 大小: {size_mb:.1f} MB')
        except Exception as e:
            self.db_info.setText(f'读取失败: {e}')

    # ── 更新数据 ──
    def _update_db(self):
        self.update_btn.setEnabled(False)
        self.update_btn.setText('⏳ 更新中...')
        self.db_info.setText('正在增量更新... (0/0)')

        def do_update(callback):
            try:
                import data_layer as dl
                # 进度回调：从子线程通过 QTimer 安全更新 UI
                def on_progress(cur, tot):
                    QTimer.singleShot(0, lambda: self.db_info.setText(f'正在增量更新... ({cur}/{tot})'))
                dl.incremental_update(progress_callback=on_progress, use_efinance=True)
                callback(True, '')
            except Exception as e:
                import traceback
                callback(False, traceback.format_exc())

        import threading
        threading.Thread(target=do_update, args=(self._after_update,), daemon=True).start()

    def _after_update(self, ok, err=''):
        self.update_btn.setEnabled(True); self.update_btn.setText('🔄  更新数据')
        if ok:
            self._refresh_db()
        else:
            self.db_info.setText(f'更新失败: {err}')
            QMessageBox.warning(self, '更新失败', f'增量更新失败:\n{err}')

    # ── 条件 ──
    def _get_conds(self):
        return {
            'l1t': self.l1t.value(), 'bh_d': self.bh_d.value(),
            'bh_t': self.bh_t.value(), 'zd_n': self.zd_n.value(),
            'st': self.st.isChecked(), 'hy': self.hy.value(), 'cc': self.cc.value(),
            'ref_img': self.ref_image_path
        }

    # ── 上传图片（自动复制到workspace，确保路径不丢失）──
    def _upload_image(self):
        src, _ = QFileDialog.getOpenFileName(self, '选择K线图片', '', '图片 (*.png *.jpg *.jpeg)')
        if not src: return
        import shutil
        ws = os.path.dirname(os.path.abspath(__file__))
        dst = os.path.join(ws, 'ref_kline.png')
        shutil.copy2(src, dst)
        self.ref_image_path = dst
        self.img_label.setText(os.path.basename(dst))
        pix = QPixmap(dst)
        self.img_preview.setPixmap(pix.scaled(240,100,Qt.KeepAspectRatio,Qt.SmoothTransformation))
        self.img_preview.setText('')

    # ── 执行选股 ──
    def _run(self):
        if not self.ref_image_path:
            QMessageBox.warning(self, '提示', '请先上传K线图片!'); return
        self.run_btn.setEnabled(False); self.run_btn.setText('⏳ 选股中...')
        self.pbar.setValue(0); self.pmsg.setText('正在解析图片...')
        self.result_tbl.setRowCount(0)
        self._last_codes = []
        self.worker = ScreenerWorker(self._get_conds())
        self.worker.sig.connect(self._on_prog)
        self.worker.data.connect(self._on_data)
        self.worker.fin.connect(self._on_done)
        self.worker.start()

    def _on_prog(self, d):
        self.pmsg.setText(d.get('m','')); self.pbar.setValue(d.get('p',0))

    def _on_data(self, d):
        layer = d.get('layer'); count = d.get('count',0)
        if layer == 'l3': self.l3c.setText(f'L3 风险过滤: {count}')
        elif layer == 'l2': self.l2c.setText(f'L2 突破连阳: {count}')

    def _on_done(self, res, fc, ts):
        self.run_btn.setEnabled(True); self.run_btn.setText('▶  执行选股')
        self.pbar.setValue(100); self.pmsg.setText(f'完成: {ts}')
        if not res:
            self.l1c.setText('L1 K线相似度: 0')
            self.result_tbl.setRowCount(1)
            it = QTableWidgetItem('未找到符合条件的股票')
            it.setTextAlignment(Qt.AlignCenter); self.result_tbl.setItem(0,0,it)
            return
        self.l1c.setText(f'L1 K线相似度: {len(res)}')
        self._last_codes = [x['code'] for x in res]
        # 自动保存到监控池
        pool_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'monitor_pool.json')
        try:
            with open(pool_path, 'w', encoding='utf-8') as f:
                json.dump({'codes': self._last_codes, 'stocks': res, 'time': ts}, f, ensure_ascii=False, indent=2)
        except: pass
        # 同步更新监控池
        self.monitor.watchlist = list(self._last_codes)
        self.monitor.triggered.clear()
        self.mon_status.setText(f'⏹ 未启动 · 监控池: {len(self._last_codes)} 只')
        self.result_tbl.setRowCount(len(res))
        for i, row in enumerate(res):
            code = row['code'].replace('SHSE.','').replace('SZSE.','')
            self.result_tbl.setItem(i, 0, mk(code, WHITE))
            self.result_tbl.setItem(i, 1, mk(row.get('sim',''), BLUE))
            self.result_tbl.setItem(i, 2, mk(row.get('price',''), WHITE))
            tp = row.get('today',0)
            self.result_tbl.setItem(i, 3, mk(tp, RED if tp >= 0 else GREEN))
        self.result_tbl.scrollToTop()

        # 漏斗
        print('='*40)
        print(f'选股完成: {ts}')
        print(f'股票总数: {fc.get("total",0)}')
        print(f'有数据: {fc.get("data_ok",0)}')
        print(f'上涨: {fc.get("rising",0)}')
        print(f'L3通过: {fc.get("l3_pass",0)}  淘汰: {fc.get("l3_fail",{})}')
        print(f'L2通过: {fc.get("l2_pass",0)}  淘汰: {fc.get("l2_fail",{})}')
        print(f'L1通过: {fc.get("l1_pass",0)}  淘汰: {fc.get("l1_fail",{})}')
        print('='*40)

    # ── 监控系统 ──
    def _mon_toggle(self):
        if self.monitor.running:
            self.monitor.running = False
            self.mon_start_btn.setText('▶ 启动')
            self.mon_status.setText(f'⏹ 已停止 · 监控池: {len(self.monitor.watchlist)} 只')
            return
        # 启动
        if not self.monitor.watchlist:
            QMessageBox.information(self, '提示', '请先执行选股以生成监控池')
            return
        # 强制重新创建 worker（避免旧线程卡在异常循环）
        old = self.monitor
        old.running = False
        self.monitor = MonitorWorker()
        self.monitor.watchlist = list(old.watchlist)
        self.monitor.entry_prices = dict(old.entry_prices)
        self.monitor.positions = list(old.positions)
        self.monitor.triggered = set(old.triggered)
        self.monitor.log.connect(self._mon_log_append)
        self.monitor.alert.connect(self._mon_alert)
        self.monitor.position_update.connect(self._mon_pos_update)
        self.monitor.set_conditions(
            breakout_days=int(self.mon_breakout.value()),
            stop_loss=float(self.mon_stop_loss.value()),
            take_profit=float(self.mon_take_profit.value())
        )
        self.monitor.running = True
        self.monitor.start()
        self.mon_start_btn.setText('⏹ 停止')
        self.mon_status.setText(f'▶ 运行中 · 监控池: {len(self.monitor.watchlist)} 只')

    def _mon_log_append(self, msg):
        self.mon_log.append(msg)

    def _mon_alert(self, code, msg):
        # 本地声音提醒
        try:
            import winsound
            winsound.PlaySound('SystemExclamation', winsound.SND_ALIAS | winsound.SND_ASYNC)
        except:
            pass
        # 弹窗
        QMessageBox.information(self, f'📥 {code}', msg)
        # 飞书通知（可选）
        import threading
        def notify(): send_feishu_msg(msg)
        threading.Thread(target=notify, daemon=True).start()

    def _mon_pos_update(self, pool_data):
        """更新持仓表（所有监控股票 + 实时价格）"""
        n = len(pool_data)
        self.pos_tbl.setRowCount(n)
        for i, p in enumerate(pool_data):
            clean = p.get('clean', p.get('code','').replace('SHSE.','').replace('SZSE.',''))
            price = p.get('price',0)
            status = p.get('status','')
            is_bought = p.get('is_bought', False)
            n_high = p.get('n_high', 0)
            entry_price = p.get('entry_price', None)
            pnl = p.get('pnl', 0)

            self.pos_tbl.setItem(i, 0, mk(clean, WHITE))
            if is_bought and entry_price is not None:
                # 已买入：显示买入价、盈亏
                self.pos_tbl.setItem(i, 1, mk(f'{entry_price:.2f}', '#e3b341'))
                self.pos_tbl.setItem(i, 2, mk(f'{price:.2f}', WHITE))
                self.pos_tbl.setItem(i, 3, mk(f'{pnl:+.1f}%', GREEN if pnl >= 0 else RED))
                self.pos_tbl.setItem(i, 4, mk(status, WHITE))
            else:
                # 未买入：显示现价、60日高点、差距
                self.pos_tbl.setItem(i, 1, mk(f'{price:.2f}', WHITE))
                self.pos_tbl.setItem(i, 2, mk(f'{n_high:.2f}', '#8b949e') if n_high > 0 else mk('-', '#8b949e'))
                pct = (price / n_high - 1) * 100 if n_high > 0 else 0
                self.pos_tbl.setItem(i, 3, mk(f'{pct:+.1f}%', RED if pct >= 0 else '#8b949e'))
                self.pos_tbl.setItem(i, 4, mk(status, '#8b949e'))

    # ── 窗口居中 ──
    def showEvent(self, ev):
        super().showEvent(ev)
        def _center():
            ps = self.screen().availableGeometry()
            g = self.geometry()
            self.move(max(ps.x(), ps.x()+(ps.width()-g.width())//2),
                      max(ps.y(), ps.y()+(ps.height()-g.height())//2))
            self.raise_(); self.activateWindow()
        QTimer.singleShot(500, _center)
        QTimer.singleShot(3000, _center)


# ══════════════════════════════════════════
if __name__ == '__main__':
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    w = MainWin()
    w.resize(1600,900); w.show()
    sys.exit(app.exec_())
