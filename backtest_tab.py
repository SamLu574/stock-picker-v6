"""backtest_tab.py Reliable Edition"""
import os, json
from datetime import datetime
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *

BLUE = "#58a6ff"
DARK = "#0d1117"
CARD2 = "#161b22"
RED = "#ef4444"
GREEN = "#22c55e"
WHITE = "#e6edf3"


def mk(t, c=WHITE):
    i = QTableWidgetItem(str(t))
    i.setForeground(QColor(c))
    return i


class BacktestTab(QWidget):
    def __init__(self, db, cf, parent=None):
        super().__init__(parent)
        self.db = db
        self.cf = cf
        d = os.path.dirname(os.path.abspath(db))
        self._sf = os.path.join(d, ".bt_progress.txt")   # state file
        self._rf = os.path.join(d, ".bt_result.json")    # result file
        self._tm = QTimer(self)
        self._tm.timeout.connect(self._poll)
        self._ui()

    # ── 轮询:从文件读进度（跨线程唯一可靠方式）───────
    def _poll(self):
        try:
            if not os.path.exists(self._sf):
                return
            with open(self._sf, "r", encoding="utf-8") as f:
                line = f.readline().strip()
            pct, msg = 0, ""
            if "|" in line:
                parts = line.split("|", 1)
                pct = int(parts[0]) if parts[0].isdigit() else 0
                msg = parts[1]
            self._pbar.setValue(pct)
            self._plbl.setText(msg)
            # 只有 pct>=100 且 结果JSON已写入 才停止
            if pct >= 100 and os.path.exists(self._rf):
                self._tm.stop()
                self._run_btn.setEnabled(True)
                self._run_btn.setText("▶ 开始回测")
                with open(self._rf, "r", encoding="utf-8") as f:
                    rdata = json.load(f)
                # 调试日志
                print(f"[BACKTEST] JSON loaded: keys={list(rdata.keys())}")
                print(f"[BACKTEST] daily={len(rdata.get('daily',[]))}, trades={len(rdata.get('trades',[]))}")
                print(f"[BACKTEST] summary={rdata.get('summary',{})}")
                self._show(rdata)
                print(f"[BACKTEST] _show done")
                # 清理
                for fp in [self._sf, self._rf]:
                    if os.path.exists(fp):
                        os.remove(fp)
        except Exception as e:
            import traceback
            print(f"[BACKTEST] _poll error: {e}")
            traceback.print_exc()

    # ── 构建所有 UI 控件并保存为实例属性 ──
    def _ui(self):
        # 全局字体统一设置为 14px
        font = QFont('Microsoft YaHei', 14)
        self.setFont(font)

        L = QVBoxLayout(self)
        L.setContentsMargins(0, 0, 0, 0)
        L.setSpacing(8)

        pf = QFrame()
        pf.setStyleSheet(f"background:{CARD2};border-radius:6px;border:1px solid #30363d")
        pv = QVBoxLayout(pf)
        pv.setContentsMargins(12, 8, 12, 8)
        pv.setSpacing(6)

        # 标题
        t = QLabel("📊 回测系统")
        t.setStyleSheet(f"color:#74c7ec;font-size:19px;font-weight:bold")
        pv.addWidget(t)

        # 第一行: 日期 + 资金 + 持仓
        r1 = QHBoxLayout()
        self.bt_start = QDateEdit()
        self.bt_start.setCalendarPopup(True)
        self.bt_start.setDate(QDate(2026, 4, 6))
        self.bt_start.setDisplayFormat("yyyy-MM-dd")
        self.bt_end = QDateEdit()
        self.bt_end.setCalendarPopup(True)
        self.bt_end.setDate(QDate(2026, 5, 6))
        self.bt_end.setDisplayFormat("yyyy-MM-dd")
        self.bt_cap = QSpinBox()
        self.bt_cap.setRange(10000, 10000000)
        self.bt_cap.setSingleStep(10000)
        self.bt_cap.setValue(100000)
        self.bt_mp = QSpinBox()
        self.bt_mp.setRange(1, 20)
        self.bt_mp.setSingleStep(1)
        self.bt_mp.setValue(5)
        for w in [self.bt_start, self.bt_end, self.bt_cap, self.bt_mp]:
            w.setStyleSheet("background:#1e1e2e;color:#cdd6f4;border:1px solid #45475a;border-radius:4px;padding:3px;font-size:19px")
            w.setFixedHeight(26)
        for nm, w in [
            ("起始", self.bt_start), ("截止", self.bt_end),
            ("资金", self.bt_cap), ("持仓", self.bt_mp)
        ]:
            r1.addWidget(self._lb(nm))
            r1.addWidget(w)
        r1.addStretch()
        pv.addLayout(r1)

        # 第二行: 止损/止盈/滑点
        r2 = QHBoxLayout()
        self.bt_sl = QDoubleSpinBox()
        self.bt_sl.setRange(0.5, 50)
        self.bt_sl.setSingleStep(0.5)
        self.bt_sl.setValue(5.0)
        self.bt_sl.setDecimals(1)
        self.bt_tp = QDoubleSpinBox()
        self.bt_tp.setRange(1, 100)
        self.bt_tp.setSingleStep(1)
        self.bt_tp.setValue(20.0)
        self.bt_tp.setDecimals(1)
        self.bt_sp = QDoubleSpinBox()
        self.bt_sp.setRange(0, 5)
        self.bt_sp.setSingleStep(0.05)
        self.bt_sp.setValue(0.1)
        self.bt_sp.setDecimals(2)
        for w in [self.bt_sl, self.bt_tp, self.bt_sp]:
            w.setStyleSheet("background:#1e1e2e;color:#cdd6f4;border:1px solid #45475a;border-radius:4px;padding:3px;font-size:19px")
            w.setFixedHeight(26)
        for nm, w in [("止损%", self.bt_sl), ("止盈%", self.bt_tp), ("滑点%", self.bt_sp)]:
            r2.addWidget(self._lb(nm))
            r2.addWidget(w)
        r2.addStretch()
        pv.addLayout(r2)

        # 按钮 + 进度
        br = QHBoxLayout()
        self._run_btn = QPushButton("▶ 开始回测")
        self._run_btn.setFixedHeight(32)
        self._run_btn.setFont(QFont('Microsoft YaHei', 13, QFont.Bold))
        self._run_btn.setStyleSheet(
            f"QPushButton{{background:{BLUE};color:{DARK};border:none;border-radius:6px;padding:6px;font-size:19px;font-weight:bold}}"
            f"QPushButton:hover{{background:#79c0ff}}"
            f"QPushButton:disabled{{background:#484f58;color:#8b949e}}")
        self._run_btn.clicked.connect(self._run)
        br.addWidget(self._run_btn)
        self._plbl = QLabel("")
        self._plbl.setStyleSheet("color:#8b949e;font-size:19px")
        br.addWidget(self._plbl)
        br.addStretch()
        pv.addLayout(br)

        self._pbar = QProgressBar()
        self._pbar.setValue(0)
        self._pbar.setFixedHeight(18)
        self._pbar.setStyleSheet(
            f"QProgressBar{{background:#1e1e2e;border:1px solid #45475a;border-radius:4px;text-align:center;color:#cdd6f4;font-size:15px}}"
            f"QProgressBar::chunk{{background:{BLUE};border-radius:4px}}")
        pv.addWidget(self._pbar)
        L.addWidget(pf, 0)

        # 指标卡片
        mf = QFrame()
        mf.setStyleSheet(f"background:{CARD2};border-radius:6px;border:1px solid #30363d")
        mv = QHBoxLayout(mf)
        mv.setContentsMargins(12, 8, 12, 8)
        mv.setSpacing(12)
        self._metrics = {}
        for lb, ky in [
            ("总收益", "ret"), ("年化", "yoy"), ("最大回撤", "dd"),
            ("胜率", "wr"), ("盈亏比", "plr"), ("最终资金", "fv"), ("次数", "tc")
        ]:
            c_, l_, v_ = self._mc(lb)
            mv.addWidget(c_)
            self._metrics[ky] = v_
        L.addWidget(mf, 0)

        # 收益曲线
        cf2 = QFrame()
        cf2.setStyleSheet(f"background:{CARD2};border-radius:6px;border:1px solid #30363d")
        cv = QVBoxLayout(cf2)
        cv.setContentsMargins(8, 4, 8, 4)
        self._chart_lbl = QLabel("图表")
        self._chart_lbl.setAlignment(Qt.AlignCenter)
        self._chart_lbl.setStyleSheet("color:#8b949e;font-size:17px;padding:30px")
        cv.addWidget(self._chart_lbl)
        L.addWidget(cf2, 1)

        # 交易记录
        tf = QFrame()
        tf.setStyleSheet(f"background:{CARD2};border-radius:6px;border:1px solid #30363d")
        tv = QVBoxLayout(tf)
        tv.setContentsMargins(8, 4, 8, 4)
        tv.addWidget(self._mt("📋 交易记录", 13))
        self._tbl = self._make_tbl()
        tv.addWidget(self._tbl)
        L.addWidget(tf, 1)

    def _lb(self, t):
        l = QLabel(t)
        l.setStyleSheet("color:#9399b2;font-size:19px")
        return l

    def _mt(self, t, sz=14):
        l = QLabel(t)
        l.setStyleSheet(f"color:#74c7ec;font-size:{sz}px;font-weight:bold")
        return l

    def _mc(self, lb):
        col = QWidget()
        cv = QVBoxLayout(col)
        cv.setContentsMargins(0, 0, 0, 0)
        cv.setSpacing(2)
        l = QLabel(lb)
        l.setStyleSheet("color:#8b949e;font-size:15px")
        l.setAlignment(Qt.AlignCenter)
        cv.addWidget(l)
        v = QLabel("-")
        v.setStyleSheet(f"color:{WHITE};font-size:19px;font-weight:bold")
        v.setAlignment(Qt.AlignCenter)
        v.setMinimumWidth(80)
        cv.addWidget(v)
        return col, l, v

    def _make_tbl(self):
        t = QTableWidget(0, 7)
        t.setHorizontalHeaderLabels(["代码", "买入日", "买价", "卖出日", "卖价", "盈亏%", "原因"])
        for i, ww in enumerate([70, 80, 60, 80, 60, 65, 60]):
            t.setColumnWidth(i, ww)
        t.horizontalHeader().setSectionResizeMode(QHeaderView.Fixed)
        t.setSelectionBehavior(QTableWidget.SelectRows)
        t.verticalHeader().setVisible(False)
        t.setStyleSheet(
            f"QTableWidget{{background:{DARK};color:#cdd6f4;gridline-color:#313244;font-size:15px;border:none}}"
            f"QHeaderView::section{{background:{CARD2};color:#74c7ec;padding:3px;font-size:15px;font-weight:bold;border:none}}"
            f"QTableWidget::item{{padding:3px}}")
        return t

    # ── 开始回测 ──
    def _run(self):
        # 重置 UI
        self._run_btn.setEnabled(False)
        self._run_btn.setText("⏳ 回测中...")
        self._pbar.setValue(0)
        self._plbl.setText("准备中…")
        self._tbl.setRowCount(0)
        self._chart_lbl.setText("")
        for v in self._metrics.values():
            v.setText("-")
        # 清理旧状态文件
        for fp in [self._sf, self._rf]:
            if os.path.exists(fp):
                os.remove(fp)
        # 启动轮询 (100ms 巡查一次)
        self._tm.start(100)

        # 获取参数
        try:
            ps = self.cf()
            print(f"[BACKTEST] ref_image = {ps.get('ref_img')!r}")
        except Exception as e:
            self._plbl.setText(f"参数错误: {e}")
            self._tm.stop()
            self._run_btn.setEnabled(True)
            self._run_btn.setText("▶ 开始回测")
            return

        # DEBUG
        ref = ps.get("ref_img")
        print(f"[BACKTEST] ref_image_path = {ref!r}")
        if ref:
            print(f"[BACKTEST] ref_image exists = {os.path.exists(ref)}")

        # 构建 BacktestConfig
        from backtest_engine import BacktestConfig
        cfg = BacktestConfig(
            start_date=self.bt_start.date().toString("yyyy-MM-dd"),
            end_date=self.bt_end.date().toString("yyyy-MM-dd"),
            initial_capital=self.bt_cap.value(),
            max_positions=self.bt_mp.value(),
            exclude_st=ps.get("st", True),
            half_year_limit=ps.get("hy", 110),
            concentration=ps.get("cc", 75),
            breakout_days=ps.get("bh_d", 60),
            breakout_threshold=ps.get("bh_t", 0.98),
            limit_up_days=ps.get("zd_n", 5),
            similarity_threshold=ps.get("l1t", 73),
            ref_image=ps.get("ref_img"),
            stop_loss=self.bt_sl.value() / 100,
            take_profit=self.bt_tp.value() / 100,
            slippage=self.bt_sp.value() / 100,
        )

        # 进度/结果文件路径
        sf = self._sf
        rf = self._rf

        # ── 回测线程内部 callback:写文件 ──
        def on_progress(pct, msg):
            try:
                with open(sf, "w", encoding="utf-8") as f:
                    f.write(f"{pct}|{msg}")
            except Exception:
                pass

        # ── 回测线程入口 ──
        def do_run():
            from backtest_engine import BacktestEngine
            try:
                eng = BacktestEngine(self.db, cfg, progress_callback=on_progress)
                res = eng.run()
                # 写结果 JSON
                d = res.to_dict()
                with open(rf, "w", encoding="utf-8") as f:
                    json.dump(d, f, ensure_ascii=False)
                with open(sf, "w", encoding="utf-8") as f:
                    f.write("100|完成")
            except Exception as e:
                import traceback
                try:
                    with open(sf, "w", encoding="utf-8") as f:
                        f.write(f"0|错误: {str(e)[:80]}")
                except Exception:
                    pass

        import threading
        threading.Thread(target=do_run, daemon=True).start()

    # ── 从 JSON 渲染结果 ──
    def _show(self, d):
        daily = d.get("daily", [])
        trades = d.get("trades", [])
        s = d.get("summary", {})

        try:
            def pct_field(key):
                try:
                    return float(s.get(key, "0%").rstrip("%"))
                except:
                    return 0

            tr_ret = pct_field("Total Return")
            cm = {
                "ret": RED if tr_ret >= 0 else GREEN,
                "yoy": RED if pct_field("Ann Return") >= 0 else GREEN,
                "dd": RED,
                "wr": GREEN if pct_field("Win Rate") > 50 else RED,
                "plr": GREEN,
                "fv": GREEN,
                "tc": WHITE,
            }
            vm = {
                "ret": s.get("Total Return", "-"),
                "yoy": s.get("Ann Return", "-"),
                "dd": s.get("Max DD", "-"),
                "wr": s.get("Win Rate", "-"),
                "plr": s.get("P/L Ratio", "-"),
                "fv": s.get("Final Value", "-"),
                "tc": str(len(trades)),
            }
            for ky, val in vm.items():
                self._metrics[ky].setText(val)
                self._metrics[ky].setStyleSheet(f"color:{cm[ky]};font-size:19px;font-weight:bold")
            print(f"[BACKTEST] metrics set")

            self._tbl.setRowCount(len(trades))
            for i, t in enumerate(trades):
                sym = t.get("symbol", "").replace("SHSE.", "").replace("SZSE.", "")
                self._tbl.setItem(i, 0, mk(sym, WHITE))
                self._tbl.setItem(i, 1, mk(str(t.get("buy_date", ""))[:10], "#9399b2"))
                self._tbl.setItem(i, 2, mk(f'{t.get("buy_price", 0):.2f}', WHITE))
                sd = t.get("sell_date", "")
                self._tbl.setItem(i, 3, mk(str(sd)[:10] if sd else "-", "#9399b2"))
                sp = t.get("sell_price")
                self._tbl.setItem(i, 4, mk(f'{sp:.2f}' if sp else "-", WHITE))
                pnl = t.get("pnl_pct", 0)
                pc = RED if pnl >= 0 else GREEN
                self._tbl.setItem(i, 5, mk(f'{pnl:+.1f}%', pc))
                self._tbl.setItem(i, 6, mk(t.get("reason", ""), "#9399b2"))
            print(f"[BACKTEST] trades set: {len(trades)}")

            self._draw_chart(daily, trades)
            print(f"[BACKTEST] chart done")
        except Exception as e:
            import traceback
            print(f"[BACKTEST] _show error: {e}")
            traceback.print_exc()

    # ── matplotlib 收益曲线 + 买卖点 ──
    def _draw_chart(self, daily, trades=None):
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            import matplotlib.dates as mdates
            from io import BytesIO
            if not daily:
                return
            if trades is None:
                trades = []

            dates = [datetime.strptime(x["date"], "%Y-%m-%d") for x in daily]
            vals = [x["total_value"] for x in daily]
            dates_vals = {d: v for d, v in zip(dates, vals)}

            fig, ax = plt.subplots(figsize=(9, 3.5), dpi=100)
            fig.patch.set_facecolor("#0d1117")
            ax.set_facecolor("#0d1117")
            cl = "#22c55e" if vals[-1] >= vals[0] else "#ef4444"
            ax.plot(dates, vals, color=cl, lw=1.5, label="组合净值")
            ax.fill_between(dates, vals, alpha=0.08, color=cl)

            # ── 买卖点标记 ──
            for t in trades:
                bd = datetime.strptime(str(t.get("buy_date", ""))[:10], "%Y-%m-%d")
                sd_str = t.get("sell_date")
                if not sd_str:
                    continue
                sd = datetime.strptime(str(sd_str)[:10], "%Y-%m-%d")
                bp = dates_vals.get(bd)
                sp = dates_vals.get(sd)
                pnl = t.get("pnl_pct", 0)
                mc = "#22c55e" if pnl >= 0 else "#ef4444"  # 盈绿亏红
                if bp:
                    ax.scatter([bd], [bp], color="#00bfff", zorder=5, s=60,
                               marker="^", edgecolors="#fff", linewidths=0.5)
                    ax.annotate(f"买", xy=(bd, bp), xytext=(0, 8),
                                textcoords="offset points", fontsize=8,
                                color="#00bfff", ha="center", fontweight="bold")
                if sp:
                    ax.scatter([sd], [sp], color=mc, zorder=5, s=60,
                               marker="v", edgecolors="#fff", linewidths=0.5)
                    ax.annotate(f"卖({pnl:+.0f}%)", xy=(sd, sp), xytext=(0, -12),
                                textcoords="offset points", fontsize=8,
                                color=mc, ha="center", fontweight="bold")

            total_pct = (vals[-1] / vals[0] - 1) * 100 if vals[0] > 0 else 0
            ax.set_title(f"收益曲线  总收益: {total_pct:+.1f}%  |  交易{len(trades)}笔",
                         color="#e6edf3", fontsize=12, fontweight="bold")
            ax.tick_params(colors="#8b949e")
            for sp2 in ["bottom", "left"]:
                ax.spines[sp2].set_color("#30363d")
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
            ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
            plt.xticks(rotation=30)
            buf = BytesIO()
            plt.tight_layout()
            plt.savefig(buf, format="png", dpi=100, facecolor="#0d1117")
            plt.close(fig)
            buf.seek(0)
            pm = QPixmap()
            pm.loadFromData(buf.read())
            self._chart_lbl.setPixmap(
                pm.scaled(self._chart_lbl.width(), 280,
                          Qt.KeepAspectRatio, Qt.SmoothTransformation))
            self._chart_lbl.setText("")
        except Exception as e:
            import traceback
            print(f"[BACKTEST] chart error: {e}")
            traceback.print_exc()
            self._chart_lbl.setText(f"图表错误: {e}")
