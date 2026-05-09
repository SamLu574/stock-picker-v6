# A股形态选股系统 v6.0

基于三层过滤（L3风险过滤 + L2突破筛选 + L1 K线相似度匹配）的A股技术面选股工具，支持实时监控、历史回测和盘中突破报警。

## 功能特性

- **三层过滤选股**：排除高风险股票 → 筛选突破形态 → 匹配K线模板
- **实时监控**：盘中每3分钟自动刷新，触发突破/止损/止盈时声音+弹窗提醒
- **历史回测**：验证策略有效性，显示收益率、胜率、交易明细
- **数据自动更新**：基于 efinance/AKShare，免费稳定

## 安装与运行

### 方式一：直接运行 exe（推荐新手）

1. 从 [Releases](https://github.com/SamLu574/stock-picker-v6/releases) 下载最新版 `.exe`
2. 双击运行，首次启动约 15-30 秒
3. 点击"🔄 更新数据"获取最新行情

### 方式二：从源码运行

```bash
# 1. 克隆仓库
git clone https://github.com/SamLu574/stock-picker-v6.git
cd stock-picker-v6

# 2. 安装依赖
pip install PyQt5 efinance akshare numpy requests Pillow

# 3. 运行主程序
python stock_picking_v6.py
```

## 界面说明

### 标签页 1：实时监控
- 添加股票代码到监控池
- 设置突破天数、止损/止盈比例
- 启动后每3分钟自动刷新价格
- 触发条件时播放声音 + 弹窗通知

### 标签页 2：形态选股
- **L3 风险过滤**：排除 ST、科创板、北交所
- **L2 突破筛选**：设置半年涨幅上限、突破天数
- **L1 K线相似度**：上传参考图片，设置相似度阈值
- 点击"▶ 执行选股"查看结果

### 标签页 3：回溯验证
- 设置回测时间范围、初始资金、仓位比例
- 点击"▶ 开始回测"查看历史表现
- 显示总收益率、最终资金、胜率、交易明细

## 项目结构

```
stock-picker-v6/
├── stock_picking_v6.py   # 主程序（GUI入口）
├── backtest_engine.py    # 回测引擎核心逻辑
├── backtest_tab.py       # 回测UI组件
├── data_layer.py         # 数据层（efinance/AKShare）
├── cn_strings.py         # 中文字符串常量
├── helpers.py            # 工具函数
├── StockPicker.spec      # PyInstaller 打包配置
├── .gitignore
└── 使用说明.txt           # 详细用户手册
```

## 注意事项

- ⚠️ **本软件仅供学习研究，不构成投资建议**
- 💾 建议定期备份 `stock_data.db` 文件
- 🌐 数据更新和实时监控需要联网
- 🔊 确保电脑音量开启以接收声音提醒

## 技术栈

- **GUI**: PyQt5
- **数据源**: efinance, AKShare
- **数据处理**: NumPy, Pandas
- **打包**: PyInstaller

## License

MIT
