# -*- mode: python ; coding: utf-8 -*-
block_cipher = None

a = Analysis(
    ['stock_picking_v6.py'],
    pathex=[],
    binaries=[],
    datas=[('stock_data.db', '.')],
    hiddenimports=['efinance', 'efinance.stock', 'akshare'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['torch', 'tensorboard', 'IPython', 'jedi', 'pygments', 'lxml', 'zmq', 'tkinter', '_tkinter'],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='A股形态选股系统v6.0',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
