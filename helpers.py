# -*- coding: utf-8 -*-
"""选股+监控 - 公共函数"""
import sys, os, json, requests, numpy as np

DARK='#0d1117'; CARD='#161b22'; CARD2='#1c2128'
WHITE='#c9d1d9'; GREEN='#3fb950'; YELL='#d29922'; RED='#f85149'; BLUE='#58a6ff'

def mk(t, c):
    from PyQt5.QtWidgets import QTableWidgetItem
    from PyQt5.QtGui import QColor
    from PyQt5.QtCore import Qt
    it = QTableWidgetItem(str(t)); it.setForeground(QColor(c)); it.setTextAlignment(Qt.AlignCenter); return it

def extract_candle_centers(image_path):
    import cv2
    img = cv2.imread(image_path)
    if img is None: return None, '无法读取图片'
    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
    lr1=np.array([0,100,100]); ur1=np.array([10,255,255]); lr2=np.array([160,100,100]); ur2=np.array([180,255,255])
    lg=np.array([35,100,100]); ug=np.array([85,255,255])
    mr=cv2.add(cv2.inRange(hsv,lr1,ur1),cv2.inRange(hsv,lr2,ur2)); mg=cv2.inRange(hsv,lg,ug)
    k=np.ones((3,3),np.uint8); mr=cv2.morphologyEx(mr,cv2.MORPH_CLOSE,k); mg=cv2.morphologyEx(mg,cv2.MORPH_CLOSE,k)
    mr=cv2.morphologyEx(mr,cv2.MORPH_OPEN,k); mg=cv2.morphologyEx(mg,cv2.MORPH_OPEN,k)
    cr,_=cv2.findContours(mr,cv2.RETR_EXTERNAL,cv2.CHAIN_APPROX_SIMPLE); cg,_=cv2.findContours(mg,cv2.RETR_EXTERNAL,cv2.CHAIN_APPROX_SIMPLE)
    candles=[]
    for cnt in cr+cg:
        if cv2.contourArea(cnt)>5:
            x,y,w,h=cv2.boundingRect(cnt)
            if w>0 and 0.1<h/w<10: candles.append((x+w/2,y+h/2))
    candles=sorted(candles,key=lambda c:c[0])
    if len(candles)<4: return None,f'只检测到{len(candles)}根K线'
    cy=np.array([c[1] for c in candles]); yn,yx=cy.min(),cy.max(); yr=yx-yn if yx!=yn else 1e-6
    return np.array([(yx-c[1])/yr for c in candles][-40:]),f'检测到{len(candles)}根K线'

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
        uf,cf=ul.flatten(),cl.flatten(); cs=cosine_similarity(uf.reshape(1,-1),cf.reshape(1,-1))[0][0]
        try: pc,_=pearsonr(uf,cf); ps=((pc+1)/2)*100 if not np.isnan(pc) else 0
        except: ps=0
        ur,cr2=np.ptp(uf),np.ptp(cf); us,css=np.std(uf),np.std(cf)
        rs=100*(1-abs(ur-cr2)/max(ur,cr2)) if max(ur,cr2)>1e-6 else 100
        ss=100*(1-abs(us-css)/max(us,css)) if max(us,css)>1e-6 else 100
        return round(max(0,min(100,0.3*((cs+1)/2)*100+0.2*ps+0.25*(rs+ss)/2+0.25*fast_dtw(uf,cf))),1)
    except: return 0.0

def send_feishu_msg(text):
    config_path = os.path.expanduser(r'~\.stepclaw\openclaw.json')
    try:
        with open(config_path, encoding='utf-8') as f: cfg = json.load(f)
        acc = cfg.get('channels',{}).get('feishu',{}).get('accounts',[{}])[0]
        app_id=acc.get('appId',''); app_secret=acc.get('appSecret',''); owner=acc.get('owner','')
    except: print('无法读取飞书配置'); return False
    if not app_id or not app_secret: print('飞书配置不完整'); return False
    try:
        r=requests.post('https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal',json={'app_id':app_id,'app_secret':app_secret})
        token=r.json().get('tenant_access_token','')
        if not token: return False
        url='https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=open_id'
        headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'}
        r2=requests.post(url,json={'receive_id':owner,'msg_type':'text','content':json.dumps({'text':text})},headers=headers)
        return r2.json().get('code',-1)==0
    except Exception as e: print(f'飞书发送失败: {e}'); return False
