#!/usr/bin/env python3
# Summarize procmon CSVs: derive rates from cumulative /proc counters.
# Usage: proc_analyze.py [--from TS --to TS] file1.csv [file2.csv ...]
# Columns: ts,cpu_user,cpu_sys,cpu_idle,cpu_iowait,rd_ios,rd_sec,wr_ios,wr_sec,io_ms,rx_bytes,tx_bytes
import sys, csv, os

def load(path):
    rows = []
    with open(path) as f:
        r = csv.reader(f)
        next(r, None)  # header
        for x in r:
            if len(x) < 12: continue
            try: rows.append([float(v) for v in x[:12]])
            except ValueError: continue
    return rows

def summarize(path, t0=None, t1=None):
    rows = load(path)
    if len(rows) < 2:
        print(f"{os.path.basename(path)}: <2 samples"); return
    iops=[]; rmb=[]; wio=[]; util=[]; busy=[]; iow=[]; rx=[]; tx=[]
    for a,b in zip(rows, rows[1:]):
        dt = b[0]-a[0]
        if dt <= 0: continue
        if t0 and b[0] < t0: continue
        if t1 and a[0] > t1: continue
        d_user,d_sys,d_idle,d_iow = b[1]-a[1], b[2]-a[2], b[3]-a[3], b[4]-a[4]
        cput = d_user+d_sys+d_idle+d_iow
        if cput>0:
            busy.append(100*(d_user+d_sys)/cput); iow.append(100*d_iow/cput)
        iops.append((b[5]-a[5])/dt)
        rmb.append((b[6]-a[6])*512/dt/1e6)
        wio.append((b[7]-a[7])/dt)
        util.append(100*(b[9]-a[9])/(dt*1000))
        rx.append((b[10]-a[10])*8/dt/1e6)
        tx.append((b[11]-a[11])*8/dt/1e6)
    n=len(iops)
    if n==0: print(f"{os.path.basename(path)}: no samples in window"); return
    def ms(v): return (sum(v)/len(v), max(v)) if v else (0,0)
    span = rows[-1][0]-rows[0][0]
    nm = os.path.basename(path).replace('procmon.','').replace('.csv','')
    print(f"── {nm}  ({n} samples, {span:.0f}s) ──")
    for label,v,unit in [("rd_iops",iops,""),("rd_MB/s",rmb,""),("wr_iops",wio,""),
                          ("io_util%",util,""),("cpu_busy%",busy,""),("iowait%",iow,""),
                          ("rx_Mbps",rx,""),("tx_Mbps",tx,"")]:
        a,p = ms(v)
        print(f"   {label:10s} mean={a:8.1f}  peak={p:8.1f}")

args = sys.argv[1:]
t0=t1=None
files=[]
i=0
while i < len(args):
    if args[i]=="--from": t0=float(args[i+1]); i+=2
    elif args[i]=="--to": t1=float(args[i+1]); i+=2
    else: files.append(args[i]); i+=1
for f in files:
    summarize(f, t0, t1)
