"""Classify strict audit rejections; never relax the audit or generate trades."""
import json,gzip
from collections import Counter
from pathlib import Path
from . import audit
ROOT=Path(__file__).parent

def zero_truncation(book,row):
    has_zero=False
    for side,other in [('yes','no'),('no','yes')]:
        expected={'bid_levels':sorted(book[side].items(),reverse=True)[:3],
                  'ask_levels':sorted((100-p,q) for p,q in book[other].items())[:3]}
        for name,levels in expected.items():
            raw=row['book'][side][name]
            has_zero|=any(audit.number(q)==0 for p,q in raw)
            positive=sorted(audit.ladder(raw).items(),reverse=name=='bid_levels')
            if not audit.equal_levels(positive,levels[:len(positive)]):return False
    return has_zero

def run():
    original=audit.verify;counts=Counter();samples=[]
    def checked(book,row):
        try:original(book,row)
        except ValueError:
            kind='zero_quantity_top3_truncation' if zero_truncation(book,row) else 'other_depth_disagreement'
            counts[kind]+=1
            if len(samples)<5:samples.append(dict(kind=kind,ticker=row['ticker'],seq=row['seq'],record_type=row['record_type'],raw_delta={k:row.get(k) for k in ('delta_side','delta_price_cents','delta_qty','level_qty_before','level_qty_after')},displayed=row['book'],reconstructed_top4={s:sorted(b.items(),reverse=True)[:4] for s,b in book.items()}))
            raise
    audit.verify=checked
    receipt=json.loads((ROOT.parent/'ask_markdown_20261016/acquisition.json').read_text())['files'][5]
    a=audit.Audit()
    for line in gzip.open(receipt['path'],'rt',encoding='utf-8'):a.ingest(json.loads(line))
    report=dict(schema='external_depth_rejection_diagnostic_v1',counts=dict(counts),samples=samples,strict_audit_unchanged=True,raw_delta_arithmetic_disagreements=a.counts['raw_delta_disagreement'],within_channel_clock_regressions=a.counts['within_channel_clock_regression'],global_clock_regressions=a.counts['global_clock_regression'],actual_orders=0,profitability_proven=False)
    with (ROOT/'zero-level-diagnostic.json').open('x') as f:json.dump(report,f,indent=2,default=str)
    print(json.dumps({k:v for k,v in report.items() if k!='samples'}))
if __name__=='__main__':run()
