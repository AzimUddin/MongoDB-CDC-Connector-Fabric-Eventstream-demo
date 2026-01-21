#!/usr/bin/env python3
import os, argparse, random, time
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient, UpdateOne

MONGODB_URI = os.environ.get('MONGODB_URI')
DB_NAME = os.environ.get('MONGO_DB','claims-CDC-demo')
COLL_NAME = os.environ.get('MONGO_COLLECTION','claims-demo')
assert MONGODB_URI, 'Set MONGODB_URI env var'

client = MongoClient(MONGODB_URI)
coll = client[DB_NAME][COLL_NAME]

REGIONS=['Texas','California','New York','Illinois','Florida','Washington','Colorado']
STATUSES_OPEN=['Open','Under Review','Approved','Rejected','Escalated']

def iso_now(offset_secs=0):
    dt=datetime.now(timezone.utc)+timedelta(seconds=offset_secs)
    return dt.isoformat(timespec='milliseconds').replace('+00:00','Z')

def new_claim_id():
    return f'CLM-{random.randint(200000,999999)}'

def new_event_id():
    return f'evt-{int(time.time()*1000)}-{random.randint(100,999)}'

def random_amount():
    base=random.choice([50,100,250,500,750,1000,1500,2000,5000])
    amt=base*random.uniform(0.5,1.5)
    if random.random()<0.2: amt*=-1.0
    return round(amt,2)

def random_fraud():
    return round(random.uniform(0.80,0.99),2) if random.random()<0.15 else round(random.uniform(0.0,0.79),2)

def build_insert_docs(n):
    docs=[]
    for _ in range(n):
        r=random.random()
        if r<0.15: eventType,claimStatus='ClaimClosed','Closed'
        elif r<0.45: eventType,claimStatus='AmountAdjusted',random.choice(STATUSES_OPEN)
        elif r<0.70: eventType,claimStatus='ClaimCreated','Open'
        else: eventType,claimStatus='StatusUpdated',random.choice(STATUSES_OPEN)
        docs.append({
            'eventId':new_event_id(),
            'claimId':new_claim_id(),
            'eventType':eventType,
            'eventTimestamp':iso_now(offset_secs=random.randint(-1200,0)),
            'claimStatus':claimStatus,
            'claimAmountDelta':float(random_amount()),
            'region':random.choice(REGIONS),
            'fraudScore':float(random_fraud())
        })
    return docs

def sample_existing_claims(limit=100):
    pipeline=[{'$sample':{'size':limit}},{'$project':{'_id':0,'claimId':1}}]
    ids=[]
    for d in coll.aggregate(pipeline):
        cid=d.get('claimId')
        if cid: ids.append(cid)
    return list(set(ids))

def build_update_ops(ids):
    ops=[]
    for cid in ids:
        r=random.random()
        if r<0.3: upd={'eventId':new_event_id(),'eventType':'ClaimClosed','eventTimestamp':iso_now(),'claimStatus':'Closed','claimAmountDelta':0.0,'fraudScore':round(random.uniform(0.0,0.5),2)}
        elif r<0.65: upd={'eventId':new_event_id(),'eventType':'AmountAdjusted','eventTimestamp':iso_now(),'claimStatus':random.choice(STATUSES_OPEN),'claimAmountDelta':float(random_amount()),'fraudScore':float(random_fraud())}
        else: upd={'eventId':new_event_id(),'eventType':'StatusUpdated','eventTimestamp':iso_now(),'claimStatus':random.choice(STATUSES_OPEN),'claimAmountDelta':0.0,'fraudScore':float(random_fraud())}
        upd.setdefault('region',random.choice(REGIONS))
        ops.append(UpdateOne({'claimId':cid},{'$set':upd}))
    return ops

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--insert',type=int,default=1000)
    ap.add_argument('--update',type=int,default=100)
    args=ap.parse_args()
    docs=build_insert_docs(args.insert)
    if docs: coll.insert_many(docs,ordered=False); print(f'Inserted {len(docs)} events')
    ids=sample_existing_claims(args.update)
    if not ids: print('No existing claims found to update. Run inserts first.'); return
    ops=build_update_ops(ids)
    if ops: res=coll.bulk_write(ops,ordered=False); print(f'Updated {res.modified_count} existing claims')

if __name__=='__main__': main()
