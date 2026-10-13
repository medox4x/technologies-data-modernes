import io, json, time, uuid, sys
import boto3, pandas as pd
from confluent_kafka import Consumer
 
c = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "bronze-writer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,          # on valide les offsets nous-mêmes
})
def on_assign(consumer, parts):
    print("partitions :", [p.partition for p in parts])
 
c.subscribe(["orders"], on_assign=on_assign)
 
s3 = boto3.client("s3", endpoint_url="http://localhost:9000",
                  aws_access_key_id="MedoDataEng",
                  aws_secret_access_key="MedoDataEng")
BATCH_SIZE, BATCH_SECONDS = 1000, 30
batch, t0 = [], time.time()
try:
    while True:
        msg = c.poll(1.0)
        if msg is not None and not msg.error():
            batch.append(json.loads(msg.value()))
        if batch and (len(batch) >= BATCH_SIZE or time.time() - t0 > BATCH_SECONDS):
            buf = io.BytesIO()
            pd.DataFrame(batch).to_parquet(buf, index=False)
            day = time.strftime("%Y-%m-%d")
            key = f"stream/orders/ingestion_date={day}/{uuid.uuid4()}.parquet"
            s3.put_object(Bucket="bronze", Key=key, Body=buf.getvalue())
            c.commit(asynchronous=False)  # APRÈS l'écriture : garantie at-least-once
            print(f"{len(batch)} événements écrits dans {key}")
            batch, t0 = [], time.time()
except KeyboardInterrupt:
    sys.exit(0)
finally:
    c.close()
