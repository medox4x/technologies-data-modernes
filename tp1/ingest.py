import io, uuid, random, datetime as dt
import boto3, pandas as pd
from faker import Faker
 
fake = Faker("fr_FR"); random.seed(42)
s3 = boto3.client("s3", endpoint_url="http://localhost:9000",
                  aws_access_key_id="MedoDataEng",
                  aws_secret_access_key="MedoDataEng")
 
N = 200_000
villes = [fake.city() for _ in range(300)]   # Faker est lent : on tire dans une liste
now = pd.Timestamp.now()
df = pd.DataFrame({
    "order_id":    [str(uuid.uuid4()) for _ in range(N)],
    "customer_id": [random.randint(1, 50_000) for _ in range(N)],
    "order_ts":    now - pd.to_timedelta(
                       [random.randint(0, 90 * 86400) for _ in range(N)], "s"),
    "city":        random.choices(villes, k=N),
    "amount":      [round(random.uniform(5, 900), 2) for _ in range(N)],
    "status":      random.choices(["PAID", "SHIPPED", "CANCELLED"], k=N),
})
 
# Métadonnées d'ingestion : indispensables en Bronze (audit, rejeu)
batch = uuid.uuid4().hex[:8]
df["_ingested_at"] = dt.datetime.now(dt.timezone.utc)
df["_batch_id"] = batch
day = dt.date.today().isoformat()
 
for fmt, write in [("csv", df.to_csv), ("parquet", df.to_parquet)]:
    buf = io.BytesIO()
    write(buf, index=False)
    key = f"{fmt}/orders/ingestion_date={day}/{batch}.{fmt}"
    s3.put_object(Bucket="bronze", Key=key, Body=buf.getvalue())
    print(f"{key} : {buf.getbuffer().nbytes / 1e6:.1f} Mo")
