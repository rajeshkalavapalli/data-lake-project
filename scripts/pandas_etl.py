import pandas as pd 

user = pd.read_csv(r'D:\data-lake-project\data\user.csv')
transaction = pd.read_csv(r'D:\data-lake-project\data\transection.csv')

merged = transaction.merge(user, on="user_id", how="left")

merged["transaction_category"] = merged["amount"].apply(
    lambda x: "high" if x > 250 else "low"
)

merged["tsx_date"] = pd.to_datetime(merged["tsx_date"])

revenue_country = merged.groupby("country")["amount"].sum().reset_index()

# Now save
merged.to_parquet(r"D:\data-lake-project\data\transaction.parquet", index=False)
revenue_country.to_csv(r"D:\data-lake-project\data\revenue_by_country.csv", index=False)
