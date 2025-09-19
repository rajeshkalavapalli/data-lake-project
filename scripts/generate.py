import pandas as pd 
import pandas as pd

import random 
import uuid
from datetime import datetime, timedelta

# generate users 

users = []

for i in range(1,101):
    users.append(
        {
            'user_id':i,
            'name':f'user_{i}',
            'country': random.choice(['UK','US','GM','DE','FR'])
        }
    )


df_users = pd.DataFrame(users)
df_users.to_csv(r"D:\data-lake-project\data\user.csv", index=False)

# genearete transection 