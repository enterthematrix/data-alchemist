import random
import pandas as pd
from faker import Faker

f = Faker()
output = [[f.name(), f.country(), f.city(), f.state(), random.randrange(10000, 99999)]
    for _ in range(10000)]

df = pd.DataFrame(output, columns=["name", "country", "city", "state", "zip"])
print(df.head())