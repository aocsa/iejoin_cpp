import pyarrow.parquet as pq
import pyarrow as pa

df = pq.read_table('/Users/aocsa/git/iejoin/employees.parquet').to_pandas()

df = df.head(100000)
df = df.sample(frac=1).reset_index(drop=True)

print ("len(df): ", len(df))


# Get the number of rows in each row group
rows_per_group = len(df) // 1000
print ("rows_per_group(df): ", rows_per_group)

# Create a PyArrow table from the DataFrame
table = pa.table(df)


# Open a Parquet writer
with pq.ParquetWriter('/Users/aocsa/git/iejoin/employees1000k.parquet', table.schema) as writer:
    start = 0
    for i in range(100):
        end = start + rows_per_group    
        # Write a slice of the DataFrame as a new row group
        writer.write_table(table.slice(start, rows_per_group))
        start = end
        
        
        
import pandas as pd
# df = pd.read_csv('/Users/aocsa/git/iejoin/employees10k.csv')
# df = df.sample(frac=1).reset_index(drop=True)
print("CSV len ", len(df))
df.to_csv('/Users/aocsa/git/iejoin/new_employees1000k.csv', index=False)
