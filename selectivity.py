import pandas as pd

# Assuming df is a DataFrame containing your database table
df = pd.DataFrame({
    'A': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'B': [5, 5, 6, 6, 7, 7, 8, 8, 9, 10]
})

condition_A = (df['A'] > 5)
condition_B = (df['B'] == 6)

selectivity_A = condition_A.sum() / len(df)
selectivity_B = condition_B.sum() / len(df)

# AND condition selectivity
and_selectivity = (condition_A & condition_B).sum() / len(df)

# OR condition selectivity
or_selectivity = (condition_A | condition_B).sum() / len(df)

print(f"Selectivity of condition A: {selectivity_A}")
print(f"Selectivity of condition B: {selectivity_B}")
print(f"Selectivity of condition A AND B: {and_selectivity}")
print(f"Selectivity of condition A OR B: {or_selectivity}")
