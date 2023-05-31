import pandas as pd

# Read the Excel files
mex_file = "MEX.xlsx"
arg_file = "ARG.xlsx"

# Load the data into dataframes
mex_df = pd.read_excel(mex_file)
arg_df = pd.read_excel(arg_file)

# Trim columns and remove signs or empty spaces
mex_df = mex_df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
arg_df = arg_df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

# Print column names
print("Columns in MEX.xlsx:")
print(mex_df.columns.tolist())
print()
print("Columns in ARG.xlsx:")
print(arg_df.columns.tolist())
print()

# Print first 5 lines of Referencia column for MEX.xlsx
print("First 5 lines of Referencia column in MEX.xlsx:")
print(mex_df["Referencia"].head(5))
print()

# Print first 5 lines of Factura column for ARG.xlsx
print("First 5 lines of Factura column in ARG.xlsx:")
print(arg_df["Factura"].head(5))
print()

# Look for all unique values in Referencia column of MEX.xlsx
mex_referencia_values = mex_df["Referencia"].unique()

# Calculate the sum of absolute values in "Importe en moneda doc." for each unique value in MEX.xlsx
mex_sum_results = []
for referencia_value in mex_referencia_values:
    mex_referencia_filter = mex_df["Referencia"] == referencia_value
    mex_filtered_df = mex_df[mex_referencia_filter]
    mex_sum = mex_filtered_df["Importe en moneda doc."].abs().sum()
    mex_sum_results.append((referencia_value, mex_sum))

# Print the results and the corresponding appearances for MEX.xlsx
print("Results for MEX.xlsx:")
for referencia_value, sum_value in mex_sum_results:
    print(f"Referencia: {referencia_value}, Sum of absolute values: {sum_value}")
    print("Appearances:")
    print(mex_df.loc[mex_df["Referencia"] == referencia_value, ["Referencia", "Importe en moneda doc."]])
    print()

# Create a dataframe with the sum results for MEX.xlsx
resulting_mex = pd.DataFrame(mex_sum_results, columns=["Referencia", "Sum of absolute values"])

# Look for all unique values in Factura column of ARG.xlsx
arg_factura_values = arg_df["Factura"].unique()

# Calculate the sum of absolute values in "Importe en moneda doc." for each unique value in ARG.xlsx
arg_sum_results = []
for factura_value in arg_factura_values:
    arg_factura_filter = arg_df["Factura"] == factura_value
    arg_filtered_df = arg_df[arg_factura_filter]
    arg_sum = arg_filtered_df["Importe en moneda doc."].abs().sum()
    arg_sum_results.append((factura_value, arg_sum))

# Print the results and the corresponding appearances for ARG.xlsx
print("Results for ARG.xlsx:")
for factura_value, sum_value in arg_sum_results:
    print(f"Factura: {factura_value}, Sum of absolute values: {sum_value}")
    print("Appearances:")
    print(arg_df.loc[arg_df["Factura"] == factura_value, ["Factura", "Importe en moneda doc."]])
    print()

# Create a dataframe with the sum results for ARG.xlsx
resulting_arg = pd.DataFrame(arg_sum_results, columns=["Factura", "Sum of absolute values"])

# Find matching values between resulting_mex and resulting_arg
matching_values = set(resulting_mex["Referencia"]) & set(resulting_arg["Factura"])

# Print matching results in a pretty way
print("Matching results:")
for value in matching_values:
    mex_sum = resulting_mex.loc[resulting_mex["Referencia"] == value, "Sum of absolute values"].values[0]
    arg_sum = resulting_arg.loc[resulting_arg["Factura"] == value, "Sum of absolute values"].values[0]
    print(f"Referencia: {value}, Sum for MEX.xlsx: {mex_sum}, Sum for ARG.xlsx: {arg_sum}")
    print()

# Find non-matching values between resulting_mex and resulting_arg
non_matching_values = set(resulting_mex["Referencia"]) ^ set(resulting_arg["Factura"])

# Print non-matching results in a pretty way
print("Non-matching results:")
for value in non_matching_values:
    if value in resulting_mex["Referencia"].values:
        mex_sum = resulting_mex.loc[resulting_mex["Referencia"] == value, "Sum of absolute values"].values[0]
        print(f"Referencia: {value}, Sum for MEX.xlsx: {mex_sum}, No corresponding value in ARG.xlsx")
    else:
        arg_sum = resulting_arg.loc[resulting_arg["Factura"] == value, "Sum of absolute values"].values[0]
        print(f"Factura: {value}, No corresponding value in MEX.xlsx, Sum for ARG.xlsx: {arg_sum}")
    print()

# Find matching results with the same sum of absolute values
matching_results_same_sum = resulting_mex.merge(resulting_arg, left_on="Referencia", right_on="Factura", suffixes=["_MEX", "_ARG"])
matching_results_same_sum_ref = resulting_mex.merge(resulting_arg, left_on="Referencia", right_on="Factura", suffixes=["_MEX", "_ARG"])
matching_results_same_sum = matching_results_same_sum[matching_results_same_sum["Sum of absolute values_MEX"] == matching_results_same_sum["Sum of absolute values_ARG"]]

# Print matching results with the same sum of absolute values
print("Matching results with the same sum of absolute values:")
print(matching_results_same_sum[["Referencia", "Sum of absolute values_MEX", "Factura", "Sum of absolute values_ARG"]])
print()

# Find matching results with a difference greater than 1.0 in the sum of absolute values
matching_results_diff_gt_1 = matching_results_same_sum_ref[
    abs(matching_results_same_sum_ref["Sum of absolute values_MEX"] - matching_results_same_sum_ref["Sum of absolute values_ARG"]) > 1.0
]

# Print matching results with a difference greater than 1.0 in the sum of absolute values
print("Matching results with a difference greater than 1.0 in the sum of absolute values:")
print(matching_results_diff_gt_1[["Referencia", "Sum of absolute values_MEX", "Factura", "Sum of absolute values_ARG"]])
print()

# Find matching results with a difference less than 1.0 in the sum of absolute values
matching_results_diff_lt_1 = matching_results_same_sum_ref[
    abs(matching_results_same_sum_ref["Sum of absolute values_MEX"] - matching_results_same_sum_ref["Sum of absolute values_ARG"]) < 1.0
]

# Print matching results with a difference less than 1.0 in the sum of absolute values
print("Matching results with a difference less than 1.0 in the sum of absolute values:")
print(matching_results_diff_lt_1[["Referencia", "Sum of absolute values_MEX", "Factura", "Sum of absolute values_ARG"]])
print()
