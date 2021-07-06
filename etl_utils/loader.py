
def write_to_csv(df, file_path, num_partitions=1, header=True, mode="overwrite"):
    df.coalesce(num_partitions).write.csv(file_path, header=header, mode=mode)