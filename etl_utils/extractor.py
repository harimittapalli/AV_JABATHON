
def read_csv(spark, file_path, header=False, *args, **kwargs):

    df = spark.read.load(file_path,
                         format='CSV',
                         header=header)
    return df

def read_json(spark, file_path, header=False, multiline=False, *args, **kwargs):

    df = spark.read.load(file_path,
                         format='JSON',
                         header=header,
                         multiline=multiline)
    return df
