import polars as pl
import logging
import argparse

def split_intents_to_two_cols(csv):
    if determine_format(csv)[0] == 1:
        df = pl.read_csv(csv, has_header= False)
        logging.info("WA format detected.")
        df = df.with_columns(
        pl.when(pl.col("column_1").str.starts_with('#'))
        .then(pl.col("column_1"))
        #.shift(1)
        .alias("Intent")
        )
        df = df.with_columns(pl.col("Intent").str.slice(1).forward_fill())
        df_with_two_cols = df.filter(~df['column_1'].str.starts_with('#'))
        #df_with_two_cols = df_with_two_cols[["column_1", "column_2"]]
        return(df_with_two_cols)
    else:
        logging.info("Not WA format.")
    
def split_entities_to_two_cols(csv):
        df = pl.read_csv(csv, has_header= False)
        df = df.with_columns(
        pl.when(pl.col("column_1").str.starts_with('@'))
        .then(pl.col("column_1"))
        #.shift(1)
        .alias("Entity")
        )
        df = df.with_columns(pl.col("Entity").str.slice(1).forward_fill())
        df_with_two_cols = df.filter(~df['column_1'].str.starts_with('@'))
        return(df_with_two_cols)

def replace_entities(intent_csv, entity_csv):
    #this function takes three random sentences for each entity example and replaces the @entity with the example
    flosintents = split_intents_to_two_cols(intent_csv)
    #print(flosintents)
    flosentities = split_entities_to_two_cols(entity_csv)
    #print(flosentities)
    intents_with_entities = flosintents.with_columns(
        pl.col("column_1").str.extract(r"@(\w+)").alias("Entity")
        )
    joined_table = intents_with_entities.join(flosentities, on='Entity', how='inner') # řezničina
    #print(joined_table)
    table = joined_table.with_columns(
        pl.when(
            pl.col("column_1").str.contains("@")
        ).then(
            pl.col("column_1").str.replace(r"@(\w+)", pl.col("column_1_right"))
        ).otherwise(pl.col("column_1")).alias("column_1")
    )
    #print(table)
    short_entities = joined_table.groupby("column_1_right").apply(lambda x: x.sample(3))
    table = short_entities.with_columns(
        pl.concat_str([pl.col("column_1_right"), pl.lit("-"), pl.col("Entity")]).alias("Entity_labeled")
    )
    replacement = pl.concat_str([table["column_1_right"], pl.lit(" ")]).alias("replacement")
    tables = table.with_columns(
        "column_1_right", 
        table["column_1"].str.replace_all(r"@\w+", replacement)
    )
    new_table = tables.select(["column_1", "Entity_labeled"])
    new_table = new_table.rename({"column_1": "prompt", "Entity_labeled":"entity"})
    new_table = new_table.select(pl.all().str.strip())

    return new_table

def mnlu_to_wa(csv):
    intent_df = pl.read_csv(csv, has_header= False)
    grouped_df = intent_df.groupby("column_2", maintain_order=True).all()
    grouped_df

    new_df_list = []

    for row in grouped_df.iter_rows():
        new_df_list.append("#" + row[0])
        for item in row[1]:
            new_df_list.append(item)

    new_df = pl.DataFrame(new_df_list)
    new_df = new_df.unique(maintain_order=True)
    return new_df

def determine_format(csv):
    df = pl.read_csv(csv, has_header= False)
    if df.shape[1] == 1:
        is_wa_file = 1
        if df[0,0].startswith("#"):
            is_intent_file = 1
        else:
            is_intent_file = 0
    else:
        is_wa_file = 0
        if len(df[0,1].split()) == 1:
            is_intent_file = 1
        else:
            is_intent_file = 0
    return(is_wa_file, is_intent_file)

def pipeline(csv):
    if determine_format(csv) == (1,1):
        df = split_intents_to_two_cols(csv)
    elif determine_format(csv) == (1,0):
        df = split_entities_to_two_cols(csv)
    elif determine_format(csv) == (0,1):
        df = mnlu_to_wa(csv)
    return df

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process CSV file(s).')
    parser.add_argument('filename', nargs='+', help='Path to the CSV file(s)')
    args = parser.parse_args()

    if len(args.filename) == 1:
        pipeline(args.filename[0]).write_csv("converted_file.csv", has_header = False)
    elif len(args.filename) == 2:
        replace_entities(args.filename[0], args.filename[1]).write_csv("converted_file.csv", has_header = False)
    else:
        print("Invalid number of CSV files provided. Please provide either one or two CSV files.")