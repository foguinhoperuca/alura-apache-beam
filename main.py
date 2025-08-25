import argparse
import logging
from pprint import pprint
# Monkey patch!!
import sys
import os.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from apache_beam import CoGroupByKey, CombinePerKey, Filter, FlatMap, Flatten, GroupByKey, Map, Pipeline  # noqa: E501, E402
from apache_beam.io import ReadFromText  # noqa: E402
from apache_beam.io.textio import WriteToText  # noqa: E402
from apache_beam.options.pipeline_options import PipelineOptions  # noqa: E402
import pandas as pd  # noqa: E402

from helper import DENGUE_COLUMNS, LOG_FORMAT_SIMPLE, dengue_process, filter_empty_fields, key_uf, key_uf_year_month_list, list_to_dict, parse_date, prepare_csv, round_me, text_to_list, unpack_elements  # noqa: E501, E402


if __name__ == "__main__":
    print("")
    print("***************************************")
    print("||            APACHE BEAM            ||")
    print("***************************************")
    print("")
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT_SIMPLE)

    parser = argparse.ArgumentParser(description="Geoprocessing Alura!!", epilog="Study")  # noqa: E501
    parser.add_argument("-d", "--dataset", choices=["full", "sample"], help="Choose correct dataset: [full | sample]")  # noqa: E501
    args = parser.parse_args()
    print(f"--- choosed args: {args} :: {args.dataset}")

    dengue_dataset: str = "data/sample_dengue.txt"
    rains_dataset: str = "data/sample_rains.csv"
    if args.dataset == "full":
        dengue_dataset = "data/dengue.txt"
        rains_dataset = "data/rains.csv"

    pipeline_options: PipelineOptions = PipelineOptions(argv=None)
    pipeline: Pipeline = Pipeline(options=pipeline_options)

    dengue = (
        pipeline
        | "Read dengue's dataset" >> ReadFromText(dengue_dataset, skip_header_lines=1)  # noqa: E501
        | "From text to list" >> Map(text_to_list)
        | "From list to dict" >> Map(list_to_dict, DENGUE_COLUMNS)
        | "Parse date" >> Map(parse_date)
        | "Create key by state" >> Map(key_uf)
        | "Group by state" >> GroupByKey()
        | "Unpack dengue's medical case" >> FlatMap(dengue_process)
        | "Sum all medical cases  by key" >> CombinePerKey(sum)
        # | "Show Results" >> Map(pprint)
    )

    rains = (
        pipeline
        | "Read rain's dataset" >> ReadFromText(rains_dataset, skip_header_lines=1)  # noqa: E501
        | "From text to rain's list" >> Map(text_to_list, delimiter=",")
        | "Create special key: UF-YYYY-mm" >> Map(key_uf_year_month_list)
        | "Sum rains total by key" >> CombinePerKey(sum)
        | "Round mm" >> Map(round_me)
        # | "Show rains data" >> Map(pprint)
    )

    result = (
    #     # (rains, dengue)
    #     # | "Stack pcols" >> Flatten()
    #     # | "Group pcols" >> GroupByKey
        ({"rains": rains, "dengue": dengue})
        | "Merge pcols" >> CoGroupByKey()
        | "Filter empty data" >> Filter(filter_empty_fields)
        | "Unpack elements" >> Map(unpack_elements)
        | "Prepare CSV" >> Map(prepare_csv, delimiter=";")
        # | "Show results of union" >> Map(pprint)
    )

    result | "Save it into a file" >> WriteToText("data/final_analyze", file_name_suffix=".csv", header="UF;YEAR;MONTH;RAIN;DENGUE")

    pipeline.run()

    print("=== Analyze ===")
    df = pd.read_csv("data/final_analyze-00000-of-00001.csv", delimiter=";")
    print(df.groupby(["UF", "YEAR"])[["RAIN", "DENGUE"]].mean().reset_index())

    sys.exit(0)
