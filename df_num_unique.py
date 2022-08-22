import logging
from multiprocessing import Pool, cpu_count
import os
import warnings
from typing import List

import pandas as pd
import yaml
from tqdm import tqdm

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


def thread_task(filename: str) -> List[pd.DataFrame]:

    with open(
        "/Users/arceb/PycharmProjects/pythonProject/config-2.yml"
    ) as f:
        num_uniques = yaml.safe_load(f)

        logging.info(f"{filename} started")
        df = pd.read_excel(filename, sheet_name="Ressources demandeur", header=7).dropna(axis=1, how="all")
    return df[df["Numéro Unique"].isin(num_uniques[f"liste"])]


def main(path_to_excel_files_dir: str):

    files = [
        "/".join([path_to_excel_files_dir, el])
        for el in os.listdir(path_to_excel_files_dir)
        if not el.startswith(("."))
    ]

    files = files[:24]
    _dfs = []
    with Pool(cpu_count() - 1) as p:
        with tqdm(total=len(files)) as pbar:
            for el in p.imap_unordered(thread_task, files[:]):
                _dfs.append(el)
                pbar.update()

    for x in _dfs:
        try:
            dfd = pd.concat((dfd, x), axis=0, ignore_index=True)


        except:
            dfd = x


    return {"Revenus_Marseille": dfd}


if __name__ == "__main__":

    path_to_excel_files_dir: str = (
        "/Users/arceb/Desktop/Stage/fichiers_demandeurs_complets_ministere"
    )

    os.chdir("../../..")  # Depends on the current localisation

    dfs = main(
        path_to_excel_files_dir=path_to_excel_files_dir,
    )

    for key, dfd in dfs.items():
        dfd.to_csv(f"/Users/arceb/PycharmProjects/pythonProject/{key}_.csv")