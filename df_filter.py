import pandas as pd
from threading import Thread
import os
import yaml
import warnings
from typing import List
import logging
from tqdm import tqdm

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


def thread_task(filename: str, dfs: dict[str, pd.DataFrame], pbar) -> None:
    logging.info(f"{filename} started")
    df = pd.read_excel(filename, header=7).dropna(axis=1, how="all")
    for ville, main_df in dfs.items():
        main_df = pd.concat(
            [
                main_df,
                df[
                    df["Code INSEE commune logt souhaité"].isin(
                        codes_geographiques[f"liste_code_geographique_{ville}"]
                    )
                ],
            ],
            axis=0,
            ignore_index=True,
        )
        pbar.update(1)
    logging.info(f"{filename} ended")


def main(path_to_excel_files_dir: str, codes_geographiques: dict[str, List[str]]):

    files = [
        "/".join([path_to_excel_files_dir, el])
        for el in os.listdir(path_to_excel_files_dir)
        if not el.startswith(("."))
    ]

    files = files[:2]
    with tqdm(total=3 * len(files), ascii="░▒█") as pbar:

        df = pd.read_excel(files[0], header=7).dropna(axis=1, how="all")

        global villes
        villes = ["Paris", "Marseille", "Lyon"]
        df_paris, df_Marseille, df_lyon = (
            df[
                df["Code INSEE commune logt souhaité"].isin(
                    codes_geographiques[f"liste_code_geographique_{ville}"]
                )
            ]
            for ville in villes
        )
        dfs = {"Paris": df_paris, "Marseille": df_Marseille, "Lyon": df_lyon}

        pbar.update(3)

        threads = []

        for excel_file in files[1:]:
            threads.append(Thread(target=thread_task, args=(excel_file, dfs, pbar)))
            threads[-1].start()

        for thread in threads:
            thread.join()

    return dfs


if __name__ == "__main__":

    path_to_excel_files_dir: str = (
        "/Users/arceb/Desktop/Stage/fichiers_demandeurs_complets_ministere"
    )

    path_to_config_file: str = "config.yml"

    with open("config.yml") as f:
        codes_geographiques = yaml.safe_load(f)

    os.chdir("../../..")  # Depends on the current localisation

dfs = main(
    path_to_excel_files_dir=path_to_excel_files_dir,
    codes_geographiques=codes_geographiques,
)

for key, df in dfs.items():
    df.to_csv(f"/Users/arceb/PycharmProjects/pythonProject/{key}_.csv")
