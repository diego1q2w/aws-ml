import pandas as pd
import os
import glob


class JoinFiles:

    def run(self, path, name):
        command = "cd " + path
        os.system(command)
        # Leemos archivos y ponemos en lista
        all_files = glob.glob(os.path.join(path, "*.csv"))
        # Convertimos a dataframe
        df_files = (pd.read_csv(file) for file in all_files)
        # Concatenamos archivos con Pandas
        concat_file = pd.concat(df_files, ignore_index = True)
        # Exportamos a archivo
        file_name = os.path.join(path, name)
        concat_file.to_csv(file_name, index = False)
        # Borramos archivos chiquitos
        command = "rm " + " ".join(map(str, all_files))
        os.system(command)

