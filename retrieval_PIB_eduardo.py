import pandas as pd

# Ejemplo: Leer un dataset de PIB desde una URL (puede ser un CSV en la web)
url = "https://raw.githubusercontent.com/datasets/gdp/master/data/gdp.csv"
df = pd.read_csv(url)

# Filtrar solo países europeos
europe = [
    "Spain", "Germany", "France", "Italy", "Netherlands", "Sweden",
    "Norway", "Switzerland", "Austria", "Belgium", "Denmark", "Finland",
    "Portugal", "Greece", "Ireland", "Poland", "Czech Republic", "Hungary"
]
df_europe = df[df["Country Name"].isin(europe)]

# Mostrar primeras filas
print(df_europe.head())

# Guardar resultado limpio
import os

# Guardar en la misma carpeta que el script
dir_proyecto = os.path.dirname(os.path.abspath(__file__))
ruta_csv = os.path.join(dir_proyecto, "pib_europa.csv")
df_europe.to_csv(ruta_csv, index=False)
print(f"Archivo '{ruta_csv}' guardado correctamente.")
