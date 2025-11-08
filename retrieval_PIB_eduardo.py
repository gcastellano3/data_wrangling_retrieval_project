import pandas as pd
import os

# Leer dataset de PIB
url = "https://raw.githubusercontent.com/datasets/gdp/master/data/gdp.csv"
df = pd.read_csv(url)

# Filtrar solo países europeos
europe = [
    "Spain", "Germany", "France", "Italy", "Netherlands", "Sweden",
    "Norway", "Switzerland", "Austria", "Belgium", "Denmark", "Finland",
    "Portugal", "Greece", "Ireland", "Poland", "Czech Republic", "Hungary"
]
df_europe = df[df["Country Name"].isin(europe)].copy()

# Convertir 'Value' a euros (ejemplo: 1 USD ≈ 0.93 EUR, puedes ajustar según el año si quieres exactitud histórica)
USD_to_EUR = 0.93
df_europe["Value_EUR"] = df_europe["Value"] * USD_to_EUR

# Opcional: agregar columna en miles de millones para visualización o análisis más cómodo
df_europe["Value_EUR_billions"] = df_europe["Value_EUR"] / 1_000_000_000

# Mostrar primeras filas
print(df_europe.head())

# Guardar resultado limpio
dir_proyecto = os.path.dirname(os.path.abspath(__file__))
ruta_csv = os.path.join(dir_proyecto, "pib_europa.csv")
df_europe.to_csv(ruta_csv, index=False)
print(f"Archivo '{ruta_csv}' guardado correctamente.")

# Mostrar tipos de datos y primeras filas de 'Value_EUR'
print(df_europe.dtypes)
print(df_europe["Value_EUR"].head())