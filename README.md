# Spark Merge & Dedupe 
Este proyecto une dos DataFrames de transacciones (dos CSVs), limpia duplicados
y mantiene **solo el registro más reciente por ID**. El resultado final debe contener
**3 registros (IDs 101, 102 y 103)**.

> Nota sobre las fechas `2323-...`: se dejan como en el enunciado. Si deseas tratar años fuera de rango como errores,
activa la bandera `--strict-date-validation` para invalidar años > 2100 y así evitar que ganen como "más recientes".

## Estructura
```
spark-merge-dedup/
├── data/
│   ├── df1.csv
│   └── df2.csv
├── src/
│   └── merge_transactions.py
├── requirements.txt
└── README.md
```

## Requisitos
- Python 3.9+
- PySpark 3.4+

Instalación rápida (opcional, si no tienes PySpark):
```bash
pip install -r requirements.txt
```


## Cómo ejecutar 

1) **Clona o descarga** este repositorio 
2) Sitúate en la carpeta del proyecto:
```bash
cd spark-merge-dedup
```
3) Ejecuta el script indicando los archivos de entrada y la carpeta de salida:
```bash
python src/merge_transactions.py   --input1 data/df1.csv   --input2 data/df2.csv   --output data/out   --strict-date-validation false
```


### Modos alternativos
- Usar `spark-submit`:
```bash
spark-submit src/merge_transactions.py   --input1 data/df1.csv --input2 data/df2.csv --output data/out
```

- Activar validación estricta de fechas (años > 2100 se invalidan para no "ganar"):
```bash
python src/merge_transactions.py   --input1 data/df1.csv --input2 data/df2.csv --output data/out   --strict-date-validation true
```

## Lógica de depuración y deduplicación
1. **Lectura** de ambos CSV con esquema explícito.
2. **Estandarización** de tipos (`Date` a tipo fecha, `Amount` a decimal).
3. **Drop de duplicados exactos** por (`ID`, `Date`, `Amount`, `Currency`).
4. **Ventana por `ID`** ordenando por `Date` descendente y, a igualdad de fecha, por `Amount` descendente (nulos al final).
5. **Elegir el `row_number = 1`** ⇒ último registro por `ID`.


## Resultado esperado con los CSV de ejemplo
- **ID 101** ⇒ Gana el registro con fecha `2323-01-10` (o `2023-01-02` si activas validación estricta).
- **ID 102** ⇒ `2023-01-07` (más reciente que `2023-01-05`).
- **ID 103** ⇒ `2323-01-15` (o quedaría **sin registro** si invalidas >2100 y no aportas otra fecha).
