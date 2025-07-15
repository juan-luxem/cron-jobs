import json

# Load the JSON file
with open("demanda.json", "r") as file:
    data = json.load(file)

# Generate the SQL query
table_name = "Demanda"  # Replace with your actual table name
columns = [
    "FechaOperacion",
    "HoraOperacion",
    "Demanda",
    "Generacion",
    "Enlace",
    "Pronostico",
    "Gerencia",
    "Sistema",
    "FechaCreacion",
    "FechaModificacion",
]

values = []
for record in data:
    row = []
    for column in columns:
        value = record[column]
        if value is None:
            row.append("NULL")
        elif isinstance(value, str):
            row.append(f"'{value}'")
        else:
            row.append(str(value))
    values.append(f"({', '.join(row)})")

query = (
    f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES\n"
    + ",\n".join(values)
    + ";"
)

# Output the query

# Write the query to a SQL file
with open("insert_data.sql", "w") as sql_file:
    sql_file.write(query)
