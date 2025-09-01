SIN = [
    "Central",
    "Noreste",
    "Noroeste",
    "Norte",
    "Occidental",
    "Oriental",
    "Peninsular",
]
BCS = ["Baja California Sur"]
BCA = ["Baja California"]

def get_system(gerencia: str) -> str:
    # Verificar si la gerencia pertenece al sistema Baja California (BCA)
    if gerencia in BCA:
        return "BCA"

    # Verificar si la gerencia pertenece al sistema Baja California Sur (BCS)
    if gerencia in BCS:
        return "BCS"

    # Verificar si la gerencia pertenece al Sistema Interconectado Nacional (SIN)
    if gerencia in SIN:
        return "SIN"

    # Retornar string vacío si la gerencia no pertenece a ningún sistema conocido
    return ""
