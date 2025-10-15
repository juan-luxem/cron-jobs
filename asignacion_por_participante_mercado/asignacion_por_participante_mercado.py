from .get_asignacion_por_participante_mercado import (
    get_asignacion_por_participante_mercado_file,
)
from .process_asignacion_por_participante_mercado import (
    process_asignacion_por_participante_mercado,
)
from global_utils import delete_csv_files_after_process

def get_asignacion_por_participante_mercado():
    get_asignacion_por_participante_mercado_file()
    process_asignacion_por_participante_mercado()
    delete_csv_files_after_process()
