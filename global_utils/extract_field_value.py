from bs4 import BeautifulSoup, Tag


def extract_field_value(soup: BeautifulSoup, field_name: str, html_element: str) -> str:
    """
    Extrae el valor de un campo específico dentro de un formulario HTML.

    Parámetros:
    - soup: Objeto BeautifulSoup con la página analizada.
    - field_name: Nombre del campo HTML del que se extraerá el valor.
    - html_element: Tipo de etiqueta HTML donde se encuentra el campo.

    Retorna:
    - Valor del campo codificado en URL para su uso en una solicitud POST.
    """
    element_tag = soup.find(html_element, {"name": field_name})
    element = element_tag.get("value", None) if isinstance(element_tag, Tag) else None
    return str(element)
