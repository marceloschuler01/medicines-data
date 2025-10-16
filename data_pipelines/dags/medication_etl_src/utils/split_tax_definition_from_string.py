import re


def split_tax_definition_from_string(texto):
    # Caso especial: sem impostos
    if "Sem Impostos" in texto:
        tipo = texto.strip()
        porcentagem = 0
        return tipo, porcentagem

    # Expressão regular para capturar valor percentual
    match = re.search(r'(\d+(?:,\d+)?)\s*%', texto)

    if match:
        porcentagem = float(match.group(1).replace(',', '.'))
        # Remove o número e o símbolo de % para capturar o tipo
        tipo_parte = re.sub(r'\d+(?:,\d+)?\s*%', '', texto).strip()
        tipo = tipo_parte.replace('  ', ' ')
    else:
        # Se não tiver porcentagem, o valor é 0
        tipo = texto.strip()
        porcentagem = 0

    # Normalização do tipo (PF, PMC, ALC, etc.)
    partes = tipo.split()
    if len(partes) > 1:
        if partes[-1] == 'ALC':
            tipo = f"{partes[0]} ALC"
        else:
            tipo = partes[0]
    else:
        tipo = partes[0]

    return tipo, porcentagem
