CREATE TABLE medicamentos (
    id SERIAL PRIMARY KEY,
    nome VARCHAR,
    codigo_laboratoria VARCHAR,
    nome_laboratorio VARCHAR,
    tipo_produto VARCHAR,
    categoria_regulatoria VARCHAR,
    numero_registro INTEGER,
    principio_ativo VARCHAR,
    dosagem_principio_ativo VARCHAR,
    classe_terapeutica VARCHAR,
    forma_administracao VARCHAR,
    volume_embalagem NUMERIC,
    preco NUMERIC
);
