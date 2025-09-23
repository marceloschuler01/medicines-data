CREATE TABLE IF NOT EXISTS fabricante_internacional
(
 id_fabricante_internacional uuid NOT NULL,
 nome_fabricante             VARCHAR NOT NULL,
 endereco                    VARCHAR NOT NULL,
 pais                        VARCHAR NOT NULL,
 codigo_anvisa               VARCHAR NOT NULL,
 etapa_fabricacao            VARCHAR NOT NULL,
 CONSTRAINT PK_12 PRIMARY KEY ( id_fabricante_internacional )
);
