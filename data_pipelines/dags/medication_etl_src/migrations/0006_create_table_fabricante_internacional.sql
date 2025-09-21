CREATE TABLE IF NOT EXISTS fabricante_internacional
(
 id_fabricante_internacional uuid NOT NULL,
 nome_fabricante             varchar() NOT NULL,
 endereco                    varchar() NOT NULL,
 pais                        varchar() NOT NULL,
 codigo_anvisa               varchar() NOT NULL,
 etapa_fabricacao            varchar() NOT NULL,
 CONSTRAINT PK_12 PRIMARY KEY ( id_fabricante_internacional )
);
