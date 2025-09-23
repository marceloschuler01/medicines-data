CREATE TABLE IF NOT EXISTS fabricante_nacional
(
 id_fabricante_nacional uuid NOT NULL,
 nome                   VARCHAR NOT NULL,
 cnpj                   VARCHAR NOT NULL,
 uf                     VARCHAR NOT NULL,
 cidade                 VARCHAR NOT NULL,
 etapa_fabricacao       VARCHAR NOT NULL,
 CONSTRAINT PK_13 PRIMARY KEY ( id_fabricante_nacional )
);


