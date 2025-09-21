CREATE TABLE IF NOT EXISTS fabricante_nacional
(
 id_fabricante_nacional uuid NOT NULL,
 nome                   varchar() NOT NULL,
 cnpj                   varchar() NOT NULL,
 uf                     varchar() NOT NULL,
 cidade                 varchar() NOT NULL,
 etapa_fabricacao       varchar() NOT NULL,
 CONSTRAINT PK_13 PRIMARY KEY ( id_fabricante_nacional )
);


