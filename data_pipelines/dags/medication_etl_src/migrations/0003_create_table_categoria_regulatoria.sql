CREATE TABLE IF NOT EXISTS categoria_regulatoria
(
 id_categoria_regulatoria uuid NOT NULL,
 codigo_anvisa            varchar() NOT NULL,
 descricao                varchar() NOT NULL,
 CONSTRAINT PK_2 PRIMARY KEY ( id_categoria_regulatoria )
);


