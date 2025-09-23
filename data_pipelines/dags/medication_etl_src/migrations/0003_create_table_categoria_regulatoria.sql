CREATE TABLE IF NOT EXISTS categoria_regulatoria
(
 id_categoria_regulatoria uuid NOT NULL,
 codigo_anvisa            VARCHAR NOT NULL,
 descricao                VARCHAR NOT NULL,
 CONSTRAINT PK_2 PRIMARY KEY ( id_categoria_regulatoria )
);


