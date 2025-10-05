CREATE TABLE IF NOT EXISTS composicao_apresentacao_medicamento
(
 id_principio_ativo          uuid NOT NULL,
 id_apresentacao_medicamento uuid NOT NULL,
 dosagem                     decimal,
 unidade_de_medida           varchar,
 CONSTRAINT PK_17 PRIMARY KEY ( id_principio_ativo, id_apresentacao_medicamento ),
 CONSTRAINT FK_6 FOREIGN KEY ( id_principio_ativo ) REFERENCES principio_ativo ( id_principio_ativo ),
 CONSTRAINT FK_7 FOREIGN KEY ( id_apresentacao_medicamento ) REFERENCES apresentacao_medicamento ( id_apresentacao_medicamento ) ON DELETE CASCADE
);


