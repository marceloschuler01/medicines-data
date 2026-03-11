CREATE TABLE IF NOT EXISTS embalagem_apresentacao_medicamento
(
 id_embalagem_medicamento    uuid NOT NULL,
 primaria                    boolean NOT NULL,
 tipo                        VARCHAR NOT NULL,
 observacao                  VARCHAR NULL,
 id_apresentacao_medicamento uuid NOT NULL,
 CONSTRAINT PK_5 PRIMARY KEY ( id_embalagem_medicamento ),
 CONSTRAINT FK_3 FOREIGN KEY ( id_apresentacao_medicamento ) REFERENCES apresentacao_medicamento ( id_apresentacao_medicamento )
);
CREATE INDEX IF NOT EXISTS idx_embalagem_apresentacao_id_apresentacao ON embalagem_apresentacao_medicamento ( id_apresentacao_medicamento );

