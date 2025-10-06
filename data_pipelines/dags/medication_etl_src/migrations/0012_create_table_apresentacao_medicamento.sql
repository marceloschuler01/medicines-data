CREATE TABLE IF NOT EXISTS apresentacao_medicamento
(
 id_apresentacao_medicamento uuid NOT NULL,
 codigo_anvisa               VARCHAR NOT NULL,
 apresentacao                VARCHAR NOT NULL,
 quantidade                  VARCHAR NULL,
 via_administracao           VARCHAR NULL,
 tipo_autorizacao_anvisa     VARCHAR NULL,
 registro_ativo              boolean NOT NULL,
 regime_preco                VARCHAR NULL,
 id_medicamento              uuid NOT NULL,
 ggrem                       VARCHAR NULL,
 ean_gtin                    VARCHAR NULL,
 ean_2                       VARCHAR NULL,
 CONSTRAINT PK_4 PRIMARY KEY ( id_apresentacao_medicamento ),
 CONSTRAINT FK_12_1 FOREIGN KEY ( id_medicamento ) REFERENCES medicamento ( id_medicamento ) ON DELETE CASCADE
);


