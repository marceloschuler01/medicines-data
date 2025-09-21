CREATE TABLE IF NOT EXISTS apresentacao_medicamento
(
 id_apresentacao_medicamento uuid NOT NULL,
 codigo_anvisa               varchar() NOT NULL,
 apresentacao                varchar() NOT NULL,
 volume_total_em_ml          decimal NOT NULL,
 via_administracao           varchar() NULL,
 tipo_autorizacao_anvisa     varchar() NULL,
 registro_ativo              boolean NOT NULL,
 regime_preco                 NULL,
 id_medicamento              uuid NOT NULL,
 ggrem                       varchar() NULL,
 ean_gtin                    varchar() NULL,
 ean_2                       varchar() NULL,
 CONSTRAINT PK_4 PRIMARY KEY ( id_apresentacao_medicamento ),
 CONSTRAINT FK_12_1 FOREIGN KEY ( id_medicamento ) REFERENCES medicamento ( id_medicamento )
);


