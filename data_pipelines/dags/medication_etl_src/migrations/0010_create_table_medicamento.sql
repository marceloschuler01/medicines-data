CREATE TABLE IF NOT EXISTS medicamento
(
 id_medicamento                  uuid NOT NULL,
 codigo_anvisa                   varchar() NULL,
 codigo_notificacao_anvisa       varchar() NULL,
 nome_comercial                  varchar() NULL,
 numero_registro_anvisa          varchar() NULL,
 numero_processo_anvisa          varchar() NULL,
 tipo_autorizacao_anvisa         varchar() NULL,
 data_registro_anvisa            varchar() NULL,
 data_vencimento_regsitro_anvisa date NULL,
 registro_ativo                  boolean NULL,
 id_categoria_regulatoria        uuid NOT NULL,
 id_empresa                      uuid NOT NULL,
 id_medicamento_referencia       uuid NOT NULL,
 sinonimos                       varchar() NULL,
 indicacoes_de_uso               varchar() NULL,
 CONSTRAINT PK_1 PRIMARY KEY ( id_medicamento ),
 CONSTRAINT FK_1 FOREIGN KEY ( id_categoria_regulatoria ) REFERENCES categoria_regulatoria ( id_categoria_regulatoria ),
 CONSTRAINT FK_15 FOREIGN KEY ( id_medicamento_referencia ) REFERENCES medicamento ( id_medicamento ),
 CONSTRAINT FK_2 FOREIGN KEY ( id_empresa ) REFERENCES empresa ( id_empresa )
);
