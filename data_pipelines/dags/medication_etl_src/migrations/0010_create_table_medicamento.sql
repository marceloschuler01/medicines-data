CREATE TABLE IF NOT EXISTS medicamento
(
 id_medicamento                  uuid NOT NULL,
 codigo_anvisa                   VARCHAR NULL,
 codigo_notificacao_anvisa       VARCHAR NULL,
 nome_comercial                  VARCHAR NULL,
 numero_registro_anvisa          VARCHAR NULL,
 numero_processo_anvisa          VARCHAR NULL,
 tipo_autorizacao_anvisa         VARCHAR NULL,
 data_registro_anvisa            VARCHAR NULL,
 data_vencimento_regsitro_anvisa date NULL,   -- TODO CHANGE FOR REGISTRO (TÁ ESCRITORIO ERRADO)
 registro_ativo                  boolean NULL,
 id_categoria_regulatoria        uuid NULL,
 id_empresa                      uuid NULL,
 id_medicamento_referencia       uuid NULL,
 sinonimos                       VARCHAR NULL,
 indicacoes_de_uso               VARCHAR NULL,
 CONSTRAINT PK_1 PRIMARY KEY ( id_medicamento ),
 CONSTRAINT FK_1 FOREIGN KEY ( id_categoria_regulatoria ) REFERENCES categoria_regulatoria ( id_categoria_regulatoria ),
 CONSTRAINT FK_15 FOREIGN KEY ( id_medicamento_referencia ) REFERENCES medicamento ( id_medicamento ),
 CONSTRAINT FK_2 FOREIGN KEY ( id_empresa ) REFERENCES empresa ( id_empresa )
);
CREATE INDEX IF NOT EXISTS idx_id_medicamento_referencia ON medicamento ( id_medicamento_referencia )
;
CREATE INDEX IF NOT EXISTS idx_id_categoria_regulatoria ON medicamento ( id_categoria_regulatoria )
;
CREATE INDEX IF NOT EXISTS idx_id_empresa ON medicamento ( id_empresa )
;
