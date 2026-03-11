CREATE TABLE IF NOT EXISTS preco_maximo_apresentacao_medicamento
(
 id_aliquota_imposto         uuid NOT NULL,
 id_tipo_preco_maximo        uuid NOT NULL,
 id_apresentacao_medicamento uuid NOT NULL,
 valor_maximo                decimal(12,2) NOT NULL,
 CONSTRAINT PK_18 PRIMARY KEY ( id_aliquota_imposto, id_tipo_preco_maximo, id_apresentacao_medicamento ),
 CONSTRAINT FK_12 FOREIGN KEY ( id_aliquota_imposto ) REFERENCES aliquota_imposto ( id_aliquota_imposto ) ON DELETE CASCADE,
 CONSTRAINT FK_13_1 FOREIGN KEY ( id_tipo_preco_maximo ) REFERENCES tipo_preco_maximo ( id_tipo_preco_maximo ) ON DELETE CASCADE,
 CONSTRAINT FK_14_1 FOREIGN KEY ( id_apresentacao_medicamento ) REFERENCES apresentacao_medicamento ( id_apresentacao_medicamento ) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_preco_maximo_apresentacao_id_apresentacao ON preco_maximo_apresentacao_medicamento ( id_apresentacao_medicamento );
CREATE INDEX IF NOT EXISTS idx_preco_maximo_apresentacao_id_tipo_preco ON preco_maximo_apresentacao_medicamento ( id_tipo_preco_maximo );
CREATE INDEX IF NOT EXISTS idx_preco_maximo_apresentacao_id_aliquota ON preco_maximo_apresentacao_medicamento ( id_aliquota_imposto );
