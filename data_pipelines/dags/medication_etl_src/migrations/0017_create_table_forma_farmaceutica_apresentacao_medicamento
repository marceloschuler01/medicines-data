CREATE TABLE IF NOT EXISTS preco_maximo_apresentacao_medicamento
(
 id_aliquota_imposto         uuid NOT NULL,
 id_tipo_preco_maximo        uuid NOT NULL,
 id_apresentacao_medicamento uuid NOT NULL,
 valor_maximo                decimal(12,2) NOT NULL,
 CONSTRAINT PK_18 PRIMARY KEY ( id_aliquota_imposto, id_tipo_preco_maximo, id_apresentacao_medicamento ),
 CONSTRAINT FK_12 FOREIGN KEY ( id_aliquota_imposto ) REFERENCES aliquota_imposto ( id_aliquota_imposto ),
 CONSTRAINT FK_13_1 FOREIGN KEY ( id_tipo_preco_maximo ) REFERENCES tipo_preco_maximo ( id_tipo_preco_maximo ),
 CONSTRAINT FK_14_1 FOREIGN KEY ( id_apresentacao_medicamento ) REFERENCES apresentacao_medicamento ( id_apresentacao_medicamento )
);


