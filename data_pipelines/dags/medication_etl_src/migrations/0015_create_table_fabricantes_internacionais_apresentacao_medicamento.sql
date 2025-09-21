CREATE TABLE IF NOT EXISTS fabricantes_internacionais_apresentacao_medicamento
(
 id_fabricante_internacional uuid NOT NULL,
 id_apresentacao_medicamento uuid NOT NULL,
 CONSTRAINT PK_13_1 PRIMARY KEY ( id_fabricante_internacional, id_apresentacao_medicamento ),
 CONSTRAINT FK_15_1 FOREIGN KEY ( id_apresentacao_medicamento ) REFERENCES apresentacao_medicamento ( id_apresentacao_medicamento ),
 CONSTRAINT FK_16 FOREIGN KEY ( id_fabricante_internacional ) REFERENCES fabricante_internacional ( id_fabricante_internacional )
);


