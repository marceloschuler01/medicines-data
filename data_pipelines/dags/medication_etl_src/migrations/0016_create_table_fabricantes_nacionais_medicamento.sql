CREATE TABLE IF NOT EXISTS fabricantes_nacionais_apresentacao_medicamento
(
 id_apresentacao_medicamento uuid NOT NULL,
 id_fabricante_nacional      uuid NOT NULL,
 CONSTRAINT PK_14 PRIMARY KEY ( id_apresentacao_medicamento, id_fabricante_nacional ),
 CONSTRAINT FK_17 FOREIGN KEY ( id_fabricante_nacional ) REFERENCES fabricante_nacional ( id_fabricante_nacional )  ON DELETE CASCADE,
 CONSTRAINT FK_18 FOREIGN KEY ( id_apresentacao_medicamento ) REFERENCES apresentacao_medicamento ( id_apresentacao_medicamento )  ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_fabricantes_nacionais_apresentacao_id_fabricante ON fabricantes_nacionais_apresentacao_medicamento ( id_fabricante_nacional );

