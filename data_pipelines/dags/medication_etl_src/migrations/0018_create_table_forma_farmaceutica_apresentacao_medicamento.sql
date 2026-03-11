CREATE TABLE IF NOT EXISTS forma_farmaceutica_apresentacao_medicamento
(
 id_apresentacao_medicamento uuid NOT NULL,
 id_forma_farmaceutica       uuid NOT NULL,
 CONSTRAINT PK_18 PRIMARY KEY ( id_apresentacao_medicamento, id_forma_farmaceutica ),
 CONSTRAINT FK_18_1 FOREIGN KEY ( id_apresentacao_medicamento ) REFERENCES apresentacao_medicamento ( id_apresentacao_medicamento ) ON DELETE CASCADE,
 CONSTRAINT FK_18_2 FOREIGN KEY ( id_forma_farmaceutica ) REFERENCES forma_farmaceutica ( id_forma_farmaceutica ) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_forma_farmaceutica_apresentacao_id_apresentacao ON forma_farmaceutica_apresentacao_medicamento ( id_apresentacao_medicamento );
