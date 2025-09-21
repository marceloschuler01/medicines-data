CREATE TABLE IF NOT EXISTS classe_terapeutica_medicamento 
(
 id_classe_terapeutica uuid NOT NULL,
 id_medicamento        uuid NOT NULL,
 CONSTRAINT PK_16 PRIMARY KEY ( id_classe_terapeutica, id_medicamento ),
 CONSTRAINT FK_13 FOREIGN KEY ( id_classe_terapeutica ) REFERENCES classe_terapeutica ( id_classe_terapeutica ),
 CONSTRAINT FK_14 FOREIGN KEY ( id_medicamento ) REFERENCES medicamento ( id_medicamento )
);


