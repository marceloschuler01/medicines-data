
CREATE TABLE IF NOT EXISTS empresa
(
 id_empresa                uuid NOT NULL,
 numero_autorizacao_anvisa varchar() NOT NULL,
 cnpj                      varchar() NOT NULL,
 razao_social              varchar() NOT NULL,
 CONSTRAINT PK_3 PRIMARY KEY ( id_empresa )
);
