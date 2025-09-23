
CREATE TABLE IF NOT EXISTS empresa
(
 id_empresa                uuid NOT NULL,
 numero_autorizacao_anvisa VARCHAR NOT NULL,
 cnpj                      VARCHAR NOT NULL,
 razao_social              VARCHAR NOT NULL,
 CONSTRAINT PK_3 PRIMARY KEY ( id_empresa )
);
