-- Frutas
CREATE TABLE lista_frutas (
    nm_fruta TEXT ,
    tp_fruta TEXT )
;
INSERT INTO lista_frutas VALUES ('banana','doce');
INSERT INTO lista_frutas VALUES ('limão','amargo');
INSERT INTO lista_frutas VALUES ('mexerica','doce');
INSERT INTO lista_frutas VALUES ('maçã','doce');

-- Corrige o cadastro
UPDATE lista_frutas
   SET nm_fruta = 'bergamota'
     , tp_fruta = 'azedo'
 WHERE nm_fruta = 'mexerica';

-- Tipos
CREATE TABLE tipo_frutas (
    tp_fruta TEXT,
    vl_fruta FLOAT);

INSERT INTO tipo_frutas VALUES ('doce',1.5);
INSERT INTO tipo_frutas VALUES ('azedo',2.3);
INSERT INTO tipo_frutas VALUES ('amargo',2.0);

-- Atualiza o valor das frutas azedas
UPDATE tipo_frutas
   SET vl_fruta = 2.5
 WHERE tp_fruta = 'azedo';

-- Vendas
CREATE TABLE venda_frutas (
    cod_venda INTEGER,
    nm_fruta TEXT,
    qtd_venda INTEGER);
INSERT INTO venda_frutas VALUES (1,'banana',10);
INSERT INTO venda_frutas VALUES (2,'banana',12);
INSERT INTO venda_frutas VALUES (3,'limão',5);
INSERT INTO venda_frutas VALUES (4,'banana',8);
INSERT INTO venda_frutas VALUES (5,'limão',8);

-- Altera o tipo da fruta 'limão' para 'azedo'
UPDATE lista_frutas
   SET tp_fruta = 'azedo'
 WHERE nm_fruta = 'limão';

-- Remove a 'bergamota' da lista de frutas :(
DELETE FROM lista_frutas 
 WHERE nm_fruta = 'bergamota';

-- Altera a quantidade vendida da venda 3 para 20
UPDATE venda_frutas
   SET qtd_venda = 20
 WHERE cod_venda = 3;

-- Salva todas as alterações
COMMIT;
