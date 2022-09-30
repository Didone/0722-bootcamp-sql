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

 COMMIT;