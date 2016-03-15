
-- Contagem de algumas coisas importantes do projeto:
SELECT year(dt), month(dt), sum(trans_BUE) FROM analise_evolucao_BRT
WHERE dt >= '2014-03-01' and dt < '2015-03-01'
GROUP BY year(dt), month(dt); -- 470757902

SELECT sum(trans_BUE) FROM analise_evolucao_BRT
WHERE dt >= '2015-03-01' and dt <= '2015-03-31'; -- 47938589

-- Load de tabela de linhas
DROP TABLE linhas_operadoras_modo_BRT;

CREATE TABLE linhas_operadoras_modo(
  `cd_linha` varchar(5) NOT NULL,
  `nr_linha_DETRO` varchar(6) DEFAULT NULL,
  `nm_linha_DETRO` varchar(70) DEFAULT NULL,
  `tp_abrangencia` varchar(1) DEFAULT NULL,
  `cd_operadora` varchar(5) DEFAULT NULL,
  `ds_qualificacao` varchar(22) DEFAULT NULL,
  `nm_razao_social` varchar(70) DEFAULT NULL,
  `tp_modo` varchar(12) DEFAULT NULL,
  `tp_modo_s` varchar(2) DEFAULT NULL,
  `nm_ramal` varchar(25) DEFAULT NULL,
  `nm_linha_consolid` varchar(70) DEFAULT NULL,
  `cd_georef` varchar(25) default NULL,
  KEY linha_index (`cd_linha`)
  ) ENGINE=InnoDB AUTO_INCREMENT=1000 DEFAULT CHARSET=latin1;


TRUNCATE linhas_operadoras_modo;

LOAD DATA INFILE 'C:\\linhas_operadoras_modo_v7.csv' 
  INTO TABLE linhas_operadoras_modo
  FIELDS TERMINATED BY ';' 
  ENCLOSED BY '"'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 ROWS;
  
UPDATE linhas_operadoras_modo SET nm_linha_consolid = NULL WHERE nm_linha_consolid = '';

SELECT * FROM linhas_operadoras_modo limit 10;

-- Etapa 1: DEFINIÇÃO DO PERÍODO DE ANÁLISE
-- 1.1. Tabela de armazenamento da data do mês:
DROP TABLE IF EXISTS analise_BRT_datas;
CREATE TABLE analise_BRT_datas
(
dt DATE,
dia_semana int(1),
PRIMARY KEY (dt)
);

-- 1.2. Inserção das datas e dias:
CREATE PROCEDURE filldates(dateStart date, dateEnd date)
BEGIN
  WHILE dateStart <= dateEnd DO
    INSERT INTO analise_BRT_datas
	VALUES (dateStart, DAYOFWEEK(dateStart));
    SET dateStart = date_add(dateStart, INTERVAL 1 DAY);
  END WHILE;
END;
$$
DELIMITER;

CALL filldates('2015-03-01','2015-03-28');

-- Etapa 2: CRIAÇÃO DA BASE DE DADOS
-- 2.1. Criação da Tabela de armazenamento de Transações:
CREATE TABLE analise_trans_20150301_20150328 (
  cd_operadora varchar(5) NOT NULL,
  cd_linha varchar(5) NOT NULL,
  cd_sentido varchar(1) NOT NULL,
  nr_estac_carro varchar(8) NOT NULL,
  dt_transacao datetime NOT NULL,
  nr_cartao varchar(13) NOT NULL,
  cd_emiss_aplic varchar(2) NOT NULL,
  cd_aplicacao varchar(4) NOT NULL,
  nr_trans_aplic varchar(5) NOT NULL,
  vl_linha int(10) unsigned NOT NULL,
  vl_transacao int(10) unsigned NOT NULL,
  vl_subsidio int(10) unsigned NOT NULL,
  cd_integracao varchar(6) DEFAULT NULL,
  qt_integracoes int(10) unsigned DEFAULT NULL,
  cd_aplicacao_ant varchar(4) DEFAULT NULL,
  nr_trans_aplic_ant varchar(5) DEFAULT NULL,
  nr_detord_subsid varchar(8) NOT NULL,
  nr_valid varchar(6) NOT NULL,
  nr_chip_sam varchar(20) NOT NULL,
  nr_seq_arq varchar(5) NOT NULL,
  nr_trans_sam varchar(6) NOT NULL,
  cd_tp_debito varchar(2) DEFAULT NULL,
  trecho varchar(3) DEFAULT NULL,
  PRIMARY KEY (nr_cartao, dt_transacao, nr_trans_aplic),
  KEY card_data_idx (nr_cartao, dt_transacao),
  KEY card_linha (nr_cartao, cd_linha),
  KEY linha_data (cd_linha, dt_transacao),
  KEY trans_key (nr_cartao, nr_trans_aplic) USING BTREE
  ) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- 2.2. Preenchimento da Tabela de armazenamento de Transações:
-- 2.2.1. Procedure para Preenchimento da Base de Dados de Forma Incremental
CREATE DEFINER=`root`@`localhost` PROCEDURE `fill_database`(dateStart date, dateEnd date)
  BEGIN
    WHILE dateStart <= dateEnd DO

  -- mes anterior
    IF dateStart <= '2015-03-05' THEN
      INSERT INTO analise_trans_20150301_20150328
      SELECT DISTINCT t1.cd_operadora, t1.cd_linha, t1.cd_sentido, t1.nr_estac_carro, t1.dt_transacao, t1.nr_cartao, t1.cd_emiss_aplic, t1.cd_aplicacao, t1. nr_trans_aplic, t1.vl_linha, t1.vl_transacao, t1.vl_subsidio, NULL, NULL, NULL, NULL,
        t1.nr_detord_subsid, t1.nr_valid, t1.nr_chip_sam, t1.nr_seq_arq, t1.nr_trans_sam, t1.cd_tp_debito, 'T1'
      FROM trans_1_20150206_20150305 t1
      WHERE DATE(dt_transacao) = dateStart;

      INSERT IGNORE INTO analise_trans_20150301_20150328
      SELECT DISTINCT *, 'T23' 
      FROM trans_23_20150206_20150305
      WHERE DATE(dt_transacao) = dateStart;
    END IF;

  -- mes atual
      INSERT INTO analise_trans_20150301_20150328
      SELECT DISTINCT t1.cd_operadora, t1.cd_linha, t1.cd_sentido, t1.nr_estac_carro, t1.dt_transacao, t1.nr_cartao, t1.cd_emiss_aplic, t1.cd_aplicacao, t1. nr_trans_aplic, t1.vl_linha, t1.vl_transacao, t1.vl_subsidio, NULL, NULL, NULL, NULL,
          t1.nr_detord_subsid, t1.nr_valid, t1.nr_chip_sam, t1.nr_seq_arq, t1.nr_trans_sam, t1.cd_tp_debito, 'T1'
      FROM trans_1_20150306_20150402 t1
      WHERE DATE(dt_transacao) = dateStart;

      INSERT IGNORE INTO analise_trans_20150301_20150328
      SELECT DISTINCT *, 'T23' 
      FROM trans_23_20150306_20150402
      WHERE DATE(dt_transacao) = dateStart; 

  -- mes seguinte
      INSERT INTO analise_trans_20150301_20150328
      SELECT DISTINCT t1.cd_operadora, t1.cd_linha, t1.cd_sentido, t1.nr_estac_carro, t1.dt_transacao, t1.nr_cartao, t1.cd_emiss_aplic, t1.cd_aplicacao, t1. nr_trans_aplic, t1.vl_linha, t1.vl_transacao, t1.vl_subsidio, 
      NULL, NULL, NULL, NULL, t1.nr_detord_subsid, t1.nr_valid, t1.nr_chip_sam, t1.nr_seq_arq, t1.nr_trans_sam, t1.cd_tp_debito, 'T1'
      FROM trans_1_20150403_20150430 t1
      WHERE DATE(dt_transacao) = dateStart;

      INSERT IGNORE INTO analise_trans_20150301_20150328
      SELECT DISTINCT *, 'T23' 
      FROM trans_23_20150403_20150430
      WHERE DATE(dt_transacao) = dateStart; 

      INSERT INTO analise_fill_database_dates
      SELECT now(), dateStart;

      SET dateStart = date_add(dateStart, INTERVAL 1 DAY);
    END WHILE;
  END

-- 2.2. Preenchimento da Base de Dados Básica:
CALL fill_database('2015-03-01','2015-03-28');

-- 2.3. Base de Dados de Transações apenas do sistema de BRTs:
-- 2.3.1. Tabela de armazenamento das transações apenas do sistema de BRTs
CREATE TABLE analise_trans_so_BRT like analise_trans_20150301_20150328;

-- 2.3.2. Alteração da Tabela de armazenamento das transações apenas do sistema de BRTs
ALTER TABLE analise_trans_so_BRT ADD t_ancora TINYINT(1) DEFAULT 0; -- Campo indicativo de Transação Ancorada

-- 2.3.3. Preenchimento da Tabela de Transações apenas do sistema de BRTs
INSERT IGNORE INTO analise_trans_so_BRT
  SELECT t.*, 0 FROM analise_trans_20150301_20150328 t
  JOIN linhas_operadoras_modo_BRT l
  ON t.cd_linha = l.cd_linha; -- 356209 --> 372573 rows -- 5480 s --> 6732 s

-- 2.4. Base de Dados de Todas Transações dos usuários do sistema de BRTs
-- 2.4.1. Tabela de Usuários do BRT e data de utilização:
CREATE TABLE analise_BRT_cartao_data_utilizacao
  (
    nr_cartao VARCHAR(13),
    dt_transacao DATE,
    PRIMARY KEY (nr_cartao, dt_transacao)
  );

TRUNCATE analise_BRT_cartao_data_utilizacao;

-- 2.4.2. Preenchimento da Tabela de Usuários do BRT e dia da Semana
INSERT IGNORE INTO analise_BRT_cartao_data_utilizacao
  SELECT nr_cartao, DATE(dt_transacao)
  FROM analise_trans_20150301_20150328 t
  JOIN linhas_operadoras_modo_BRT l
  ON t.cd_linha = l.cd_linha
  GROUP BY nr_cartao, DATE(dt_transacao); -- 301326 --> 314482 rows -- 9844 --> 5641 s

-- 2.4.3. Criação da Tabela de Todas Transações dos usuários do sistema de BRTs
CREATE TABLE analise_trans_BRT LIKE analise_trans_20150301_20150328;

-- 2.4.4. Preenchimento da Tabela de Todas Transações dos usuários do sistema de BRTs
INSERT IGNORE INTO analise_trans_BRT
  SELECT t.*
  FROM analise_trans_20150301_20150328 t
  JOIN analise_BRT_cartao_data_utilizacao a
  ON (t.nr_cartao, DATE(t.dt_transacao)) = (a.nr_cartao, a.dt_transacao); -- 955698 --> 996031 rows -- 12 s --> 1541 s


-- Etapa 3: PROCESSAMENTO COM ANÁLISE DE COMPORTAMENTO DE USUÁRIOS
-- 3.1. Detecção de Usuários Regulares do BRT
-- 3.1.1. Criação da Tabela de Cartões
CREATE TABLE analise_BRT_cartao_ancora
  (
    nr_cartao VARCHAR(13),
    c_qtd_ancora INT DEFAULT 0,
    t_brt_periodo INT,
    t_total_periodo INT,
    PRIMARY KEY (nr_cartao, c_qtd_ancora)
  );

TRUNCATE TABLE analise_BRT_cartao_ancora;

-- 3.1.2. Preenchimento básico da Tabela de Cartões
INSERT INTO analise_BRT_cartao_ancora (nr_cartao, t_brt_periodo)
  SELECT nr_cartao, count(*)
  FROM analise_trans_so_BRT
  GROUP BY nr_cartao; -- 80011 rows -- 2.7 seg

UPDATE analise_BRT_cartao_ancora a
  INNER JOIN (
    SELECT nr_cartao, count(*) t_total_periodo
    FROM analise_trans_BRT 
    GROUP BY nr_cartao) t
  ON a.nr_cartao = t.nr_cartao
  SET a.t_total_periodo = t.t_total_periodo;

-- 3.1.3. Criação da Tabela Auxiliar de Pares Cartão - Linha Âncora
CREATE TABLE analise_BRT_par_cartao_linha_ancora
  (
    nr_cartao VARCHAR(13),
    cd_linha VARCHAR(6),
    l_nm_ramal VARCHAR(13),
    l_sem_periodo INT,
    l_t_periodo INT,
    perc_total DECIMAL(3,2),
    PRIMARY KEY(nr_cartao, cd_linha)
  );

truncate analise_BRT_par_cartao_linha_ancora;
SELECT count(*) FROM analise_BRT_par_cartao_linha_ancora;
DROP TABLE analise_BRT_par_cartao_linha_ancora;

-- 3.1.4. Preenchimento da Tabela Auxiliar de Pares Cartão - Linha Âncora
INSERT INTO analise_BRT_par_cartao_linha_ancora
  SELECT nr_cartao, t.cd_linha, l.nm_ramal, count(distinct week(dt_transacao)), count(*), CAST(count(*) / (SELECT count(*) FROM analise_trans_so_BRT WHERE nr_cartao = t.nr_cartao) AS DECIMAL(3,2)) perc_total
    FROM analise_trans_so_BRT t
    JOIN linhas_operadoras_modo l
    ON t.cd_linha = l.cd_linha
    GROUP BY nr_cartao, cd_linha
      HAVING count(distinct week(dt_transacao)) >= 3 and perc_total >= 0.2; -- 23543 rows -- 1.4 s

-- 3.1.5. Update da Tabela de Cartões para indicação de quantidade de âncoras
UPDATE analise_BRT_cartao_ancora ca
  SET ca.c_qtd_ancora = 0;

UPDATE analise_BRT_cartao_ancora ca
  INNER JOIN (
    SELECT nr_cartao, count(distinct cd_linha) qtd_ancora 
    FROM analise_BRT_par_cartao_linha_ancora 
    GROUP BY nr_cartao) p
  ON ca.nr_cartao = p.nr_cartao
  SET ca.c_qtd_ancora = p.qtd_ancora;

-- 3.1.6. Update da Tabela de Transações para indicação de transações ancoradas
UPDATE analise_trans_so_BRT t
  SET t.t_ancora = 0;
  
UPDATE analise_trans_so_BRT t
  INNER JOIN analise_BRT_par_cartao_linha_ancora p
  ON (t.nr_cartao, t.cd_linha) = (p.nr_cartao, p.cd_linha) 
  SET t.t_ancora = 1;

-- 3.1.7. Criação da Tabela Auxiliar de Cartão - Data de Utilização por Ramais
CREATE TABLE analise_BRT_cartao_data_utilizacao_ramais
  (
    nr_cartao VARCHAR(13),
    dt_transacao DATE,
    nm_ramais VARCHAR(50),
    PRIMARY KEY (nr_cartao, dt_transacao)
  );

CREATE TABLE analise_BRT_cartao_data_utilizacao_estacao like analise_BRT_cartao_data_utilizacao_ramais;

TRUNCATE analise_BRT_cartao_data_utilizacao_ramais;
SELECT * FROM linhas_operadoras_modo WHERE tp_modo = 'brt' and nm_linha_consolid like '%olaria%' ;

-- 3.1.8. Preenchimento da Tabela de Usuários do BRT, data de Utilização e ramais
-- Quantidade detalhada de ramais: não utilizado
/*INSERT IGNORE INTO analise_BRT_cartao_data_utilizacao_ramais
		SELECT nr_cartao, dt,
        CASE 
			WHEN max(alvorada) = 1 AND count(distinct nm_ramal) = 2 THEN 'Alvorada + TCA'
            WHEN max(alvorada) = 1 AND count(distinct nm_ramal) = 1 AND count(distinct nm_linha_consolid) = 1 THEN 'Alvorada' 
			WHEN max(alvorada) = 1 AND count(distinct nm_ramal) = 1 AND count(distinct nm_linha_consolid) > 1 THEN 'Alvorada + TOE' 
			WHEN max(alvorada) = 0 AND count(distinct nm_ramal) = 2 THEN 'TOE + TCA'
            ELSE nm_ramal END nm_ramal
		FROM (
			SELECT nr_cartao, DATE(dt_transacao) dt, nm_ramal, l.nm_linha_consolid, 
				CASE WHEN nm_linha_consolid = 'Terminal Alvorada' THEN 1 ELSE 0 END alvorada 
			FROM analise_trans_so_BRT t
			JOIN linhas_operadoras_modo l
			ON t.cd_linha = l.cd_linha AND l.tp_modo = 'BRT'
			GROUP BY nr_cartao, dt, nm_ramal, nm_linha_consolid) tl
		GROUP BY nr_cartao, dt; */

INSERT IGNORE INTO analise_BRT_cartao_data_utilizacao_estacao
		SELECT nr_cartao, dt,
        CASE 
            WHEN max(estacao) = 2 THEN 'Madureira' 
			WHEN max(estacao) = 3 THEN 'Santa Cruz'
			WHEN max(estacao) = 4 THEN 'Penha'
            WHEN max(estacao) = 5 THEN 'Campo Grande'
			ELSE nm_ramal END nm_ramal
		FROM (
			SELECT nr_cartao, DATE(dt_transacao) dt, nm_ramal, l.nm_linha_consolid, 
				CASE 
                WHEN nm_linha_consolid LIKE '%MADUREIRA%' THEN 2
                WHEN nm_linha_consolid LIKE '%SANTA CRUZ%' THEN 3
                WHEN nm_linha_consolid LIKE '%PENHA%' THEN 4
                WHEN nm_linha_consolid LIKE '%CAMPO GRANDE%' THEN 5
                ELSE 0 END estacao 
			FROM analise_trans_so_BRT t
			JOIN linhas_operadoras_modo l
			ON t.cd_linha = l.cd_linha AND l.tp_modo = 'BRT'
			GROUP BY nr_cartao, dt, nm_ramal, nm_linha_consolid) tl
		GROUP BY nr_cartao, dt; 

-- Quantidade agregada de ramais
INSERT IGNORE INTO analise_BRT_cartao_data_utilizacao_ramais
		SELECT nr_cartao, dt,
        CASE 
			WHEN count(distinct nm_ramal) = 2 THEN 'TOE + TCA'
            ELSE nm_ramal END nm_ramal
		FROM (
			SELECT nr_cartao, DATE(dt_transacao) dt, nm_ramal, l.nm_linha_consolid, 
				CASE WHEN nm_linha_consolid = 'Terminal Alvorada' THEN 1 ELSE 0 END alvorada 
			FROM analise_trans_so_BRT t
			JOIN linhas_operadoras_modo l
			ON t.cd_linha = l.cd_linha AND l.tp_modo = 'BRT'
			GROUP BY nr_cartao, dt, nm_ramal, nm_linha_consolid) tl
		GROUP BY nr_cartao, dt; 
        
SELECT DISTINCT nm_ramais, count(distinct nr_cartao) FROM analise_BRT_cartao_data_utilizacao_ramais GROUP BY nm_ramais;

SELECT nr, case when nr = 1 then a.nm_ramais end, count(distinct t1.nr_cartao)
FROM(
SELECT distinct nr_cartao, count(distinct nm_ramais) nr
FROM analise_BRT_cartao_data_utilizacao_ramais 
GROUP BY nr_cartao
) t1
join analise_BRT_cartao_data_utilizacao_ramais  a
on a.nr_cartao = t1.nr_cartao
GROUP BY nr, nm_ramais;

-- 3.2. Análise de Padrões dos Usuários do BRT por Regular / Esporádico
-- 3.2.1. Dados Básicos Analisados

SELECT count(*), count(distinct nr_cartao) FROM analise_trans_20150301_20150328; -- 43186632	1707195
SELECT count(*), count(distinct nr_cartao) FROM analise_trans_so_BRT; -- 372573	80011
SELECT count(*), count(distinct nr_cartao) FROM analise_trans_so_BRT
WHERE cd_linha in (SELECT cd_linha FROM linhas_operadoras_modo WHERE nm_ramal = 'TransCarioca'); -- 248624	56664
SELECT count(*), count(distinct nr_cartao) FROM analise_trans_so_BRT
WHERE cd_linha in (SELECT cd_linha FROM linhas_operadoras_modo WHERE nm_ramal = 'TransOeste'); -- 123949	30039
SELECT count(*), count(distinct nr_cartao) FROM analise_trans_BRT; -- 996031	80011

-- 3.2.2. Relação de Pares Cartão-Estação por Condições de Regularidade
SELECT qtd, count(*)
	FROM (SELECT count(*) qtd
		FROM analise_trans_so_BRT t 
		GROUP BY nr_cartao, cd_linha) g
    GROUP BY qtd;
    
SELECT qtd_sem, count(*)
	FROM (SELECT count(distinct week(dt_transacao)) qtd_sem 
		FROM analise_trans_so_BRT t 
		GROUP BY nr_cartao, cd_linha) g
    GROUP BY qtd_sem;

SELECT perc_total, count(*)
	FROM (SELECT 10 * FLOOR(10 * count(*) / (SELECT count(*) FROM analise_trans_so_BRT WHERE nr_cartao = t.nr_cartao)) perc_total
		FROM analise_trans_so_BRT t 
		GROUP BY nr_cartao, cd_linha) a
    GROUP BY perc_total;
    
SELECT qtd_sem,
	CASE 
		WHEN perc_total < 0.1 THEN 'g_1'
		WHEN perc_total < 0.2 THEN 'g_2'
        WHEN perc_total < 0.3 THEN 'g_3'
        WHEN perc_total < 0.4 THEN 'g_4'
        WHEN perc_total < 0.5 THEN 'g_5'
        WHEN perc_total >= 0.5 THEN 'g_6'
		END perc_total_group,
	count(*)
FROM (
SELECT nr_cartao, cd_linha, 
	count(distinct week(dt_transacao)) qtd_sem, 
    count(*), 
    CAST(count(*) / (SELECT count(*) FROM analise_trans_so_BRT WHERE nr_cartao = t.nr_cartao) AS DECIMAL(3,2)) perc_total
	FROM analise_trans_so_BRT t 
    GROUP BY nr_cartao, cd_linha) a
    GROUP BY qtd_sem, perc_total_group;

    
SELECT qtd_sem, 
	CASE 
		WHEN qtd > 4 THEN 'mais 4'
		ELSE qtd
		END qtd_group,
	count(*)
FROM (
SELECT nr_cartao, cd_linha, 
	count(distinct week(dt_transacao)) qtd_sem, 
    count(*) qtd, 
    CAST(count(*) / (SELECT count(*) FROM analise_trans_so_BRT WHERE nr_cartao = t.nr_cartao) AS DECIMAL(3,2)) perc_total
	FROM analise_trans_so_BRT t 
    GROUP BY nr_cartao, cd_linha) a
    GROUP BY qtd_sem, qtd_group;

-- 3.2.2. Relação de variáveis em função da quantidade de âncoras por cartâo
-- 3.2.2.1. Sistema BRT
SELECT a.c_qtd_ancora, 
	count(distinct a.nr_cartao) qtd_anc_cart, 
	count(distinct a.nr_cartao) / (SELECT count(distinct nr_cartao) FROM analise_BRT_cartao_ancora) perc_qtd_anc_cart,
    count(t.dt_transacao) qtd_trans,
    count(t.dt_transacao) / (SELECT count(dt_transacao) FROM analise_trans_so_BRT) perc_qtd_trans,
    count(CASE WHEN t_ancora = 1 THEN 1 END) qtd_anc_trans, 
	count(CASE WHEN t_ancora = 1 THEN 1 END) / 
		(SELECT count(*) FROM analise_trans_so_BRT tt JOIN analise_BRT_cartao_ancora aa
		ON aa.nr_cartao = tt.nr_cartao WHERE aa.c_qtd_ancora = a.c_qtd_ancora) perc_qtd_anc_trans
    FROM analise_BRT_cartao_ancora a
    INNER JOIN analise_trans_so_BRT t
    ON a.nr_cartao = t.nr_cartao
    GROUP BY a.c_qtd_ancora;

SELECT a.c_qtd_ancora, 
	count(distinct a.nr_cartao) qtd_anc_cart, 
	count(distinct a.nr_cartao) / (SELECT count(distinct nr_cartao) FROM analise_BRT_cartao_ancora) perc_qtd_anc_cart,
    count(t.dt_transacao) qtd_trans,
    count(t.dt_transacao) / (SELECT count(dt_transacao) FROM analise_trans_so_BRT) perc_qtd_trans,
    count(CASE WHEN t_ancora = 1 THEN 1 END) qtd_anc_trans, 
	count(CASE WHEN t_ancora = 1 THEN 1 END) / 
		(SELECT count(*) FROM analise_trans_so_BRT tt JOIN analise_BRT_cartao_ancora aa
		ON aa.nr_cartao = tt.nr_cartao WHERE aa.c_qtd_ancora = a.c_qtd_ancora) perc_qtd_anc_trans
    FROM analise_BRT_cartao_ancora a
    INNER JOIN analise_trans_so_BRT t
    ON a.nr_cartao = t.nr_cartao
    WHERE a.t_brt_periodo = 1
    GROUP BY a.c_qtd_ancora;

-- 3.2.2.2. Corredor de BRT
SELECT a.c_qtd_ancora, 
	count(distinct a.nr_cartao) qtd_anc_cart, 
	count(distinct a.nr_cartao) / 
		(SELECT count(distinct aa.nr_cartao) FROM analise_BRT_cartao_ancora aa
		INNER JOIN analise_trans_so_BRT tt
		ON aa.nr_cartao = tt.nr_cartao
		INNER JOIN linhas_operadoras_modo ll
		ON ll.cd_linha = tt.cd_linha 
		WHERE ll.nm_ramal = 'TransOeste'
		) perc_qtd_anc_cart,
    count(t.dt_transacao) qtd_trans,
    count(t.dt_transacao) / 
		(SELECT count(*) FROM analise_trans_so_BRT ttt
        INNER JOIN linhas_operadoras_modo lll
        ON lll.cd_linha = ttt.cd_linha
        WHERE lll.nm_ramal = 'TransOeste'
        ) perc_qtd_trans,
    count(CASE WHEN t_ancora = 1 THEN 1 END) qtd_anc_trans, 
	count(CASE WHEN t_ancora = 1 THEN 1 END) / 
		(SELECT count(*) FROM analise_trans_so_BRT tttt
        INNER JOIN analise_BRT_cartao_ancora aaaa
		ON aaaa.nr_cartao = tttt.nr_cartao 
        INNER JOIN linhas_operadoras_modo llll
        ON llll.cd_linha = tttt.cd_linha
        WHERE aaaa.c_qtd_ancora = a.c_qtd_ancora AND llll.nm_ramal = 'TransOeste') perc_qtd_anc_trans
    FROM analise_BRT_cartao_ancora a
    INNER JOIN analise_trans_so_BRT t
    ON a.nr_cartao = t.nr_cartao
    INNER JOIN linhas_operadoras_modo l
    ON l.cd_linha = t.cd_linha 
    WHERE nm_ramal = 'TransOeste'
    GROUP BY a.c_qtd_ancora;

-- 3.2.3. Distribuição Temporal - Relação diária das variáveis
-- 3.2.3.1. Tabela completa por dia analisado
SELECT date(t.dt_transacao) dt,
	count(t.dt_transacao) t, 
	count(CASE WHEN t_ancora = 1 THEN 1 END) t_anc, 
    count(CASE WHEN t_ancora = 1 THEN 1 END)/count(t.dt_transacao) rt_t_anc,
    count(DISTINCT t.nr_cartao) c,
	count(DISTINCT c_anc.nr_cartao) c_anc,
    count(DISTINCT c_anc.nr_cartao)/count(DISTINCT t.nr_cartao) rt_c_anc
	FROM analise_trans_so_BRT t
	LEFT JOIN analise_brt_cartao_ancora c_anc
    ON t.nr_cartao = c_anc.nr_cartao AND c_anc.c_qtd_ancora != 0
	GROUP BY dt;

-- 3.2.3.2. Tabela por dia da semana
SELECT dayofweek(dt) dia_semana, avg(t), avg(t_anc), avg(rt_t_anc), avg(c), avg(c_anc), avg(rt_c_anc)
	FROM( 
		SELECT date(t.dt_transacao) dt,
        count(t.dt_transacao) t,
		count(CASE WHEN t_ancora = 1 THEN 1 END) t_anc, 
		count(DISTINCT t.nr_cartao) c,
		count(DISTINCT c_anc.nr_cartao) c_anc,
		count(CASE WHEN t_ancora = 1 THEN 1 END)/count(t.dt_transacao) rt_t_anc,
		count(DISTINCT c_anc.nr_cartao)/count(DISTINCT t.nr_cartao) rt_c_anc
		FROM analise_trans_so_BRT t
		LEFT JOIN analise_brt_cartao_ancora c_anc
		ON t.nr_cartao = c_anc.nr_cartao AND c_anc.c_qtd_ancora != 0
		GROUP BY dt) g
	GROUP BY dia_semana;

-- 3.2.4. Distribuição Temporal - Relação horária das variáveis
-- 3.2.4.1. Tabela por tipo de dia
SELECT tipo_dia, hr, avg(t), avg(rt_c_anc), std(rt_c_anc)
	FROM(
	SELECT 
		date(dt_transacao) dt,
        dayofweek(dt_transacao) dia,
		CASE dayofweek(dt_transacao)
			WHEN 1 THEN 'Domingo'
			WHEN 7 THEN 'Sábado'
			ELSE 'Dia útil'
			END tipo_dia,
		hour(t.dt_transacao) hr, 
		count(*) t,
        count(DISTINCT c_anc.nr_cartao)/count(DISTINCT t.nr_cartao) rt_c_anc
		FROM analise_trans_so_BRT t
		LEFT JOIN analise_brt_cartao_ancora c_anc
		ON t.nr_cartao = c_anc.nr_cartao AND c_anc.c_qtd_ancora != 0
		GROUP BY dt, hour(dt_transacao)) t
	GROUP BY tipo_dia, hr;

-- 3.2.4.2. Tabela por dia de semana
SELECT dia, tipo_dia, hr, avg(t), avg(rt_c_anc), std(rt_c_anc)
	FROM(
	SELECT 
		date(dt_transacao) dt,
		dayofweek(dt_transacao) dia,
		CASE dayofweek(dt_transacao)
			WHEN 1 THEN 'Domingo'
			WHEN 7 THEN 'Sábado'
			ELSE 'Dia útil'
			END tipo_dia,
		hour(t.dt_transacao) hr, 
        count(*) t,
		count(DISTINCT c_anc.nr_cartao)/count(DISTINCT t.nr_cartao) rt_c_anc
		FROM analise_trans_so_BRT t
		LEFT JOIN analise_brt_cartao_ancora c_anc
		ON t.nr_cartao = c_anc.nr_cartao AND c_anc.c_qtd_ancora != 0
		GROUP BY dt, hour(dt_transacao)) t
	GROUP BY dia, tipo_dia, hr;

-- 3.2.5. Distribuição Espacial
-- 3.2.5.1. Estações de BRT

SELECT l.nm_linha_consolid nm_estacao, l.nm_ramal, cast(l.cd_georef as UNSIGNED) cd_georef, 
    count(CASE WHEN hour(dt_transacao) = 6 THEN 1 END)/20 am_t_dia,
    count(CASE WHEN hour(dt_transacao) = 6 THEN 1 END)/
		(SELECT t_corr FROM (SELECT ll.nm_ramal, hour(dt_transacao) hr, count(*) t_corr FROM analise_trans_so_BRT tt INNER JOIN linhas_operadoras_modo ll
			ON tt.cd_linha = ll.cd_linha
			WHERE dayofweek(dt_transacao) != 1 and dayofweek(dt_transacao) != 7 and hour(dt_transacao) = 6
            GROUP BY nm_ramal, hr) d
            WHERE nm_ramal = l.nm_ramal and hr = 6)
            am_rt_t_corr,
    count(CASE WHEN t_ancora = 1 AND hour(dt_transacao) = 6 THEN 1 END)/count(CASE WHEN hour(dt_transacao) = 6 THEN 1 END) am_rt_t_anc,
    count(CASE WHEN hour(dt_transacao) = 17 THEN 1 END)/20 pm_t_dia,
    count(CASE WHEN hour(dt_transacao) = 17 THEN 1 END)/
		(SELECT t_corr FROM (SELECT ll.nm_ramal, hour(dt_transacao) hr, count(*) t_corr FROM analise_trans_so_BRT tt INNER JOIN linhas_operadoras_modo ll
			ON tt.cd_linha = ll.cd_linha
			WHERE dayofweek(dt_transacao) != 1 and dayofweek(dt_transacao) != 7 and hour(dt_transacao) = 17
            GROUP BY nm_ramal, hr) d
            WHERE nm_ramal = l.nm_ramal and hr = 17)
            pm_rt_t_corr,
	count(CASE WHEN t_ancora = 1 AND hour(dt_transacao) = 17 THEN 1 END)/count(CASE WHEN hour(dt_transacao) = 17 THEN 1 END) pm_rt_t_anc
    FROM analise_trans_so_BRT t
    INNER JOIN linhas_operadoras_modo l
    ON t.cd_linha = l.cd_linha
    WHERE dayofweek(dt_transacao) != 1 and dayofweek(dt_transacao) != 7
    GROUP BY l.nm_linha_consolid
    HAVING 
		(am_t_dia != 0 and pm_t_dia != 0)
    ORDER BY nm_ramal, am_rt_t_corr DESC;

-- 3.2.4.2. Distribuição Espacial - Relação de integrações

-- Distribuição de transacoes por ramal utilizado e por corredor - nao utilizado
/*-- Divisão de transações e cartões por ramal utilizado
SELECT cdr.nm_ramais, a.c_qtd_ancora,
	count(distinct a.nr_cartao) c, 
	count(distinct a.nr_cartao) / 
		(SELECT count(distinct aa.nr_cartao) FROM analise_BRT_cartao_ancora aa
		INNER JOIN analise_trans_so_BRT t2
		ON aa.nr_cartao = t2.nr_cartao
		INNER JOIN analise_brt_cartao_data_utilizacao_ramais cdr2
		ON (t2.nr_cartao, date(t2.dt_transacao)) = (cdr2.nr_cartao, cdr2.dt_transacao)
		WHERE cdr2.nm_ramais = cdr.nm_ramais
		) rt_c_tot,
    count(*) t,
    count(*) /
		(SELECT count(*) FROM analise_trans_so_BRT t3
        INNER JOIN analise_brt_cartao_data_utilizacao_ramais cdr3
		ON (t3.nr_cartao, date(t3.dt_transacao)) = (cdr3.nr_cartao, cdr3.dt_transacao)
		WHERE cdr3.nm_ramais = cdr.nm_ramais
		) rt_t_tot,
    count(CASE WHEN t_ancora = 1 THEN 1 END) t_anc, 
	count(CASE WHEN t_ancora = 1 THEN 1 END) / 
		(SELECT count(*) FROM analise_trans_so_BRT t4
        INNER JOIN analise_BRT_cartao_ancora aaaa
		ON aaaa.nr_cartao = t4.nr_cartao 
        INNER JOIN analise_brt_cartao_data_utilizacao_ramais cdr4
		ON (t4.nr_cartao, date(t4.dt_transacao)) = (cdr4.nr_cartao, cdr4.dt_transacao)
		WHERE cdr4.nm_ramais = cdr.nm_ramais AND aaaa.c_qtd_ancora = a.c_qtd_ancora) rt_t_anc
	FROM analise_trans_so_BRT t
    INNER JOIN analise_brt_cartao_data_utilizacao_ramais cdr
    ON (t.nr_cartao, date(t.dt_transacao)) = (cdr.nr_cartao, cdr.dt_transacao)
    INNER JOIN analise_brt_cartao_ancora a
    ON cdr.nr_cartao = a.nr_cartao
    GROUP BY cdr.nm_ramais, a.c_qtd_ancora;
    




SELECT a.c_qtd_ancora, 
	count(distinct a.nr_cartao) qtd_anc_cart, 
	count(distinct a.nr_cartao) / 
		(SELECT count(distinct aa.nr_cartao) FROM analise_BRT_cartao_ancora aa
		INNER JOIN analise_trans_so_BRT tt
		ON aa.nr_cartao = tt.nr_cartao
		INNER JOIN linhas_operadoras_modo ll
		ON ll.cd_linha = tt.cd_linha 
		WHERE ll.nm_ramal = 'TransOeste'
		) perc_qtd_anc_cart,
    count(t.dt_transacao) qtd_trans,
    count(t.dt_transacao) / 
		(SELECT count(*) FROM analise_trans_so_BRT ttt
        INNER JOIN linhas_operadoras_modo lll
        ON lll.cd_linha = ttt.cd_linha
        WHERE lll.nm_ramal = 'TransOeste'
        ) perc_qtd_trans,
    count(CASE WHEN t_ancora = 1 THEN 1 END) qtd_anc_trans, 
	count(CASE WHEN t_ancora = 1 THEN 1 END) / 
		(SELECT count(*) FROM analise_trans_so_BRT tttt
        INNER JOIN analise_BRT_cartao_ancora aaaa
		ON aaaa.nr_cartao = tttt.nr_cartao 
        INNER JOIN linhas_operadoras_modo llll
        ON llll.cd_linha = tttt.cd_linha
        WHERE aaaa.c_qtd_ancora = a.c_qtd_ancora AND llll.nm_ramal = 'TransOeste') perc_qtd_anc_trans
    FROM analise_BRT_cartao_ancora a
    INNER JOIN analise_trans_so_BRT t
    ON a.nr_cartao = t.nr_cartao
    INNER JOIN linhas_operadoras_modo l
    ON l.cd_linha = t.cd_linha 
    WHERE nm_ramal = 'TransOeste'
    GROUP BY a.c_qtd_ancora; */

-- Quantidade de transações e divisão modal para modos complementares por ramal
SELECT cdr.nm_ramais, 
	CASE WHEN a.c_qtd_ancora = 0 THEN 0 ELSE 1 END c_ancora,
	l.tp_modo,
    count(*) qtd_t,
    count(*)/
		(SELECT count(*) FROM analise_trans_BRT t2
			INNER JOIN linhas_operadoras_modo l2
			ON t2.cd_linha = l2.cd_linha and tp_modo != 'brt'
            INNER JOIN analise_brt_cartao_data_utilizacao_ramais cdr2
			ON (t2.nr_cartao, date(t2.dt_transacao)) = (cdr2.nr_cartao, cdr2.dt_transacao)
            WHERE cdr2.nm_ramais = cdr.nm_ramais) perc_t_tot
    FROM analise_trans_BRT t
    INNER JOIN linhas_operadoras_modo l
    ON t.cd_linha = l.cd_linha and tp_modo != 'brt'
    INNER JOIN analise_brt_cartao_data_utilizacao_ramais cdr
    ON (t.nr_cartao, date(t.dt_transacao)) = (cdr.nr_cartao, cdr.dt_transacao)
    INNER JOIN analise_brt_cartao_ancora a
    ON t.nr_cartao = a.nr_cartao
    GROUP BY cdr.nm_ramais, c_ancora, l.tp_modo
    ORDER BY nm_ramais, perc_t_tot DESC;

SELECT cdr.nm_ramais, 
	CASE WHEN a.c_qtd_ancora = 0 THEN 0 ELSE 1 END c_ancora,
	l.tp_modo,
    count(*) t
    FROM analise_trans_BRT t
    INNER JOIN linhas_operadoras_modo l
    ON t.cd_linha = l.cd_linha and tp_modo != 'brt'
    INNER JOIN analise_brt_cartao_data_utilizacao_ramais cdr
    ON (t.nr_cartao, date(t.dt_transacao)) = (cdr.nr_cartao, cdr.dt_transacao)
    INNER JOIN analise_brt_cartao_ancora a
    ON t.nr_cartao = a.nr_cartao
    GROUP BY cdr.nm_ramais, c_ancora, l.tp_modo
    ORDER BY nm_ramais, t DESC;


SELECT cdr.nm_ramais, 
	CASE WHEN a.c_qtd_ancora = 0 THEN 0 ELSE 1 END c_ancora,
	l.tp_modo,
    count(*) t
    FROM analise_trans_BRT t
    INNER JOIN linhas_operadoras_modo l
    ON t.cd_linha = l.cd_linha and tp_modo != 'brt'
    INNER JOIN analise_brt_cartao_data_utilizacao_estacao cdr
    ON (t.nr_cartao, date(t.dt_transacao)) = (cdr.nr_cartao, cdr.dt_transacao)
    INNER JOIN analise_brt_cartao_ancora a
    ON t.nr_cartao = a.nr_cartao
    GROUP BY cdr.nm_ramais, c_ancora, l.tp_modo
    ORDER BY nm_ramais, t DESC;

-- quantidade de transacoes por linhas complementares e por ramal - não utilizado
SELECT cdr.nm_ramais, l.tp_modo, nm_linha_DETRO,
	count(*) qtd_t
    FROM analise_trans_BRT t
    INNER JOIN linhas_operadoras_modo l
    ON t.cd_linha = l.cd_linha 
    INNER JOIN analise_brt_cartao_data_utilizacao_ramais cdr
    ON (t.nr_cartao, date(t.dt_transacao)) = (cdr.nr_cartao, cdr.dt_transacao)
    INNER JOIN analise_brt_cartao_ancora a
    ON cdr.nr_cartao = a.nr_cartao
    GROUP BY cdr.nm_ramais, l.tp_modo, l.nm_linha_DETRO
    HAVING qtd_t > 1000
    ORDER BY nm_ramais, qtd_t DESC;    
-- 
SELECT cd_aplicacao, count(distinct t.nr_cartao) c, count(case when a.c_qtd_ancora != 0 then 1 end) c_anc,
 count(distinct t.nr_cartao) / count(case when a.c_qtd_ancora != 0 then 1 end) rt_c_anc,
 count(*) t, count(case when t_ancora = 1 then 1 end) t_anc, 
 count(case when t_ancora = 1 then 1 end)/count(*) rt_t_anc
 FROM analise_trans_so_BRT t
 INNER JOIN analise_brt_cartao_ancora a
    ON t.nr_cartao = a.nr_cartao
    GROUP BY cd_aplicacao;