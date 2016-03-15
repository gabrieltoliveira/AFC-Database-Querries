-- brt_estimativa
-- ETAPA 1. CRIAÇÃO DA BASE DE DADOS CORRETA
-- 1.1. Criação da tabela com todas as transações realizadas pelos cartões nos dias em que utilizaram o BRT
DROP TABLE IF EXISTS analise_trans_estimativa_brt_base;
CREATE TABLE analise_trans_estimativa_brt_base LIKE analise_trans_brt;
TRUNCATE analise_trans_estimativa_brt_base;

-- 1.2. Preenchimento da tabela com todas transações realizadas pelos cartões nos dias em que utilizaram o BRT
INSERT INTO analise_trans_estimativa_brt_base
	SELECT t.* FROM analise_trans_brt t
	JOIN analise_brt_cartao_data_utilizacao_ramais cdr
    ON (t.nr_cartao, date(t.dt_transacao)) = (cdr.nr_cartao, cdr.dt_transacao)
	WHERE dayofweek(t.dt_transacao) not in (1,7) and nm_ramais in ('TransCarioca', 'TOE + TCA')
    ORDER BY nr_cartao, dt_transacao, nr_trans_aplic; -- 125981 rows 3 s // 619005 rows 10 s

SELECT @dias_estudados := COUNT(distinct date(dt)) as dias_estudados from analise_viagens_brt;

-- 1.3. Inserção de campo com número da transação em ordem sequencial
ALTER TABLE analise_trans_estimativa_brt_base DROP PRIMARY KEY, ADD COLUMN nr_trans INT NOT NULL AUTO_INCREMENT PRIMARY KEY; -- 22 s

-- ETAPA 2. MONTAGEM DE VIAGENS EM QUE HOUVE TRANSAÇÃO DO BRT
-- TRANSACAO ANTERIOR - EMBARQUE NO BRT - DESEMBARQUE NO BRT - PROXIMA TRANSACAO NA MESMA VIAGEM - PROXIMA TRANSACAO EM OUTRA VIAGEM
-- 2.1. Criação da tabela de armazenamento de viagens no BRT
DROP TABLE IF EXISTS analise_viagens_brt;

CREATE TABLE analise_viagens_brt
(
nr_cartao varchar(13),
dt date,
nr_viagem INT NOT NULL auto_increment,
 -- dados transação anterior
dt_trans_A datetime, 
nr_trans_A varchar(6),
cd_linha_A varchar(5),
cd_oper_A varchar(5),
tp_modo_A varchar(12),
nm_linha_A varchar(70),
-- dados transação BRT
dt_trans_B datetime,
nr_trans_B varchar(6),
cd_linha_B varchar(5),
cd_oper_B varchar(5),
tp_modo_B varchar(12),
nm_linha_B varchar(70),
nm_linha_dbq_B varchar(70),
-- dados transação Perior
tp_trans_P varchar(10),
dt_trans_P datetime,
nr_trans_P varchar(6),
cd_linha_P varchar(5),
cd_oper_P varchar(5),
tp_modo_P varchar(12),
nm_linha_P varchar(70),
PRIMARY KEY (nr_cartao, dt, nr_viagem)
) ENGINE = 'MyISAM'; -- MyISAM permite a criação de número sequencial para cada viagem realizada em determinado dia por determinado cartão

-- 2.2. Montagem da Viagens com base em análise horária elaborada (intervalo diferenciado de tempo em horário de pico):
TRUNCATE analise_viagens_brt;
SET @intervalo = 90; -- intervalo de tempo que caracteriza transferência, em horário fora de pico
SET @intervalo_pico = 120; --  intervalo de tempo que caracteriza transferência, em horário de pico
SET @intervalo_viagem = 180; -- intervalo de tempo que caracteriza viagens longas

INSERT INTO analise_viagens_brt
	(nr_cartao, dt,
        dt_trans_A, nr_trans_A, cd_linha_A, cd_oper_A, tp_modo_A, nm_linha_A,
        dt_trans_B, nr_trans_B, cd_linha_B, cd_oper_B, tp_modo_B, nm_linha_B, nm_linha_dbq_B,
        tp_trans_P, dt_trans_P, nr_trans_P, cd_linha_P, cd_oper_P, tp_modo_P, nm_linha_P)
	SELECT t.nr_cartao, date(t.dt_transacao),
		tA.dt_transacao, tA.nr_trans, tA.cd_linha, tA.cd_operadora, tA_l.tp_modo, tA_l.nm_linha_DETRO, -- Detalhes transação anterior A
		t.dt_transacao, t.nr_trans, t.cd_linha, t.cd_operadora, t_l.tp_modo, t_l.nm_linha_consolid, NULL, -- Detalhes transação BRT
		CASE -- Tipo de transação Posterior (transferência ou próxima viagem)
			WHEN tP_l.tp_modo != 'BRT' AND -- no caso de transação realizada não em BRT, pode ser transferência, caso repeite intervalos horários
				((HOUR(t.dt_transacao) IN (6,7,17,18) AND TIMESTAMPDIFF(MINUTE, t.dt_transacao, tP.dt_transacao) < @intervalo_pico) OR 
                (HOUR(t.dt_transacao) NOT IN (6,7,17,18) AND TIMESTAMPDIFF(MINUTE, t.dt_transacao, tP.dt_transacao) < @intervalo))
				THEN 'TRNSF'
			WHEN tP.dt_transacao IS NULL THEN NULL
			ELSE 'PVG' END tp_transf,
		tP.dt_transacao, tP.nr_trans, tP.cd_linha, tP.cd_operadora, tP_l.tp_modo, tP_l.nm_linha_DETRO
	FROM analise_trans_estimativa_brt_base t
	JOIN linhas_operadoras_modo t_l
	ON (t_l.cd_linha, t_l.cd_operadora) = (t.cd_linha, t.cd_operadora) AND t_l.tp_modo = 'BRT'
	LEFT JOIN analise_trans_estimativa_brt_base tA -- busca de transação anterior na mesma viagem
    ON (t.nr_cartao, date(t.dt_transacao)) = (tA.nr_cartao, date(tA.dt_transacao)) -- para mesma data e mesmo cartão
    	AND tA.nr_trans = t.nr_trans - 1 AND 
        ((HOUR(t.dt_transacao) IN (6,7,17,18) AND TIMESTAMPDIFF(MINUTE, tA.dt_transacao, t.dt_transacao) < @intervalo_pico) -- intervalos horários
        OR (HOUR(t.dt_transacao) NOT IN (6,7,17,18) AND TIMESTAMPDIFF(MINUTE, tA.dt_transacao, t.dt_transacao) < @intervalo))
	LEFT JOIN linhas_operadoras_modo tA_l -- interseção para busca de dados da transação anterior
	ON (tA_l.cd_linha, tA_l.cd_operadora) = (tA.cd_linha, tA.cd_operadora) AND tA_l.tp_modo != 'BRT' -- impossibilita transação no BRT como trecho anterior de mesma viagem
	LEFT JOIN analise_trans_estimativa_brt_base tP -- interseção para busca de dados da transação posterior da mesma viagem ou da próxima viagem
    ON (t.nr_cartao, date(t.dt_transacao)) = (tP.nr_cartao, date(tP.dt_transacao)) 
    	AND tP.nr_trans = t.nr_trans + 1
	LEFT JOIN linhas_operadoras_modo tP_l -- interseção para busca de dados da transação posterior
	ON (tP_l.cd_linha, tP_l.cd_operadora) = (tP.cd_linha, tP.cd_operadora); -- 231287 rows 10 s

-- 2.3. Anulação de trechos em viagens com mais de 3 horas entre A e P
-- 2.3.1. Anulação de A para viagens com mais de 3 horas em que B realizado no pico da tarde
UPDATE analise_viagens_brt
	SET dt_trans_A = NULL, nr_trans_A = NULL, cd_linha_A = NULL, cd_oper_A = NULL, tp_modo_A = NULL, nm_linha_A = NULL
    WHERE tp_trans_P = 'TRNSF' AND timestampdiff(minute, dt_trans_A, dt_trans_P) >= @intervalo_viagem AND hour(dt_trans_B) in (17,18); -- 10

-- 2.3.2. Anulação de A para viagens com mais de 3 horas em que B realizado no pico da manhã
UPDATE analise_viagens_brt
	SET tp_trans_P = NULL, dt_trans_P = NULL, nr_trans_P = NULL, cd_linha_P = NULL, cd_oper_P = NULL, tp_modo_P = NULL, nm_linha_P = NULL
    WHERE tp_trans_P = 'TRNSF' AND timestampdiff(minute, dt_trans_A, dt_trans_P) >= @intervalo_viagem AND hour(dt_trans_B) in (6,7); -- 20

-- 2.4. Distribuição de Viagens por tipo
SELECT 
	FLOOR(COUNT(*)) V, 
    (SELECT FLOOR(COUNT(*)) FROM analise_viagens_brt WHERE nm_linha_A IS NULL and (nm_linha_P IS NULL)) N_B_N,
    (SELECT FLOOR(COUNT(*)) FROM analise_viagens_brt WHERE nm_linha_A IS NULL and (nm_linha_P IS NULL OR tp_trans_P = 'PVG')) N_B_NPpvg,
	(SELECT FLOOR(COUNT(*)) FROM analise_viagens_brt WHERE nm_linha_A IS NOT NULL and (nm_linha_P IS NULL)) A_B_N,
	(SELECT FLOOR(COUNT(*)) FROM analise_viagens_brt WHERE nm_linha_A IS NOT NULL and (nm_linha_P IS NULL OR tp_trans_P = 'PVG')) A_B_NPpvg,
	(SELECT FLOOR(COUNT(*)) FROM analise_viagens_brt WHERE nm_linha_A IS NULL and tp_trans_P = 'TRNSF') N_B_Ptrnsf,
	(SELECT FLOOR(COUNT(*)) FROM analise_viagens_brt WHERE nm_linha_A IS NOT NULL and (nm_linha_P IS NOT NULL and tp_trans_P = 'TRNSF')) A_B_Ptrnsf,
	(SELECT FLOOR(COUNT(*)) FROM analise_viagens_brt WHERE nm_linha_A IS NULL and tp_trans_P = 'PVG') N_B_Ppvg,
	(SELECT FLOOR(COUNT(*)) FROM analise_viagens_brt WHERE nm_linha_A IS NOT NULL and (nm_linha_P IS NOT NULL and tp_trans_P = 'PVG')) A_B_Ppvg
	FROM analise_viagens_brt;
    
SELECT FLOOR(COUNT(*)/@dias_estudados) V, 
    (SELECT FLOOR(COUNT(*)/@dias_estudados) FROM analise_viagens_brt WHERE nm_linha_A IS NULL and (nm_linha_P IS NULL OR tp_trans_P = 'PVG')) N_B_N,
	(SELECT FLOOR(COUNT(*)/@dias_estudados) FROM analise_viagens_brt WHERE nm_linha_A IS NOT NULL and (nm_linha_P IS NULL OR tp_trans_P = 'PVG')) A_B_N,
	(SELECT FLOOR(COUNT(*)/@dias_estudados) FROM analise_viagens_brt WHERE nm_linha_A IS NULL and tp_trans_P = 'TRNSF') N_B_Ptrnsf,
	(SELECT FLOOR(COUNT(*)/@dias_estudados) FROM analise_viagens_brt WHERE nm_linha_A IS NOT NULL and (nm_linha_P IS NOT NULL and tp_trans_P = 'TRNSF')) A_B_Ptrnsf,
	(SELECT FLOOR(COUNT(*)/@dias_estudados) FROM analise_viagens_brt WHERE nm_linha_A IS NULL and tp_trans_P = 'PVG') N_B_Ppvg,
	(SELECT FLOOR(COUNT(*)/@dias_estudados) FROM analise_viagens_brt WHERE nm_linha_A IS NOT NULL and (nm_linha_P IS NOT NULL and tp_trans_P = 'PVG')) A_B_Ppvg
	FROM analise_viagens_brt;

-- ETAPA 2A. CRIAÇÃO DA BASE DE DADOS COM TRANSAÇÕES ENVOLVIDAS NAS VIAGENS REALIZADAS NO BRT
-- 2A.1. Criação da Tabela de Transações envolvidas em Viagens realizadas no BRT
DROP TABLE IF EXISTS analise_trans_estimativa_brt;
CREATE TABLE analise_trans_estimativa_brt LIKE analise_trans_estimativa_brt_base;
TRUNCATE analise_trans_estimativa_brt;

-- 2A.2. Inserção das transações
INSERT IGNORE INTO analise_trans_estimativa_brt
	SELECT DISTINCT t.* 
	FROM analise_trans_estimativa_brt_base t
	JOIN analise_viagens_brt v
	ON t.nr_trans = v.nr_trans_B 
		OR t.nr_trans = v.nr_trans_A 
		OR (t.nr_trans = v.nr_trans_P AND v.tp_trans_P = 'TRNSF');

-- 2A.3. Comparação de cadeias de viagens da base e aquela que será utilizada para estimativa:
SELECT T1.c, CASE WHEN a.c_qtd_ancora != 0 THEN 1 ELSE 0 END c_anc, 
	T1.dt, T2.ch_modo cadeia_base, T2.ch_tempo tempo_base, T1.qtd qtd_estimativa, T1.ch_modo cadeia_estimativa, T1.ch_tempo tempo_estimativa
	FROM (
		SELECT c, date(dt_transacao) dt, COUNT(*) qtd, 
	    group_concat(tp_modo_s ORDER BY nr_trans ASC SEPARATOR ',') ch_modo, 
		group_concat(timediff ORDER BY nr_trans ASC SEPARATOR ',') ch_tempo
		FROM 
		(SELECT t.nr_cartao c, t.dt_transacao, t.nr_trans, l.tp_modo_s,
	      	TIMESTAMPDIFF(minute,		
				(SELECT dt_transacao
					FROM analise_trans_estimativa_brt 
					WHERE nr_trans = t.nr_trans - 1 AND nr_cartao = t.nr_cartao AND date(dt_transacao) = date(t.dt_transacao)),
				dt_transacao) AS timediff
			FROM analise_trans_estimativa_brt t
			JOIN linhas_operadoras_modo l
			ON l.cd_linha = t.cd_linha AND l.cd_operadora = t.cd_operadora) d
			GROUP BY c, date(dt_transacao)) T1
	JOIN (
		SELECT c, date(dt_transacao) dt, COUNT(*), 
	    group_concat(tp_modo_s ORDER BY nr_trans ASC SEPARATOR ',') ch_modo, 
		group_concat(timediff ORDER BY nr_trans ASC SEPARATOR ',') ch_tempo
		FROM 
		(SELECT t.nr_cartao c, t.dt_transacao, t.nr_trans, l.tp_modo_s,
	      	TIMESTAMPDIFF(minute,		
				(SELECT dt_transacao
					FROM analise_trans_estimativa_brt_base 
					WHERE nr_trans = t.nr_trans - 1 AND nr_cartao = t.nr_cartao AND date(dt_transacao) = date(t.dt_transacao)),
				dt_transacao) AS timediff
		FROM analise_trans_estimativa_brt_base t
		JOIN linhas_operadoras_modo l
		ON l.cd_linha = t.cd_linha AND l.cd_operadora = t.cd_operadora) d
		GROUP BY c, date(dt_transacao)) T2
	ON (T1.c, T1.dt) = (T2.c, T2.dt)
	JOIN analise_brt_cartao_ancora a
	ON T1.c = a.nr_cartao
	ORDER BY T1.c;

-- ETAPA 2B. CONSTRUÇÃO DA TABELA COM PARES A-B PARA ESTIMATIVA DE PROXIMIDADE EM LINHAS CAPILARES
-- 2B.1. Criação da Tabela com Pares A-B e respectivo Quantitativo
DROP TABLE linhas_dbq_estimativa_BRT;
CREATE TABLE IF NOT EXISTS linhas_dbq_estimativa_BRT(
	cd_linha varchar(5),
	tp_modo varchar(12),
	nm_linha varchar(70),
	nm_estc_BRT_dbq varchar(70),
	prob int,
    prob_perc float,
	KEY (cd_linha)
);

TRUNCATE table linhas_dbq_estimativa_BRT;

-- 2B.2. Preenchimento da Tabela com Pares A-B e respectivo Quantitativo
INSERT IGNORE INTO linhas_dbq_estimativa_BRT
SELECT cd_linha_A, tp_modo_A, nm_linha_A, nm_linha_B, COUNT(*), NULL
FROM analise_viagens_brt
WHERE nm_linha_A is not null
GROUP BY cd_linha_A, cd_linha_B
ORDER BY cd_linha_A, COUNT(*) desc;

-- 2B.2.1. Preenchimento do percentual de cada Par A-B para o total de transações realizadas em A
UPDATE linhas_dbq_estimativa_BRT l
JOIN (SELECT cd_linha, SUM(prob) probtot FROM linhas_dbq_estimativa_BRT ltot group by cd_linha) ltot
ON l.cd_linha = ltot.cd_linha
SET l.prob_perc = l.prob/ltot.probtot;

-- 2B.3. Seleção dos Pares A-B que respeitam aos critérios de mínimo de uilização e proximidade
-- 2B.3.1. Todos pares A-B para A que repeita critérios
SELECT @dias_estudados := COUNT(distinct date(dt)) from analise_viagens_brt;
SET @utilizacao_AB_dia = 5;
SET @utilizacao_AB = @utilizacao_AB_dia * @dias_estudados;
SET @proporcao_AB = 0.5;

SELECT * FROM linhas_dbq_estimativa_BRT
WHERE cd_linha IN 
	(SELECT cd_linha FROM linhas_dbq_estimativa_BRT
	WHERE prob > @utilizacao_AB and prob_perc > @proporcao_AB)
GROUP BY nm_linha
ORDER BY cd_linha, prob DESC;

-- 2B.3.2. Pares A-B que respeitam critérios
SELECT cd_linha, tp_modo, trim(nm_linha),  trim(nm_estc_BRT_dbq), floor(prob/@dias_estudados) prob_dia, prob_perc FROM linhas_dbq_estimativa_BRT
WHERE prob > @utilizacao_AB and prob_perc > @proporcao_AB
GROUP BY nm_linha
ORDER BY prob_dia DESC;

-- ETAPA 3. ESTIMATIVA EMBARQUE E DESEMBARQUE
-- 3.0. Reset dos desembarques
UPDATE analise_viagens_brt
	SET nm_linha_dbq_B = NULL;

-- 3.1. Estimativa Desembarque para Viagens com P
-- 3.1.1. Estimativa Desembarque para Viagens com P = BRT
UPDATE analise_viagens_brt v
	JOIN linhas_operadoras_modo lP
    ON v.cd_linha_P = lP.cd_linha
	SET nm_linha_dbq_B = IF(nm_linha_B != lP.nm_linha_consolid, lP.nm_linha_consolid, NULL)
	WHERE tp_modo_P = 'BRT'; -- 7807 // 37717

-- 3.1.2. Estimativa Desembarque para Viagens com P onde B-P é similar a A-P realizada no mesmo dia
-- 3.1.2.1. Viagens com P = Trem ou Metrô
UPDATE analise_viagens_brt v
	JOIN linhas_operadoras_modo lP
		ON v.cd_linha_P = lP.cd_linha
		AND v.nm_linha_dbq_B IS NULL
        AND v.tp_trans_P = 'TRNSF' -- apenas viagens em que há registro de transferência
	JOIN analise_viagens_brt vOTR
		ON ((vOTR.nr_cartao, vOTR.dt, vOTR.nr_viagem) = (v.nr_cartao, v.dt, v.nr_viagem - 1) -- viagens anteriores v-1
    	OR (vOTR.nr_cartao, vOTR.dt, vOTR.nr_viagem) = (v.nr_cartao, v.dt, v.nr_viagem + 1)) -- viagens posteriores v+1
	JOIN linhas_operadoras_modo lOTR_A
		ON vOTR.cd_linha_A = lOTR_A.cd_linha 
		AND vOTR.tp_modo_A = v.tp_modo_P AND v.tp_modo_P in ('Trem', 'Metrô') -- viagens ant. ou post. em que modo A_v+1/A_v-1 = modo P_v da viagem analisada
		AND lOTR_A.nm_ramal = lP.nm_ramal -- ramal de A_v+1/A_v-1 e P_v é o mesmo
	JOIN linhas_operadoras_modo lOTR_B -- interseção utilizada para chegar a nome da linha consolidado
		ON vOTR.cd_linha_B = lOTR_B.cd_linha
    SET v.nm_linha_dbq_B = IF(v.nm_linha_B != lOTR_B.nm_linha_consolid, lOTR_B.nm_linha_consolid, NULL); -- 3495 // 16569

/*-- 0104011644508	2015-03-11
select * from analise_viagens_brt where nr_cartao = '0104011644508' ;
select v.* from analise_viagens_Brt v
where (nr_cartao, dt) in (select v.nr_cartao, v.dt from analise_viagens_BRT v
JOIN linhas_operadoras_modo lP
    ON v.cd_linha_P = lP.cd_linha AND v.tp_modo_P in ('Trem', 'Metrô')
	JOIN analise_viagens_brt vOTR
    ON ((vOTR.nr_cartao, vOTR.dt, vOTR.nr_viagem) = (v.nr_cartao, v.dt, v.nr_viagem - 1) 
    	OR (vOTR.nr_cartao, vOTR.dt, vOTR.nr_viagem) = (v.nr_cartao, v.dt, v.nr_viagem + 1))
    	AND vOTR.tp_modo_A = v.tp_modo_P
	JOIN linhas_operadoras_modo lOTR_A
    ON vOTR.cd_linha_A = lOTR_A.cd_linha AND lOTR_A.nm_ramal != lP.nm_ramal
	JOIN linhas_operadoras_modo lOTR_B
    ON vOTR.cd_linha_B = lOTR_B.cd_linha) limit 1000;*/

-- 3.1.2.2. Viagens com P = Ônibus ou Van
UPDATE analise_viagens_brt v
	JOIN linhas_operadoras_modo lP
		ON v.cd_linha_P = lP.cd_linha
		AND v.nm_linha_dbq_B IS NULL
        AND v.tp_modo_P in ('Ônibus I.', 'Ônibus M.', 'Ônibus Alim.', 'Van Interm.') 
        AND v.tp_trans_P = 'TRNSF'
	JOIN analise_viagens_brt vOTR
		ON ((vOTR.nr_cartao, vOTR.dt, vOTR.nr_viagem) = (v.nr_cartao, v.dt, v.nr_viagem - 1) -- viagens anteriores
    	OR (vOTR.nr_cartao, vOTR.dt, vOTR.nr_viagem) = (v.nr_cartao, v.dt, v.nr_viagem + 1)) -- viagens posteriores
    	AND vOTR.tp_modo_A = v.tp_modo_P -- viagens ant. ou post. em que modo A = modo P da viagem analisada
    	AND vOTR.nm_linha_A = v.nm_linha_P -- linha de A_v+1/A_v-1 e P_v é a mesma
	JOIN linhas_operadoras_modo lOTR_B
		ON vOTR.cd_linha_B = lOTR_B.cd_linha
    SET v.nm_linha_dbq_B = IF(v.nm_linha_B != lOTR_B.nm_linha_consolid, lOTR_B.nm_linha_consolid, NULL); -- 1679 // 7865

-- 3.1.3. Estimativa com base em Proximidade
-- 3.1.3.1 Estimativa Desembarque para Viagens com P = estação Trem/Metrô predominantemente próxima a certa estação de BRT
UPDATE analise_viagens_brt v
	JOIN linhas_operadoras_modo lP
		ON v.cd_linha_P = lP.cd_linha 
        AND v.tp_modo_P in ('Trem', 'Metrô')
    	AND lP.nm_linha_consolid IS NOT NULL -- Estações de trem ou metrô que possuem uma estação de BRT predominantemente próxima
        AND v.nm_linha_dbq_B IS NULL
	SET v.nm_linha_dbq_B = IF(v.nm_linha_B != lP.nm_linha_consolid, lP.nm_linha_consolid, NULL); -- 9614 --> 9616 // 48803

/*select v.* from analise_viagens_Brt v
where (nr_cartao, dt) in (select v.nr_cartao, v.dt from analise_viagens_BRT v
	JOIN linhas_operadoras_modo lP
    ON v.cd_linha_P = lP.cd_linha AND v.tp_modo_P in ('Trem', 'Metrô') AND lP.nm_linha_consolid IS NOT NULL
    WHERE v.nm_linha_dbq_B IS NULL
) limit 1000;*/

-- 3.1.3.2. Estimativa Desembarque para Viagens com P = estação Ônibus/Van predominantemente próxima a certa estação de BRT
SELECT @dias_estudados := COUNT(distinct date(dt)) from analise_viagens_brt;
SET @utilizacao_AB_dia = 5;
SET @utilizacao_AB = @utilizacao_AB_dia * @dias_estudados;
SET @proporcao_AB = 0.5;

UPDATE analise_viagens_brt v
	JOIN linhas_operadoras_modo lP
		ON v.cd_linha_P = lP.cd_linha AND v.tp_modo_P in ('Ônibus I.', 'Ônibus M.', 'Ônibus Alim.', 'Van Interm.')
        AND v.nm_linha_dbq_B IS NULL
    JOIN (SELECT * FROM linhas_dbq_estimativa_BRT 
		WHERE prob > @utilizacao_AB AND prob_perc > @proporcao_AB GROUP BY cd_linha ORDER BY cd_linha, prob desc) lde -- Linhas de ônibus / van que possuem uma estação de BRT predominantemente próxima
		ON lde.cd_linha = v.cd_linha_P
	SET v.nm_linha_dbq_B = IF(v.nm_linha_B != lde.nm_estc_BRT_dbq, lde.nm_estc_BRT_dbq, NULL); -- 7238 --> 7239 // 38066

/*select v.* from analise_viagens_Brt v
where (nr_cartao, dt) in (select v.nr_cartao, v.dt from analise_viagens_BRT v
	JOIN linhas_operadoras_modo lP
    ON v.cd_linha_P = lP.cd_linha AND v.tp_modo_P in ('Ônibus I.', 'Ônibus M.', 'Ônibus Alim.', 'Van Interm.')
    JOIN (SELECT * FROM linhas_dbq_estimativa_BRT WHERE prob > 20 AND prob_perc > 0.5 GROUP BY cd_linha ORDER BY cd_linha, prob desc) lde
    ON lde.cd_linha = v.cd_linha_P
    WHERE v.nm_linha_dbq_B IS NOT NULL
) limit 10000;*/

-- 3.2. Estimativa Desembarque para Viagens sem P
-- 3.2.1. Estimativa Desembarque para Viagens sem P onde A-B é similar a B-P de outras viagens do cartão no mesmo dia.
-- 3.2.1.1. Viagens com A = Trem / Metrô
UPDATE analise_viagens_brt v
	JOIN linhas_operadoras_modo lA
		ON v.cd_linha_A = lA.cd_linha
        AND v.nm_linha_dbq_B IS NULL
	JOIN analise_viagens_brt vOTR
		ON ((vOTR.nr_cartao, vOTR.dt, vOTR.nr_viagem) = (v.nr_cartao, v.dt, v.nr_viagem - 1) -- viagens anteriores v-1
    	OR (vOTR.nr_cartao, vOTR.dt, vOTR.nr_viagem) = (v.nr_cartao, v.dt, v.nr_viagem + 1)) -- viagens posteriores v+1
		AND vOTR.tp_trans_P = 'TRNSF'
	JOIN linhas_operadoras_modo lOTR_P
		ON vOTR.cd_linha_P = lOTR_P.cd_linha 
		AND vOTR.tp_modo_P = v.tp_modo_A AND v.tp_modo_A in ('Trem', 'Metrô') -- viagens ant. ou post. em que modo A_v+1/A_v-1 = modo P_v da viagem analisada
		AND lOTR_P.nm_ramal = lA.nm_ramal -- ramal de P_v+1/P_v-1 e A_v é o mesmo
	JOIN linhas_operadoras_modo lOTR_B -- interseção utilizada para chegar a nome da linha consolidado
		ON vOTR.cd_linha_B = lOTR_B.cd_linha
    SET v.nm_linha_dbq_B = IF(v.nm_linha_B != lOTR_B.nm_linha_consolid, lOTR_B.nm_linha_consolid, NULL); -- 201 --> 198 // 933

-- 3.2.1.2. Viagens com A = Ônibus / Van        
UPDATE analise_viagens_brt v
	JOIN linhas_operadoras_modo lA
		ON v.cd_linha_A = lA.cd_linha
		AND v.nm_linha_dbq_B IS NULL
        AND v.tp_modo_A in ('Ônibus I.', 'Ônibus M.', 'Ônibus Alim.', 'Van Interm.') 
	JOIN analise_viagens_brt vOTR
		ON ((vOTR.nr_cartao, vOTR.dt, vOTR.nr_viagem) = (v.nr_cartao, v.dt, v.nr_viagem - 1) -- viagens anteriores
    	OR (vOTR.nr_cartao, vOTR.dt, vOTR.nr_viagem) = (v.nr_cartao, v.dt, v.nr_viagem + 1)) -- viagens posteriores
    	AND vOTR.tp_trans_P = 'TRNSF'
        AND vOTR.tp_modo_P = v.tp_modo_A -- viagens ant. ou post. em que modo A = modo P da viagem analisada
    	AND vOTR.nm_linha_P = v.nm_linha_A -- linha de A_v+1/A_v-1 e P_v é a mesma
	JOIN linhas_operadoras_modo lOTR_B
		ON vOTR.cd_linha_B = lOTR_B.cd_linha
    SET v.nm_linha_dbq_B = IF(v.nm_linha_B != lOTR_B.nm_linha_consolid, lOTR_B.nm_linha_consolid, NULL); -- 342 // 1386
    
-- 3.2.2. Último desembarque do dia = Primeiro embarque do dia
/*UPDATE analise_viagens_brt v
	JOIN analise_viagens_brt vANT
		ON (vANT.nr_cartao, vANT.dt) = (v.nr_cartao, v.dt) 
        AND vANT.nr_viagem = 1 AND vANT.nr_viagem < v.nr_viagem -- primeira viagem do dia
        AND v.nm_linha_dbq_B IS NULL
	JOIN linhas_operadoras_modo lANT_B -- join necessário para usar nome consolidado
		ON vANT.cd_linha_B = lANT_B.cd_linha
    SET v.nm_linha_dbq_B = IF(vANT.nm_linha_dbq_B = v.nm_linha_B, lANT_B.nm_linha_consolid, NULL); -- 3108*/

INSERT INTO analise_viagens_brt_log
SELECT v.* FROM analise_viagens_brt v
	JOIN analise_viagens_brt vANT
		ON (vANT.nr_cartao, vANT.dt) = (v.nr_cartao, v.dt) 
        AND vANT.nr_viagem = 1 AND vANT.nr_viagem < v.nr_viagem -- primeira viagem do dia
        AND v.nm_linha_dbq_B IS NULL
	JOIN linhas_operadoras_modo lANT_B -- join necessário para usar nome consolidado
		ON vANT.cd_linha_B = lANT_B.cd_linha;
        
SELECT * FROM analise_viagens_brt
WHERE (nr_cartao, dt) IN
	(SELECT nr_cartao, dt FROM analise_viagens_brt_log WHERE tp_trans_P is not null);


UPDATE analise_viagens_brt v
	JOIN analise_viagens_brt vANT
		ON (vANT.nr_cartao, vANT.dt) = (v.nr_cartao, v.dt) 
        AND vANT.nr_viagem = 1 AND vANT.nr_viagem < v.nr_viagem -- primeira viagem do dia
        AND v.nm_linha_dbq_B IS NULL
	JOIN linhas_operadoras_modo lANT_B -- join necessário para usar nome consolidado
		ON vANT.cd_linha_B = lANT_B.cd_linha
    SET v.nm_linha_dbq_B = IF(v.nm_linha_B != lANT_B.nm_linha_consolid, lANT_B.nm_linha_consolid, NULL); -- 4093

-- ETAPA 4. PADRÕES DE REGULARIDADE NA ESTIMATIVA DE DESEMBARQUE
-- 4.0. Quantidade de Transações analisadas
-- 4.0.1. Total do BRT
SELECT COUNT(*) t, 
	FLOOR(COUNT(*)/COUNT(DISTINCT date(t.dt_transacao))) t_d, 
	COUNT(DISTINCT t.nr_cartao) c
	FROM analise_trans_so_brt t
	WHERE dayofweek(t.dt_transacao) not in (1,7);

-- 4.0.2. Total dos Cartões que Utilizaram BRT TransCarioca
SELECT COUNT(*) t, 
	FLOOR(COUNT(*)/COUNT(DISTINCT date(t.dt_transacao))) t_d, 
	COUNT(DISTINCT t.nr_cartao) c 
	FROM analise_trans_so_brt t
	JOIN analise_brt_cartao_data_utilizacao_ramais cdr
    ON (t.nr_cartao, date(t.dt_transacao)) = (cdr.nr_cartao, cdr.dt_transacao)
	WHERE dayofweek(t.dt_transacao) not in (1,7) and nm_ramais in ('TransCarioca', 'TOE + TCA')
    ORDER BY t.nr_cartao, t.dt_transacao, t.nr_trans_aplic;

-- 4.1. Análise da Taxa de Estimativa com Ênfase na Regularidade
-- 4.2. Taxa de Estimativa Geral
SELECT COUNT(*) qtd, SUM(case when nm_linha_dbq_B is not null then 1 end) qtd_dbq
	FROM analise_viagens_BRT; 
-- 231287	155432 67%

-- 4.2.1. Taxa de Estimativa para Cartões com e sem Âncora
SELECT CASE WHEN c.c_qtd_ancora != 0 THEN 1 ELSE 0 END ancora, COUNT(distinct v.nr_cartao) c, COUNT(*) v, SUM(case when nm_linha_dbq_B is not null then 1 end) v_dbq
	FROM analise_viagens_BRT v
	JOIN analise_brt_cartao_ancora c
	ON v.nr_cartao = c.nr_cartao
	GROUP BY ancora;
-- 0	69890	44207 -- 63%
-- 1	161397	114239 -- 71%

SELECT c.c_qtd_ancora, COUNT(distinct v.nr_cartao) c, COUNT(*) v, SUM(case when nm_linha_dbq_B is not null then 1 end) v_dbq
	FROM analise_viagens_BRT v
	JOIN analise_brt_cartao_ancora c
	ON v.nr_cartao = c.nr_cartao
	GROUP BY c_qtd_ancora;

-- 4.2.2. Taxa de Estimativa e Percentual de Embarques com Âncora para Cada Estação
SELECT v.nm_linha_B, 
	FLOOR(COUNT(*)/@dias_estudados) qtd,  
	SUM(CASE WHEN nm_linha_dbq_B IS NOT NULL THEN 1 end)/qtd_tot qtd_dbq, 
    SUM(CASE WHEN c.c_qtd_ancora != 0 THEN 1 ELSE 0 END)/qtd_tot qtd_anc
	FROM analise_viagens_BRT v
	JOIN analise_brt_cartao_ancora c
	ON v.nr_cartao = c.nr_cartao
	JOIN (SELECT nm_linha_B, COUNT(*) qtd_tot FROM analise_viagens_BRT GROUP BY nm_linha_B) vTOT
    ON vTOT.nm_linha_B = v.nm_linha_B
    GROUP BY v.nm_linha_B;

-- 4.2.3. Gráfico par B-DBQ x Âncora
SELECT nm_linha_B, nm_linha_dbq_B, (COUNT(*)/@dias_estudados) qtd_dia, (SUM(CASE WHEN c.c_qtd_ancora != 0 THEN 1 ELSE 0 END)/@dias_estudados) qtd_anc_dia
	FROM analise_viagens_BRT v
	JOIN analise_brt_cartao_ancora c
	ON v.nr_cartao = c.nr_cartao
	WHERE nm_linha_dbq_B IS NOT NULL
	GROUP BY nm_linha_B, nm_linha_dbq_B;
    
-- 4.2.4. Estimativa por Horário de Pico:
SELECT hour(dt_trans_B) hr, CASE WHEN c.c_qtd_ancora != 0 THEN 1 ELSE 0 END ancora, COUNT(*) qtd, SUM(case when nm_linha_dbq_B is not null then 1 end) qtd_dbq
	FROM analise_viagens_BRT v
	JOIN analise_brt_cartao_ancora c
	ON v.nr_cartao = c.nr_cartao
    WHERE hour(dt_trans_B) in (6, 17)
    GROUP BY hr; 

SELECT hour(dt_trans_B) hr, count(*), SUM(case when nm_linha_dbq_B is not null then 1 end) qtd_dbq 
	FROM analise_viagens_BRT v
	JOIN analise_brt_cartao_ancora c
	ON v.nr_cartao = c.nr_cartao
    WHERE hour(dt_trans_B) in (6, 17)
		AND tp_trans_P IS NOT NULL
    GROUP BY hr; 

-- 4.3. Análise da Distribuição Espacial de Viagens com Desembarque Estimado
-- 4.3.1. Relação ED por Ramal
-- 4.3.1.1. Set do Ramal do Terminal Alvorada para TransCarioca
UPDATE `bu`.`LINHAS_OPERADORAS_MODO` SET `nm_ramal`='TransCarioca' WHERE `cd_linha`='09101';

-- 4.3.1.2. Consulta para Qtd por Ramal
SELECT hour(dt_trans_B) hr, lO.nm_ramal ramal_O, lD.nm_ramal ramal_D, COUNT(*), SUM(CASE WHEN c.c_qtd_ancora != 0 THEN 1 ELSE 0 END) qtd_anc
	FROM analise_viagens_BRT v
	JOIN analise_brt_cartao_ancora c
	ON v.nr_cartao = c.nr_cartao
	JOIN linhas_operadoras_modo lO
	ON v.cd_linha_B = lO.cd_linha
	JOIN linhas_operadoras_modo lD
	ON v.nm_linha_dbq_B = lD.nm_linha_consolid AND lD.tp_modo = 'BRT'
	WHERE hour(dt_trans_B) IN (6, 17) AND nm_linha_dbq_B IS NOT NULL
	GROUP BY hr, lO.nm_ramal, lD.nm_ramal;

-- 4.3.1.3. Set do Ramal do Terminal Alvorada para TransOeste
UPDATE `bu`.`LINHAS_OPERADORAS_MODO` SET `nm_ramal`='TransOeste' WHERE `cd_linha`='09164';

-- 4.3.2. Fluxo em cada sentido a partir de Par ED no Pico da Manhã e da Tarde
SELECT hour(dt_trans_B) hr, nm_linha_B, nm_linha_dbq_B, (COUNT(*)/@dias_estudados) qtd_dia, (SUM(CASE WHEN c.c_qtd_ancora != 0 THEN 1 ELSE 0 END)/@dias_estudados) qtd_anc_dia
	FROM analise_viagens_BRT v
	JOIN analise_brt_cartao_ancora c
	ON v.nr_cartao = c.nr_cartao
	WHERE nm_linha_dbq_B IS NOT NULL
    AND hour(dt_trans_B) in (6, 17)
	GROUP BY hour(dt_trans_B), nm_linha_B, nm_linha_dbq_B;


-- 4.3.3. Divisão modal
SELECT hour(dt_trans_B) hr, 'A' trecho,
		CASE WHEN tp_modo_A IS NULL THEN 'Inicial' 
			WHEN tp_modo_A in('Metrô', 'Ônibus Alim.', 'Ônibus M.', 'Van Interm.') THEN 'Outro'
			ELSE tp_modo_A END modo, COUNT(*)/qtd_tot perc
		FROM analise_viagens_BRT v
		JOIN (SELECT hour(dt_trans_B) hr, COUNT(*) qtd_tot
			FROM analise_viagens_BRT
			WHERE nm_linha_dbq_B IS NOT NULL AND hour(dt_trans_B) in (6, 17)
			GROUP BY hr) vTOT
		ON hour(v.dt_trans_B) = vTOT.hr
		WHERE v.nm_linha_dbq_B IS NOT NULL AND hour(dt_trans_B) in (6, 17)
		GROUP BY hr, modo
	UNION ALL
	SELECT hour(dt_trans_B) hr, 'P' trecho,
		CASE WHEN tp_modo_P IS NULL OR tp_trans_P = 'PVG' THEN 'Final' 
			WHEN tp_modo_P in('Metrô', 'Ônibus Alim.', 'Ônibus M.', 'Van Interm.') THEN 'Outro'
			ELSE tp_modo_P END modo, COUNT(*)/qtd_tot perc
		FROM analise_viagens_BRT v
		JOIN (SELECT hour(dt_trans_B) hr, COUNT(*) qtd_tot
			FROM analise_viagens_BRT
			WHERE nm_linha_dbq_B IS NOT NULL AND hour(dt_trans_B) in (6, 17)
			GROUP BY hr) vTOT
		ON hour(v.dt_trans_B) = vTOT.hr
		WHERE v.nm_linha_dbq_B IS NOT NULL AND hour(dt_trans_B) in (6, 17)
		GROUP BY hr, modo
		ORDER BY hr asc, trecho, perc DESC;


-- 4.3.4. Representação das Origens (Trnsf. e Absoluta) e Destinos (Trnsf. e Absoluto) para cada estação para Mapa
DROP TABLE analise_origem_destino;

-- 4.3.4.1. Origem
CREATE TABLE analise_origem_destino AS
SELECT nm_linha_B, hour(dt_trans_B) hr, 'O' AS 'OD', l.cd_georef, l.nm_ramal, CASE WHEN nm_linha_A IS NULL THEN 'ABS.' ELSE 'TRNSF.' END tp, COUNT(*) qtd
FROM analise_viagens_BRT v
JOIN linhas_operadoras_modo l
ON v.cd_linha_B = l.cd_linha
WHERE hour(dt_trans_B) IN (6, 17) AND nm_linha_dbq_B IS NOT NULL
GROUP BY nm_linha_B, hour(dt_trans_B), l.cd_georef, l. nm_ramal, tp;

-- 4.3.4.2. Destino
INSERT INTO analise_origem_destino
SELECT nm_linha_dbq_B, hour(dt_trans_B) hr, 'D',  l.cd_georef, l.nm_ramal, CASE WHEN tp_trans_P = 'TRNSF' THEN 'TRNSF.' ELSE 'ABS.' END tp, COUNT(*) qtd
FROM analise_viagens_BRT v
JOIN (SELECT * FROM linhas_operadoras_modo
	WHERE tp_modo = 'BRT'
	GROUP BY nm_linha_consolid, cd_georef, nm_ramal) l
ON v.nm_linha_dbq_B = l.nm_linha_consolid AND l.tp_modo = 'BRT'
WHERE hour(dt_trans_B) IN (6, 17) AND nm_linha_dbq_B IS NOT NULL
GROUP BY nm_linha_dbq_B, hour(dt_trans_B), l.cd_georef, l.nm_ramal, tp;

-- 4.3.4.3. Para Exportar:
SELECT * FROM analise_origem_destino;







--
--
--
-- consultas outras
/*
-- Amostra de viagens
select nr_cartao, dt, TIME(dt_trans_A) tm_A, tp_modo_A, nm_linha_A, TIME(dt_trans_B) tm_B, tp_modo_B, nm_linha_B, tp_trans_P, time(dt_trans_P) tm_P, tp_modo_P, nm_linha_P
from analise_viagens_brt limit 1000;
select * from analise_viagens_BRT  where nr_viagem = 2 limit 10000;
select nr_cartao, dt, COUNT(*) from analise_viagens_BRT  group by nr_cartao, dt having COUNT(*) > 1;

SELECT * FROM analise_viagens_brt WHERE nm_linha_A IS NOT NULL and (nm_linha_P IS NOT NULL and tp_trans_P = 'TRNSF');

-- busca das transacoes da base para um cartão qualquer
SELECT t.nr_cartao, a.c_qtd_ancora, date(dt_transacao) dt, dt_transacao, nr_trans_aplic, nr_trans, l.tp_modo, l.nm_linha_DETRO, l.nr_linha_DETRO
	FROM analise_trans_estimativa_brt_base t
	JOIN linhas_operadoras_modo l ON l.cd_linha = t.cd_linha
    JOIN analise_brt_cartao_ancora a ON a.nr_cartao = t.nr_cartao
	WHERE t.nr_cartao = '0104011644508';

SELECT * FROM analise_viagens_brt 
where nr_cartao in(
select nr_cartao from analise_viagens_brt where tp_modo_P = 'Trem' and nm_linha_P like 'M. de Madu%')
limit 1000;

-- Viagens com maiores diferenças temporais entre trechos P e A
select timestampdiff(minute, dt_trans_A, dt_trans_P) tm_df, nr_cartao, dt, TIME(dt_trans_A) tm_A, tp_modo_A, nm_linha_A, TIME(dt_trans_B) tm_B, tp_modo_B, nm_linha_B, tp_trans_P, time(dt_trans_P) tm_P, tp_modo_P, nm_linha_P
from analise_viagens_brt 
where nm_linha_A IS NOT NULL and (nm_linha_P IS NOT NULL and tp_trans_P = 'TRNSF')
ORDER BY TM_DF DESC;

-- Pares de linha A - linha B
select tp_modo_A , nm_linha_A, nm_linha_B, nm_linha_P, COUNT(*) from analise_viagens_brt 
where nm_linha_A is not null and tp_trans_P = 'TRNSF' group by nm_linha_A, nm_linha_B, nm_linha_P order by nm_linha_A, COUNT(*) desc;

select * from analise_viagens_brt where nm_linha_A =  'Duque de Caxias - Pilares (via Cidade Alta)                 ' and (nm_linha_B = 'Olaria - Cacique de Ramos' or nm_linha_B = 'Pastor José Santos');

select nm_linha_A, nm_linha_B from
(select nm_linha_A, nm_linha_B, COUNT(*) from analise_viagens_brt 
where nm_linha_A is not null group by nm_linha_A, nm_linha_B order by COUNT(*) * rand() desc) t;

select tp_modo_A , nm_linha_A, nm_linha_B, COUNT(*) from analise_viagens_brt 
where nm_linha_A is not null group by nm_linha_A, nm_linha_B order by nm_linha_A, COUNT(*) desc;

SELECT * from
analise_viagens_BRt 
where (nr_cartao, dt) in (select v.nr_cartao, v.dt from analise_viagens_brt v
	JOIN analise_viagens_brt vANT
    ON (vANT.nr_cartao, vANT.dt, vANT.nr_viagem) = (v.nr_cartao, v.dt, v.nr_viagem - 1)
	WHERE v.dt_trans_P IS NULL and vANT.nm_linha_dbq_B != v.nm_linha_B);

select * from analise_viagens_brt
where nm_linha_b = nm_linha_dbq_B;
UPDATE analise_viagens_brt v
SET nm_linha_dbq_B = NULL
WHERE nm_linha_b = nm_linha_dbq_B;


select * from linhas_operadoras_modo;

select * FROM analise_viagens_brt v
WHERE nm_linha_dbq_B is null and tp_modo_P in ('Trem', 'Metrô');

select dt, nr_viagem, nm_linha_A, nm_linha_B, nm_linha_dbq_B, nm_linha_P 
from analise_viagens_BRT where (nr_cartao, dt) in (SELECT nr_cartao, dt FROM analise_viagens_brt v
WHERE nm_linha_dbq_B is null and tp_modo_P in ('Trem', 'Metrô'))  limit 1000;

select dt, nr_viagem, nm_linha_A, nm_linha_B, nm_linha_dbq_B, nm_linha_P 
from analise_viagens_BRT where (nr_cartao, dt) in (select v.nr_cartao, v.dt
from analise_viagens_brt v
	JOIN analise_viagens_brt vANT
    ON ((vANT.nr_cartao, vANT.dt, vANT.nr_viagem) = (v.nr_cartao, v.dt, v.nr_viagem - 1) 
    	OR (vANT.nr_cartao, vANT.dt, vANT.nr_viagem) = (v.nr_cartao, v.dt, v.nr_viagem + 1))
    	AND vANT.tp_modo_A = v.tp_modo_P AND vANT.nm_linha_A = v.nm_Linha_P
	WHERE v.tp_modo_P in ('Ônibus I.', 'Ônibus M.', 'Ônibus Alim.', 'Van Interm.')) limit 1000;

select *
FROM analise_viagens_brt v
	JOIN linhas_dbq_estimativa_BRT led
    ON v.cd_linha_P = led.cd_linha
WHERE nm_linha_dbq_B is null and tp_modo_P in ('Trem', 'Metrô')
AND NM_LINHA_p NOT IN ('Madureira', 'M. de Madureira', 'Penha') limit 1000;

select dt, time(dt_trans_B), nr_viagem, nm_linha_A, nm_linha_B, nm_linha_dbq_B, nm_linha_P 
from analise_viagens_BRT where (nr_cartao, dt) in 
(
select v.nr_cartao, v.dt
	from analise_viagens_brt v
	JOIN linhas_operadoras_modo lP
    ON v.cd_linha_P = lP.cd_linha
	JOIN analise_viagens_brt vANT
    ON (vANT.nr_cartao, vANT.dt, vANT.nr_viagem) = (v.nr_cartao, v.dt, v.nr_viagem + 1) AND vANT.tp_modo_A = v.tp_modo_P
	JOIN linhas_operadoras_modo lANT
    ON vANT.cd_linha_A = lANT.cd_linha
	WHERE v.tp_modo_P in ('Trem', 'Metrô'));
    ;

-- CONTAGEM

select cd_linha_P, nm_linha_P, COUNT(*) from analise_viagens_BRT where nm_linha_dbq_B is null and tp_modo_P = 'trem' 
group by cd_linha_P order by COUNT(*) desc limit 1000 ;

select cd_linha_A, nm_linha_A, COUNT(*) from analise_viagens_BRT where nm_linha_dbq_B is null and tp_modo_A = 'trem' 
group by cd_linha_A order by COUNT(*) desc limit 1000 ;

-- selecionar todas as viagens para cartoes que tem viagem sem desembarque encontrado
select nr_cartao, dt, nr_viagem, time(dt_trans_A) tmA, nm_linha_A, time(dt_trans_B) tmB, nm_linha_B, nm_linha_dbq_B, time(dt_trans_P) tmP, tp_modo_P, nm_linha_P  from analise_viagens_BRT
where nr_cartao in
(select nr_cartao from analise_viagens_brt where nm_linha_dbq_B is null) limit 1000;


SELECT tp_trans_P, COUNT(*) FROM analise_viagens_BRT v
WHERE nm_linha_dbq_B like '%Fund%' and hour(dt_trans_B) IN (6)
group by tp_trans_P;

SELECT nm_linha_B, tp_trans_P, nm_linha_P, COUNT(*) FROM analise_viagens_BRT v
WHERE nm_linha_dbq_B like '%Fund%' and hour(dt_trans_B) IN (6)
group by nm_linha_B, tp_trans_P, nm_linha_P
order by tp_trans_P desc, COUNT(*) desc;
*/