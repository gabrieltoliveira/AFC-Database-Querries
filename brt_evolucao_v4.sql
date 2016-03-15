
-- Dados Consolidados para Análise da Evolução da rede de BRTs
-- Eventuais comandos necessários nessa etapa:
truncate dados_consolid_geral;
truncate analise_evolucao_BRT;
drop table analise_evolucao_BRT;

-- 1. Criação da Tabela de Consolidação de Dados Históricos
create table dados_consolid_geral like dados_consolid_20140207_20140306;

-- 2. População da Tabela de Consolidação de Dados Históricos
replace into dados_consolid_geral select * from dados_consolid_20140207_20140306;
replace into dados_consolid_geral select * from dados_consolid_20140307_20140403;
replace into dados_consolid_geral select * from dados_consolid_20140404_20140501;
replace into dados_consolid_geral select * from dados_consolid_20140502_20140605;
replace into dados_consolid_geral select * from dados_consolid_20140606_20140703;
replace into dados_consolid_geral select * from dados_consolid_20140704_20140731;
replace into dados_consolid_geral select * from dados_consolid_20140801_20140904;
replace into dados_consolid_geral select * from dados_consolid_20140905_20141002;
replace into dados_consolid_geral select * from dados_consolid_20141003_20141106;
replace into dados_consolid_geral select * from dados_consolid_20141107_20141204;
replace into dados_consolid_geral select * from dados_consolid_20141205_20141231;
replace into dados_consolid_geral select * from dados_consolid_20150101_20150205;
replace into dados_consolid_geral select * from dados_consolid_20150206_20150305;
replace into dados_consolid_geral select * from dados_consolid_20150206_20150305;
replace into dados_consolid_geral select * from dados_consolid_20150306_20150402;
replace into dados_consolid_geral select * from dados_consolid_20150403_20150430;
replace into dados_consolid_geral select * from dados_consolid_20150501_20150604;

-- 3. Criação e População da Tabela de Dados de Evolução da Demanda do SBE e da Rede de BRTs
create table analise_evolucao_BRT (
	dt DATE,
	trans_BUE INT,
	trans_BRT_TC INT,
	trans_BRT_TO INT,
	trans_BRT_Term_Alv INT
);

-- 3.1 População da Tabela com Dados de Evolução da Demanda do SBE

insert ignore into analise_evolucao_BRT
select date(dt_utilizacao), sum(qt_trans), null, null, null
from dados_consolid_geral
where date(dt_utilizacao) >= '2014-03-01' and date(dt_utilizacao) < '2015-04-01'
group by date(dt_utilizacao);

-- 3.2 Criação e População de Tabela Temporária para Dados de Evolução da Demanda da Rede de BRTs
/* DROP TABLE IF EXISTS linhas_BRT;

create table linhas_BRT(
cd_linha varchar(6),
nr_linha_DETRO varchar(6),
nm_linha_DETRO varchar(60),
tp_modo varchar(10),
nm_ramal varchar(60),
PRIMARY KEY (cd_linha, nm_ramal)
)
DEFAULT CHARACTER SET = utf8;

LOAD DATA INFILE
	'C:\\linhas_BRT.csv' 
	INTO TABLE linhas_BRT
	FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n'; */

create temporary table analise_evolucao_BRT_TC_tmp 
as select date(dt_utilizacao) dt, sum(qt_trans) trans_BRT_TC
from dados_consolid_geral d
join linhas_operadoras_modo_BRT l
on d.cd_linha = l.cd_linha and nm_ramal = 'TransCarioca'
where date(dt_utilizacao) >= '2014-03-01' and date(dt_utilizacao) < '2015-04-01'
group by date(dt_utilizacao);

create temporary table analise_evolucao_BRT_TO_tmp 
as select date(dt_utilizacao) dt, sum(qt_trans) trans_BRT_TO
from dados_consolid_geral d
join linhas_operadoras_modo_BRT l
on d.cd_linha = l.cd_linha and nm_ramal = 'TransOeste'
where date(dt_utilizacao) >= '2014-03-01' and date(dt_utilizacao) < '2015-04-01'
group by date(dt_utilizacao);

create temporary table analise_evolucao_BRT_Term_Alv_tmp 
as select date(dt_utilizacao) dt, sum(qt_trans) trans_BRT_Term_Alv
from dados_consolid_geral d
join linhas_operadoras_modo_BRT l
on d.cd_linha = l.cd_linha and nm_linha_DETRO like '%alvorada%'
where date(dt_utilizacao) >= '2014-03-01' and date(dt_utilizacao) < '2015-04-01'
group by date(dt_utilizacao);

-- 3.3 Consolidação da Tabela de Dados de Evolução da Demanda do SBE e da rede de BRTs
update analise_evolucao_BRT t
left join analise_evolucao_BRT_TC_tmp tmp
	on t.dt = tmp.dt
set t.trans_BRT_TC = tmp.trans_BRT_TC;

update analise_evolucao_BRT t
set t.trans_BRT_TC = 0
where trans_BRT_TC is null;

update analise_evolucao_BRT t
left join analise_evolucao_BRT_TO_tmp tmp
	on t.dt = tmp.dt
set t.trans_BRT_TO = tmp.trans_BRT_TO;

update analise_evolucao_BRT t
set t.trans_BRT_TO = 0
where trans_BRT_TO is null;

update analise_evolucao_BRT t
left join analise_evolucao_BRT_Term_Alv_tmp tmp
	on t.dt = tmp.dt
set t.trans_BRT_Term_Alv = tmp.trans_BRT_Term_Alv;

update analise_evolucao_BRT t
set t.trans_BRT_Term_Alv = 0
where trans_BRT_Term_Alv is null;

-- Comandos de Chamada de Tabelas Consolidadas Finais
select * from linhas_operadoras_modo_BRT;
select * from analise_evolucao_BRT;