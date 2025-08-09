-- ===========================================
-- Script de criação do Data Mart - IDA
-- ===========================================

-- Criar schema separado para organização
CREATE SCHEMA IF NOT EXISTS datamart_ida;

-- ========================
-- Tabela Dimensão Serviço
-- ========================
CREATE TABLE datamart_ida.dim_servico (
    id_servico SERIAL PRIMARY KEY,
    nome_servico VARCHAR(50) NOT NULL UNIQUE
);
COMMENT ON TABLE datamart_ida.dim_servico IS 'Dimensão com os tipos de serviço (SMP, SCM, STFC)';
COMMENT ON COLUMN datamart_ida.dim_servico.nome_servico IS 'Nome do serviço';

-- ========================
-- Tabela Dimensão Grupo Econômico
-- ========================
CREATE TABLE datamart_ida.dim_grupo_economico (
    id_grupo SERIAL PRIMARY KEY,
    nome_grupo VARCHAR(100) NOT NULL UNIQUE
);
COMMENT ON TABLE datamart_ida.dim_grupo_economico IS 'Dimensão com os grupos econômicos (ALGAR, CLARO, etc.)';
COMMENT ON COLUMN datamart_ida.dim_grupo_economico.nome_grupo IS 'Nome do grupo econômico';

-- ========================
-- Tabela Dimensão Tempo
-- ========================
CREATE TABLE datamart_ida.dim_tempo (
    id_tempo SERIAL PRIMARY KEY,
    ano INT NOT NULL,
    mes INT NOT NULL,
    mes_ano DATE NOT NULL UNIQUE
);
COMMENT ON TABLE datamart_ida.dim_tempo IS 'Dimensão de tempo para consultas mensais';
COMMENT ON COLUMN datamart_ida.dim_tempo.mes_ano IS 'Primeiro dia do mês correspondente ao dado';

-- ========================
-- Tabela Fato IDA
-- ========================
CREATE TABLE datamart_ida.fato_ida (
    id_fato SERIAL PRIMARY KEY,
    id_servico INT NOT NULL REFERENCES datamart_ida.dim_servico(id_servico),
    id_grupo INT NOT NULL REFERENCES datamart_ida.dim_grupo_economico(id_grupo),
    id_tempo INT NOT NULL REFERENCES datamart_ida.dim_tempo(id_tempo),
    indicador VARCHAR(200) NOT NULL,
    valor NUMERIC(20,3) NOT NULL
);
COMMENT ON TABLE datamart_ida.fato_ida IS 'Tabela fato contendo os valores de indicadores IDA';
COMMENT ON COLUMN datamart_ida.fato_ida.indicador IS 'Nome do indicador (ex: Taxa de Resolvidas em 5 dias úteis)';
COMMENT ON COLUMN datamart_ida.fato_ida.valor IS 'Valor do indicador para o mês e grupo econômico';
