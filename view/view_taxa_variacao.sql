CREATE EXTENSION IF NOT EXISTS tablefunc;

-- Cria (ou substitui) a view final com a análise de variação
CREATE OR REPLACE VIEW datamart_ida.vw_variacao_ida AS
WITH
-- 1. Filtra a tabela fato apenas para o indicador de interesse de forma flexível
dados_indicador AS (
    SELECT
        t.mes_ano,
        g.nome_grupo,
        f.valor
    FROM
        datamart_ida.fato_ida f
    JOIN
        datamart_ida.dim_tempo t ON f.id_tempo = t.id_tempo
    JOIN
        datamart_ida.dim_grupo_economico g ON f.id_grupo = g.id_grupo
    WHERE
        f.indicador ILIKE '%Resolvidas em 5 dias úteis%'
),

-- 2. Calcula a variação mensal para cada grupo econômico individualmente
variacao_por_grupo AS (
    SELECT
        mes_ano,
        nome_grupo,
        -- Usa a função LAG para buscar o valor do mês anterior do mesmo grupo
        COALESCE(
            (valor - LAG(valor, 1) OVER (PARTITION BY nome_grupo ORDER BY mes_ano)) * 100.0 / NULLIF(LAG(valor, 1) OVER (PARTITION BY nome_grupo ORDER BY mes_ano), 0),
            0
        ) AS taxa_variacao
    FROM
        dados_indicador
),

-- 3. Calcula a taxa de variação média geral para cada mês
media_mensal AS (
    SELECT
        mes_ano,
        ROUND(AVG(taxa_variacao), 2) AS taxa_variacao_media
    FROM
        variacao_por_grupo
    GROUP BY
        mes_ano
)

-- 4. Junta a média com os dados pivotados dos grupos
SELECT
    m.mes_ano,
    m.taxa_variacao_media,
    -- Selecionando explicitamente as colunas para omitir 'mes_ano_pivot'
    ct."ALGAR",
    ct."CLARO",
    ct."EMBRATEL",
    ct."GVT",
    ct."NET",
    ct."NEXTEL",
    ct."OI",
    ct."SERCOMTEL",
    ct."SKY",
    ct."TIM",
    ct."VIVO"
FROM
    media_mensal m
LEFT JOIN
    crosstab(
        'WITH
            dados_indicador_interno AS (
                SELECT t.mes_ano, g.nome_grupo, f.valor
                FROM datamart_ida.fato_ida f
                JOIN datamart_ida.dim_tempo t ON f.id_tempo = t.id_tempo
                JOIN datamart_ida.dim_grupo_economico g ON f.id_grupo = g.id_grupo
                WHERE f.indicador ILIKE ''%Resolvidas em 5 dias úteis%''
            ),
            variacao_por_grupo_interno AS (
                SELECT mes_ano, nome_grupo,
                    COALESCE((valor - LAG(valor, 1) OVER (PARTITION BY nome_grupo ORDER BY mes_ano)) * 100.0 / NULLIF(LAG(valor, 1) OVER (PARTITION BY nome_grupo ORDER BY mes_ano), 0), 0) AS taxa_variacao
                FROM dados_indicador_interno
            ),
            media_mensal_interno AS (
                SELECT mes_ano, AVG(taxa_variacao) AS taxa_variacao_media
                FROM variacao_por_grupo_interno
                GROUP BY mes_ano
            )
        SELECT 
            vpg.mes_ano, 
            vpg.nome_grupo, 
            (vpg.taxa_variacao - mm.taxa_variacao_media)
        FROM variacao_por_grupo_interno vpg
        JOIN media_mensal_interno mm ON vpg.mes_ano = mm.mes_ano
        ORDER BY 1,2',
        
        'SELECT DISTINCT g.nome_grupo
         FROM datamart_ida.fato_ida f
         JOIN datamart_ida.dim_grupo_economico g ON f.id_grupo = g.id_grupo
         WHERE f.indicador ILIKE ''%Resolvidas em 5 dias úteis%''
         ORDER BY 1'
    ) AS ct(
        mes_ano_pivot DATE,
        "ALGAR" NUMERIC(20,2),
        "CLARO" NUMERIC(20,2),
        "EMBRATEL" NUMERIC(20,2),
        "GVT" NUMERIC(20,2),
        "NET" NUMERIC(20,2),
        "NEXTEL" NUMERIC(20,2),
        "OI" NUMERIC(20,2),
        "SERCOMTEL" NUMERIC(20,2),
        "SKY" NUMERIC(20,2),
        "TIM" NUMERIC(20,2),
        "VIVO" NUMERIC(20,2)
    )
ON m.mes_ano = ct.mes_ano_pivot
ORDER BY m.mes_ano;

COMMENT ON VIEW datamart_ida.vw_variacao_ida IS 'View com a taxa de variação para o valor médio da "Taxa de Resolvidas em 5 dias úteis" e a diferença para cada grupo econômico.';
