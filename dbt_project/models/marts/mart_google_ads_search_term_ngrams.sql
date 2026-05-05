{{
  config(
    materialized='table'
  )
}}

-- N-gram analysis of Google Ads search terms — 1 and 2-word tokens.
-- Each row = one n-gram × campaign × ISO week (Monday start).
-- total_spend is the sum of spend from search terms *containing* this n-gram;
-- one search term contributes to each of its constituent n-grams (intentional).
-- Use signal = 'wasteful' to surface negative keyword candidates at a token level,
-- catching waste that is spread too thin across search terms to trigger term-level alerts.

with weekly_terms as (

    select
        date_trunc(date_start, week(monday)) as week_start,
        search_term,
        campaign_name,
        sum(impressions)  as impressions,
        sum(clicks)       as clicks,
        sum(spend)        as spend,
        sum(conversions)  as conversions

    from {{ ref('stg_google_ads__search_terms') }}
    where spend > 0
        and date_start >= date_sub(current_date(), interval 8 week)
    group by 1, 2, 3

),

words as (

    select
        week_start,
        campaign_name,
        search_term,
        impressions,
        clicks,
        spend,
        conversions,
        word,
        pos,
        lead(word, 1) over (
            partition by week_start, campaign_name, search_term
            order by pos
        ) as next_word

    from weekly_terms,
        unnest(
            split(
                lower(regexp_replace(trim(search_term), r'[^a-z0-9 ]+', ' ')),
                ' '
            )
        ) as word with offset as pos

    where trim(word) != ''
        -- exclude articles, prepositions, pronouns — no targeting signal
        and word not in (
            'a', 'an', 'the', 'and', 'or', 'but',
            'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'from',
            'is', 'are', 'was', 'were', 'be', 'been',
            'it', 'its', 'this', 'that', 'i', 'my', 'me',
            'we', 'us', 'you', 'your'
        )

),

unigrams as (

    select
        week_start,
        campaign_name,
        search_term,
        impressions,
        clicks,
        spend,
        conversions,
        1        as ngram_length,
        word     as ngram
    from words

),

bigrams as (

    select
        week_start,
        campaign_name,
        search_term,
        impressions,
        clicks,
        spend,
        conversions,
        2                              as ngram_length,
        concat(word, ' ', next_word)   as ngram
    from words
    where next_word is not null
        and trim(next_word) != ''
        and next_word not in (
            'a', 'an', 'the', 'and', 'or', 'but',
            'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'from',
            'is', 'are', 'was', 'were', 'be', 'been',
            'it', 'its', 'this', 'that', 'i', 'my', 'me',
            'we', 'us', 'you', 'your'
        )

),

all_ngrams as (
    select * from unigrams
    union all
    select * from bigrams
)

select
    week_start,
    ngram,
    ngram_length,
    campaign_name,
    count(distinct search_term)                               as unique_search_terms,
    sum(impressions)                                          as total_impressions,
    sum(clicks)                                               as total_clicks,
    sum(spend)                                                as total_spend,
    sum(conversions)                                          as total_conversions,
    safe_divide(sum(clicks), sum(impressions))                as avg_ctr,
    safe_divide(sum(conversions), nullif(sum(clicks), 0))     as avg_cvr,
    case
        when sum(conversions) = 0 and sum(spend) >= 10 then 'wasteful'
        when sum(conversions) = 0 and sum(spend) >= 5  then 'watch'
        when safe_divide(sum(conversions), nullif(sum(clicks), 0)) >= 0.05
            then 'top_performer'
        when safe_divide(sum(conversions), nullif(sum(clicks), 0)) >= 0.02
            then 'good'
        else 'neutral'
    end                                                       as signal

from all_ngrams
group by 1, 2, 3, 4
having
    -- drop singleton low-spend n-grams — not actionable
    count(distinct search_term) >= 2
    or sum(spend) >= 5

