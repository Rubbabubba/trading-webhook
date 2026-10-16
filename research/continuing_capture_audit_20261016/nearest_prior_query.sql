-- Read-only supplementary query; quote cutoff pins the audited source window.
SELECT t.id,t.executed_at,
       (SELECT MAX(q.observed_at) FROM quotes q
        WHERE q.ticker=t.ticker AND q.usable=1
          AND q.observed_at<t.executed_at AND q.observed_at<=1792122787.474572)
FROM trades t;
