import { subMinutes } from 'date-fns';
import { CLICKHOUSE, RELATIONAL } from 'lib/constants';
import { getDateFormatClickhouse, rawQuery, rawQueryClickhouse, runAnalyticsQuery } from 'lib/db';

export async function getActiveVisitors(...args) {
  return runAnalyticsQuery({
    [RELATIONAL]: () => relationalQuery(...args),
    [CLICKHOUSE]: () => clickhouseQuery(...args),
  });
}

async function relationalQuery(website_id) {
  const date = subMinutes(new Date(), 5);
  const params = [website_id, date];

  return rawQuery(
    `
    select count(distinct s.session_id) x
    from (
      select distinct session_id
      from "event" e
      where e.website_id = $1
        and e.created_at >= $2
      union
      select distinct session_id
      from "pageview" v
      where v.website_id = $1
        and v.created_at >= $2
    ) s
    `,
    params,
  );
}

async function clickhouseQuery(website_id) {
  const params = [website_id];

  return rawQueryClickhouse(
    `
    select count(distinct s.session_id) x
    from (
      select distinct session_id
      from "event" e
      where e.website_id = $1
        and e.created_at >= ${getDateFormatClickhouse(subMinutes(new Date(), 5))}
      union
      select distinct session_id
      from "pageview" v
      where v.website_id = $1
        and v.created_at >= ${getDateFormatClickhouse(subMinutes(new Date(), 5))}
    ) s
    `,
    params,
  );
}
