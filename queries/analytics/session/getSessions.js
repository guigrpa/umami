import { CLICKHOUSE, RELATIONAL } from 'lib/constants';
import { getDateFormatClickhouse, rawQuery, rawQueryClickhouse, runAnalyticsQuery } from 'lib/db';

export async function getSessions(...args) {
  return runAnalyticsQuery({
    [RELATIONAL]: () => relationalQuery(...args),
    [CLICKHOUSE]: () => clickhouseQuery(...args),
  });
}

async function relationalQuery(websites, start_at) {
  const params = [start_at];

  return rawQuery(
    `
    select * 
    from session s
    where s.session_id in (
      select distinct session_id
      from "event" e
      where e.website_id in (${websites.join(',')})
        and e.created_at >= $1
      union
      select distinct session_id
      from "pageview" v
      where v.website_id in (${websites.join(',')})
        and v.created_at >= $1
    )
    `,
    params,
  );
}

async function clickhouseQuery(websites, start_at) {
  return rawQueryClickhouse(
    `
    select * 
    from session s
    where s.session_id in (
      select distinct session_id
      from "event" e
      where e.website_id in (${websites.join(',')})
        and e.created_at >= ${getDateFormatClickhouse(start_at)}
      union
      select distinct session_id
      from "pageview" v
      where v.website_id in (${websites.join(',')})
        and v.created_at >= ${getDateFormatClickhouse(start_at)}
    )
    `,
  );
}
