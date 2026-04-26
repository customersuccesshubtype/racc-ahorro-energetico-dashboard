#!/usr/bin/env python3
"""
generate_data.py — RACC Estalvi Energètic Dashboard
Queries Tinybird and writes data.json for GitHub Pages.

Run locally:
  TINYBIRD_TOKEN=p.xxxx python generate_data.py

Run via GitHub Actions (token from GitHub Secret TINYBIRD_TOKEN).
"""

import os, json, sys
import urllib.request, urllib.parse
from datetime import date, timedelta, datetime, timezone

# ── CONFIG ──────────────────────────────────────────────────
TOKEN = os.environ.get("TINYBIRD_TOKEN", "")
ORG   = "1f5d9252-b60f-490f-8ccd-1d76c4149273"
BOT   = "299465da-119b-4ca7-9d2c-33a82c8ec2d6"
START = "2026-01-31"
END   = (date.today() + timedelta(days=1)).isoformat()  # upper bound (exclusive)

if not TOKEN:
    sys.exit("ERROR: TINYBIRD_TOKEN environment variable not set.")

# ── TINYBIRD QUERY ───────────────────────────────────────────
def tb_query(sql):
    url = "https://api.tinybird.co/v0/sql?q=" + urllib.parse.quote(sql + " FORMAT JSON")
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {TOKEN}"})
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            return json.loads(resp.read()).get("data", [])
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        sys.exit(f"Tinybird HTTP {e.code}: {body[:400]}")

# ── SQL QUERIES ──────────────────────────────────────────────

SQL_FUNNEL = f"""
WITH flow_conversations AS(
  SELECT conversation_id FROM PROD_ENT0.ds_messages
  WHERE organization_id='{ORG}' AND bot_id='{BOT}'
  AND created_at>='{START}' AND created_at<'{END}'
  AND JSONExtractString(_event_data,'flow_name')='Servicio Eficiencia Energetica'
  GROUP BY conversation_id
),
kw_origins AS(
  SELECT conversation_id,
    if(max(if(JSONExtractString(_event_data,'nlu_keyword_name') LIKE 'Hola%',1,0))=1,'Meta',
      if(max(if(JSONExtractString(_event_data,'nlu_keyword_name') IN(
        'Quiero información sobre el servicio de ahorro energético',
        'Vull informació sobre el servei d''estalvi energètic'
      ),1,0))=1,'Landing','WA')) as origen
  FROM PROD_ENT0.ds_messages
  WHERE organization_id='{ORG}' AND action='nlu_keyword'
  AND created_at>='{START}' AND created_at<'{END}'
  GROUP BY conversation_id
),
df AS(SELECT DISTINCT conversation_id FROM PROD_ENT0.ds_messages
  WHERE organization_id='{ORG}' AND bot_id='{BOT}' AND created_at>='{START}'
  AND(action='flow_node' OR action='bot_action')
  AND JSONExtractString(_event_data,'flow_node_content_id') IN(
    'Confirmacion_Datos_Factura_Luz','Confirmacion_Datos_Factura_Gas')),
ed AS(SELECT DISTINCT conversation_id FROM PROD_ENT0.ds_messages
  WHERE organization_id='{ORG}' AND bot_id='{BOT}' AND created_at>='{START}'
  AND action='message_sent_by_enduser' AND type IN('document','image')),
cot AS(SELECT DISTINCT conversation_id FROM PROD_ENT0.ds_messages
  WHERE organization_id='{ORG}' AND bot_id='{BOT}' AND created_at>='{START}'
  AND(action='flow_node' OR action='bot_action')
  AND JSONExtractString(_event_data,'flow_node_content_id') IN(
    'Quote Received Gas','Quote Received Elec')),
con AS(SELECT DISTINCT conversation_id FROM PROD_ENT0.ds_messages
  WHERE organization_id='{ORG}' AND bot_id='{BOT}' AND created_at>='{START}'
  AND(action='flow_node' OR action='bot_action')
  AND JSONExtractString(_event_data,'flow_node_content_id') IN(
    'Contract Received Elec','Contract Received Gas'))
SELECT
  if(kw.origen='','WA',kw.origen) as origen,
  count() as conversaciones,
  countIf(dfx.conversation_id IS NOT NULL OR edx.conversation_id IS NOT NULL) as factura_total,
  countIf(edx.conversation_id IS NOT NULL) as factura_chat,
  countIf(dfx.conversation_id IS NOT NULL) as factura_dades,
  countIf(cotx.conversation_id IS NOT NULL) as cotitzacions,
  countIf(conx.conversation_id IS NOT NULL) as contractes
FROM flow_conversations fc
LEFT JOIN kw_origins kw ON fc.conversation_id=kw.conversation_id
LEFT JOIN df dfx ON fc.conversation_id=dfx.conversation_id
LEFT JOIN ed edx ON fc.conversation_id=edx.conversation_id
LEFT JOIN cot cotx ON fc.conversation_id=cotx.conversation_id
LEFT JOIN con conx ON fc.conversation_id=conx.conversation_id
GROUP BY origen ORDER BY conversaciones DESC
"""

SQL_WEEKLY = f"""
WITH fc AS(
  SELECT conversation_id, min(created_at) fecha FROM PROD_ENT0.ds_messages
  WHERE organization_id='{ORG}' AND bot_id='{BOT}'
  AND created_at>='{START}' AND created_at<'{END}'
  AND JSONExtractString(_event_data,'flow_name')='Servicio Eficiencia Energetica'
  GROUP BY conversation_id
),
cot AS(SELECT DISTINCT conversation_id FROM PROD_ENT0.ds_messages
  WHERE organization_id='{ORG}' AND bot_id='{BOT}' AND created_at>='{START}'
  AND(action='flow_node' OR action='bot_action')
  AND JSONExtractString(_event_data,'flow_node_content_id') IN(
    'Quote Received Gas','Quote Received Elec')),
con AS(SELECT DISTINCT conversation_id FROM PROD_ENT0.ds_messages
  WHERE organization_id='{ORG}' AND bot_id='{BOT}' AND created_at>='{START}'
  AND(action='flow_node' OR action='bot_action')
  AND JSONExtractString(_event_data,'flow_node_content_id') IN(
    'Contract Received Elec','Contract Received Gas'))
SELECT
  toString(toStartOfWeek(fc.fecha, 1)) semana,
  count() conversaciones,
  countIf(cotx.conversation_id IS NOT NULL) cotitzacions,
  countIf(conx.conversation_id IS NOT NULL) contractes
FROM fc
LEFT JOIN cot cotx ON fc.conversation_id=cotx.conversation_id
LEFT JOIN con conx ON fc.conversation_id=conx.conversation_id
GROUP BY semana ORDER BY semana
"""

SQL_CLOSURE = f"""
WITH fc AS(
  SELECT conversation_id FROM PROD_ENT0.ds_messages
  WHERE organization_id='{ORG}' AND bot_id='{BOT}'
  AND created_at>='{START}' AND created_at<'{END}'
  AND JSONExtractString(_event_data,'flow_name')='Servicio Eficiencia Energetica'
  GROUP BY conversation_id
),
hi AS(
  SELECT conversation_id, min(created_at) ht FROM PROD_ENT0.ds_messages
  WHERE organization_id='{ORG}' AND bot_id='{BOT}' AND created_at>='{START}'
  AND action='case_created'
  GROUP BY conversation_id
),
ce AS(
  SELECT conversation_id,
    max(JSONExtract(_event_data,'custom_fields','Map(String, String)')['eventName']) ev
  FROM PROD_ENT0.ds_messages
  WHERE organization_id='{ORG}' AND bot_id='{BOT}'
  AND created_at>='{START}' AND action='custom'
  GROUP BY conversation_id
),
mh AS(
  SELECT m.conversation_id,
    max(if(JSONExtractString(m._event_data,'flow_node_content_id')='Ahorro_Luz_No',1,0)) al,
    max(if(JSONExtractString(m._event_data,'flow_node_content_id')='Ahorro_Gas_No',1,0)) ag,
    max(if(JSONExtractString(m._event_data,'flow_node_content_id')='No titular elec',1,0)) nte,
    max(if(JSONExtractString(m._event_data,'flow_node_content_id')='No titular gas',1,0)) ntg,
    max(if(JSONExtractString(m._event_data,'flow_node_content_id')=
      'Handoff_Eficiencia_Energetica_No_Cotizable',1,0)) nc,
    max(if(JSONExtractString(m._event_data,'flow_node_content_id')=
      'Handoff_Eficiencia_Energetica_Too_Many_Retries',1,0)) tmr,
    max(if(JSONExtractString(m._event_data,'flow_node_content_id')=
      'Energy Efficiency Generic Error',1,0)) ge,
    max(if(JSONExtractString(m._event_data,'flow_node_content_id')=
      'Handoff Estalvi Energetic',1,0)) hae
  FROM PROD_ENT0.ds_messages m
  INNER JOIN hi h ON m.conversation_id=h.conversation_id
  WHERE m.organization_id='{ORG}' AND m.bot_id='{BOT}'
  AND(m.action='flow_node' OR m.action='bot_action')
  AND m.created_at < h.ht
  GROUP BY m.conversation_id
)
SELECT multiIf(
  h.conversation_id IS NULL,                          'Automatitzada \u2713',
  if(cex.ev='','?',cex.ev)='negativeSavings',         'Ahorro negatiu',
  if(cex.ev='','?',cex.ev)='over70Savings',           'Ahorro > 70%',
  if(cex.ev='','?',cex.ev)='handleInvoiceFileError',  'Error arxiu factura',
  mhx.nc=1,   'No cotitzable',
  mhx.tmr=1,  'Massa reintents',
  mhx.al=1,   'Sense estalvi (llum)',
  mhx.ag=1,   'Sense estalvi (gas)',
  mhx.nte=1,  'No titular (elec)',
  mhx.ntg=1,  'No titular (gas)',
  mhx.ge=1,   'Error gen\u00e8ric',
  mhx.hae=1,  'Handoff Estalvi Energ\u00e8tic',
  'Handoff \u2013 Sense motiu'
) tipologia, count() total
FROM fc
LEFT JOIN hi h   ON fc.conversation_id=h.conversation_id
LEFT JOIN ce cex ON fc.conversation_id=cex.conversation_id
LEFT JOIN mh mhx ON fc.conversation_id=mhx.conversation_id
GROUP BY tipologia ORDER BY total DESC
"""

SQL_INCORRECT = f"""
WITH fc AS(
  SELECT conversation_id FROM PROD_ENT0.ds_messages
  WHERE organization_id='{ORG}' AND bot_id='{BOT}'
  AND created_at>='{START}' AND created_at<'{END}'
  AND JSONExtractString(_event_data,'flow_name')='Servicio Eficiencia Energetica'
  GROUP BY conversation_id
),
cot AS(SELECT DISTINCT conversation_id FROM PROD_ENT0.ds_messages
  WHERE organization_id='{ORG}' AND bot_id='{BOT}' AND created_at>='{START}'
  AND(action='flow_node' OR action='bot_action')
  AND JSONExtractString(_event_data,'flow_node_content_id') IN(
    'Quote Received Gas','Quote Received Elec')),
cl AS(SELECT conversation_id,
  multiIf(payload LIKE '%019be65a-099a%' AND payload LIKE '%source_0%','Si',
    payload LIKE '%019be65a-1a8d%' AND payload LIKE '%source_1%','No',
    payload LIKE '%019be65a-1a8d%' AND payload LIKE '%source_2%','No ho se','?') resp
  FROM PROD_ENT0.ds_messages
  WHERE organization_id='{ORG}' AND bot_id='{BOT}' AND created_at>='{START}'
  AND action='message_sent_by_enduser' AND type='postback'
  AND(payload LIKE '%019be65a-099a%' OR payload LIKE '%019be65a-1a8d%')),
cg AS(SELECT conversation_id,
  multiIf(payload LIKE '%019be65a-2216%' AND payload LIKE '%source_0%','Si',
    payload LIKE '%019be65a-33ea%' AND payload LIKE '%source_1%','No',
    payload LIKE '%019be65a-33ea%' AND payload LIKE '%source_2%','No ho se','?') resp
  FROM PROD_ENT0.ds_messages
  WHERE organization_id='{ORG}' AND bot_id='{BOT}' AND created_at>='{START}'
  AND action='message_sent_by_enduser' AND type='postback'
  AND(payload LIKE '%019be65a-2216%' OR payload LIKE '%019be65a-33ea%'))
SELECT
  countIf(cotx.conversation_id IS NOT NULL) total_cot,
  countIf(cotx.conversation_id IS NOT NULL
    AND(clx.resp IN('No','No ho se') OR cgx.resp IN('No','No ho se'))) cot_ko,
  countIf(cotx.conversation_id IS NOT NULL
    AND clx.resp IN('No','No ho se')) ko_llum,
  countIf(cotx.conversation_id IS NOT NULL
    AND cgx.resp IN('No','No ho se')) ko_gas
FROM fc
LEFT JOIN cot cotx ON fc.conversation_id=cotx.conversation_id
LEFT JOIN cl clx   ON fc.conversation_id=clx.conversation_id
LEFT JOIN cg cgx   ON fc.conversation_id=cgx.conversation_id
"""

# ── MAIN ─────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"Querying Tinybird: {START} \u2192 {date.today().isoformat()}")

    print("  \u00b7 funnel\u2026")
    funnel = tb_query(SQL_FUNNEL)
    print(f"    {len(funnel)} rows")

    print("  \u00b7 weekly\u2026")
    weekly = tb_query(SQL_WEEKLY)
    print(f"    {len(weekly)} rows")

    print("  \u00b7 closure\u2026")
    closure = tb_query(SQL_CLOSURE)
    print(f"    {len(closure)} rows")

    print("  \u00b7 incorrect\u2026")
    incorrect_rows = tb_query(SQL_INCORRECT)
    incorrect = incorrect_rows[0] if incorrect_rows else {}
    print("    done")

    data = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "date_from":    START,
        "date_to":      date.today().isoformat(),
        "funnel":       funnel,
        "weekly":       weekly,
        "closure":      closure,
        "incorrect":    incorrect,
    }

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    size = len(json.dumps(data, ensure_ascii=False))
    print(f"\n\u2713 data.json written ({size:,} bytes)")
    print(f"  Converses totals: {sum(int(r.get('conversaciones',0)) for r in funnel)}")
    print(f"  Cotitzacions:     {sum(int(r.get('cotitzacions',0)) for r in funnel)}")
    print(f"  Contractes:       {sum(int(r.get('contractes',0)) for r in funnel)}")
