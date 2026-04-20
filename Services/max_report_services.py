from datetime import datetime, timedelta, timezone

from Clients.db import get_pool

MSK = timezone(timedelta(hours=3))
SALES_STATUS_IDS = (142, 75366150, 78036790)
IN_WORK_STATUS_NAMES = ("неразобранное", "первичный контакт", "переговоры")


def _safe_pct(current: float, previous: float) -> float:
    if previous == 0:
        return 0.0
    return ((current - previous) / previous) * 100.0


def _fmt_delta(current: float, previous: float, suffix: str = "") -> str:
    diff = current - previous
    pct = _safe_pct(current, previous)
    sign = "+" if diff >= 0 else ""
    return f"{sign}{diff:.2f}{suffix} ({sign}{pct:.1f}%)"


async def _period_metrics(start_dt: datetime, end_dt: datetime) -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        deals_row = await conn.fetchrow(
            """
            SELECT
                COUNT(*)::int AS applications,
                COUNT(*) FILTER (WHERE ps.deal_status = 1)::int AS orders,
                COALESCE(SUM(d.price) FILTER (WHERE ps.deal_status = 1), 0)::int AS revenue,
                COALESCE(SUM(d.profit) FILTER (WHERE ps.deal_status = 1), 0)::int AS profit
            FROM deals d
            LEFT JOIN pipeline_statuses ps ON ps.amo_status_id = d.status_id
            WHERE d.amo_created_at >= $1
              AND d.amo_created_at < $2
            """,
            start_dt,
            end_dt,
        )

        response_row = await conn.fetchrow(
            """
            WITH inbound AS (
                SELECT amo_deal_id, created_at AS inbound_at
                FROM chat_events
                WHERE direction = 'inbound'
                  AND created_at >= $1
                  AND created_at < $2
            )
            SELECT
                COUNT(*)::int AS pairs_count,
                COALESCE(AVG(EXTRACT(EPOCH FROM (o.outbound_at - i.inbound_at))), 0)::float8 AS avg_seconds
            FROM inbound i
            JOIN LATERAL (
                SELECT created_at AS outbound_at
                FROM chat_events c
                WHERE c.amo_deal_id = i.amo_deal_id
                  AND c.direction = 'outbound'
                  AND c.created_at >= i.inbound_at
                ORDER BY c.created_at
                LIMIT 1
            ) o ON TRUE
            """,
            start_dt,
            end_dt,
        )

    applications = int(deals_row["applications"] or 0)
    orders = int(deals_row["orders"] or 0)
    revenue = int(deals_row["revenue"] or 0)
    profit = int(deals_row["profit"] or 0)
    conversion = (orders / applications * 100.0) if applications else 0.0
    avg_check = (revenue / orders) if orders else 0.0
    avg_response_seconds = float(response_row["avg_seconds"] or 0.0)

    return {
        "applications": applications,
        "orders": orders,
        "conversion": conversion,
        "revenue": revenue,
        "profit": profit,
        "avg_check": avg_check,
        "avg_response_seconds": avg_response_seconds,
    }


def _weekly_bounds() -> tuple[datetime, datetime, datetime, datetime]:
    now = datetime.now(MSK)
    week_start = datetime(now.year, now.month, now.day, tzinfo=MSK) - timedelta(days=now.weekday())
    current_start = week_start - timedelta(days=7)
    current_end = week_start
    prev_start = current_start - timedelta(days=7)
    prev_end = current_start
    # DB timestamps are stored as naive local time
    return (
        current_start.replace(tzinfo=None),
        current_end.replace(tzinfo=None),
        prev_start.replace(tzinfo=None),
        prev_end.replace(tzinfo=None),
    )


async def build_weekly_report_text() -> str:
    current_start, current_end, prev_start, prev_end = _weekly_bounds()
    curr = await _period_metrics(current_start, current_end)
    prev = await _period_metrics(prev_start, prev_end)

    lines = [
        "Еженедельный отчет",
        f"Период: {current_start:%d.%m.%Y} - {(current_end - timedelta(days=1)):%d.%m.%Y}",
        "",
        f"Заявки: {curr['applications']} | {_fmt_delta(curr['applications'], prev['applications'])}",
        f"Заказы: {curr['orders']} | {_fmt_delta(curr['orders'], prev['orders'])}",
        f"Конверсия: {curr['conversion']:.1f}% | {_fmt_delta(curr['conversion'], prev['conversion'], '%')}",
        f"Выручка: {curr['revenue']} ₽ | {_fmt_delta(curr['revenue'], prev['revenue'], ' ₽')}",
        f"Прибыль: {curr['profit']} ₽ | {_fmt_delta(curr['profit'], prev['profit'], ' ₽')}",
        f"Средний чек: {curr['avg_check']:.0f} ₽ | {_fmt_delta(curr['avg_check'], prev['avg_check'], ' ₽')}",
        f"Среднее время ответа: {curr['avg_response_seconds']:.0f} c | {_fmt_delta(curr['avg_response_seconds'], prev['avg_response_seconds'], ' c')}",
    ]
    return "\n".join(lines)


async def build_stats_command_text() -> str:
    now = datetime.now(MSK).replace(tzinfo=None)
    start_dt = now - timedelta(days=7)

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                COUNT(*)::int AS applications,
                COUNT(*) FILTER (
                    WHERE d.status_id = ANY($3::bigint[])
                )::int AS sales,
                COUNT(*) FILTER (
                    WHERE LOWER(ps.name) = ANY($4::text[])
                )::int AS in_work
            FROM deals d
            LEFT JOIN pipeline_statuses ps ON ps.amo_status_id = d.status_id
            WHERE d.amo_created_at >= $1
              AND d.amo_created_at < $2
            """,
            start_dt,
            now,
            list(SALES_STATUS_IDS),
            list(IN_WORK_STATUS_NAMES),
        )

    applications = int(row["applications"] or 0)
    sales = int(row["sales"] or 0)
    in_work = int(row["in_work"] or 0)
    denominator = applications - in_work
    conversion = (sales / denominator * 100.0) if denominator > 0 else 0.0
    grade = "конверсия ниже нормы" if conversion < 20.0 else "все ок"

    lines = [
        f"Статистика за последние 7 дней ({start_dt:%d.%m.%Y} - {now:%d.%m.%Y})",
        f"количество заявок - {applications}",
        f"количество продаж - {sales}",
        f"количество заявок в работе - {in_work}",
        f"конверсия - {conversion:.1f}%",
        f"оценка - {grade}",
    ]
    return "\n".join(lines)
