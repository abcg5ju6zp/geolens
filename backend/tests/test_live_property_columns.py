"""The live column list matches what ``information_schema`` reports, in order."""

import uuid

from sqlalchemy import text

from app.modules.catalog.features.service import live_property_columns


async def test_live_property_columns_skip_dropped_and_reserved_columns(test_db_session):
    table = f"lpc_{uuid.uuid4().hex[:10]}"
    await test_db_session.execute(
        text(
            f'CREATE TABLE "data"."{table}" ('
            "gid serial PRIMARY KEY, geom geometry(Point, 3857), "
            'name varchar(255), "a""b" int, "\\:id" text, gone int, tail numeric(8, 2), '
            "geom_4326 geometry(Point, 4326))"
        )
    )
    await test_db_session.execute(
        text(f'ALTER TABLE "data"."{table}" DROP COLUMN gone')
    )
    await test_db_session.execute(
        text(f'ALTER TABLE "data"."{table}" ADD COLUMN late text')
    )
    try:
        expected = await test_db_session.scalars(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'data' AND table_name = :t "
                "AND column_name NOT IN ('gid', 'geom', 'geom_4326') "
                "ORDER BY ordinal_position"
            ),
            {"t": table},
        )
        want = ", ".join(
            '"' + n.replace('"', '""').replace(":", "\\:") + '"' for n in expected.all()
        )
        got = await live_property_columns(test_db_session, table)
        assert got == want
        assert got == '"name", "a""b", "\\:id", "tail", "late"'
    finally:
        await test_db_session.execute(text(f'DROP TABLE IF EXISTS "data"."{table}"'))
        await test_db_session.commit()
