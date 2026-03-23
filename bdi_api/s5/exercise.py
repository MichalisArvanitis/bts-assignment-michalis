from typing import Annotated

from fastapi import APIRouter, status, HTTPException
from fastapi.params import Query
from sqlalchemy import create_engine, text

from bdi_api.settings import Settings

settings = Settings()

engine = create_engine(settings.db_url, future=True)

s5 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s5",
    tags=["s5"],
)


# --------------------------------------------------
# DB INIT
# --------------------------------------------------

@s5.post("/db/init")
def init_database() -> str:
    with engine.begin() as conn:
        with open("hr_schema.sql", "r", encoding="utf-8") as f:
            sql = f.read()
        conn.exec_driver_sql(sql)
    return "OK"


# --------------------------------------------------
# DB SEED
# --------------------------------------------------

@s5.post("/db/seed")
def seed_database() -> str:
    with engine.begin() as conn:
        with open("hr_seed_data.sql", "r", encoding="utf-8") as f:
            sql = f.read()
        conn.exec_driver_sql(sql)
    return "OK"


# --------------------------------------------------
# LIST DEPARTMENTS
# --------------------------------------------------

@s5.get("/departments/")
def list_departments() -> list[dict]:
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, name, location
            FROM department
            ORDER BY id
        """))
        return [dict(row._mapping) for row in result]


# --------------------------------------------------
# LIST EMPLOYEES (PAGINATED)
# --------------------------------------------------

@s5.get("/employees/")
def list_employees(
    page: Annotated[int, Query(ge=1)] = 1,
    per_page: Annotated[int, Query(ge=1, le=100)] = 10,
) -> list[dict]:

    offset = (page - 1) * per_page

    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT e.id,
                       e.first_name,
                       e.last_name,
                       e.email,
                       e.salary,
                       d.name AS department_name
                FROM employee e
                LEFT JOIN department d ON e.department_id = d.id
                ORDER BY e.id
                LIMIT :limit OFFSET :offset
            """),
            {"limit": per_page, "offset": offset},
        )

        return [dict(row._mapping) for row in result]


# --------------------------------------------------
# EMPLOYEES IN A DEPARTMENT
# --------------------------------------------------

@s5.get("/departments/{dept_id}/employees")
def list_department_employees(dept_id: int) -> list[dict]:

    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT id,
                       first_name,
                       last_name,
                       email,
                       salary,
                       hire_date
                FROM employee
                WHERE department_id = :dept_id
                ORDER BY id
            """),
            {"dept_id": dept_id},
        )

        return [dict(row._mapping) for row in result]


# --------------------------------------------------
# DEPARTMENT STATS
# --------------------------------------------------

@s5.get("/departments/{dept_id}/stats")
def department_stats(dept_id: int) -> dict:

    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT d.name AS department_name,
                       COUNT(DISTINCT e.id) AS employee_count,
                       AVG(e.salary) AS avg_salary,
                       COUNT(DISTINCT p.id) AS project_count
                FROM department d
                LEFT JOIN employee e ON e.department_id = d.id
                LEFT JOIN project p ON p.department_id = d.id
                WHERE d.id = :dept_id
                GROUP BY d.id
            """),
            {"dept_id": dept_id},
        ).first()

        if not result:
            raise HTTPException(status_code=404, detail="Department not found")

        return dict(result._mapping)


# --------------------------------------------------
# SALARY HISTORY
# --------------------------------------------------

@s5.get("/employees/{emp_id}/salary-history")
def salary_history(emp_id: int) -> list[dict]:

    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT change_date,
                       old_salary,
                       new_salary,
                       reason
                FROM salary_history
                WHERE employee_id = :emp_id
                ORDER BY change_date
            """),
            {"emp_id": emp_id},
        )

        return [dict(row._mapping) for row in result]