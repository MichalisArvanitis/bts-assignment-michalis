from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from neo4j import GraphDatabase

from bdi_api.settings import Settings

settings = Settings()

s7 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s7",
    tags=["s7"],
)


class PersonCreate(BaseModel):
    name: str
    city: str
    age: int


class RelationshipCreate(BaseModel):
    from_person: str
    to_person: str
    relationship_type: str = "FRIENDS_WITH"


@s7.post("/graph/person")
def create_person(person: PersonCreate) -> dict:
    driver = GraphDatabase.driver(
        settings.neo4j_url,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )

    with driver.session() as session:
        session.run(
            """
            CREATE (p:Person {name: $name, city: $city, age: $age})
            """,
            name=person.name,
            city=person.city,
            age=person.age,
        )

    driver.close()
    return {"status": "ok", "name": person.name}


@s7.get("/graph/persons")
def list_persons() -> list[dict]:
    driver = GraphDatabase.driver(
        settings.neo4j_url,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )

    with driver.session() as session:
        result = session.run(
            """
            MATCH (p:Person)
            RETURN p.name AS name, p.city AS city, p.age AS age
            ORDER BY p.name
            """
        )
        persons = [dict(record) for record in result]

    driver.close()
    return persons


@s7.get("/graph/person/{name}/friends")
def get_friends(name: str) -> list[dict]:
    driver = GraphDatabase.driver(
        settings.neo4j_url,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )

    with driver.session() as session:
        exists = session.run(
            """
            MATCH (p:Person {name: $name})
            RETURN p
            """,
            name=name,
        ).single()

        if not exists:
            driver.close()
            raise HTTPException(status_code=404, detail=f"Person '{name}' not found")

        result = session.run(
            """
            MATCH (p:Person {name: $name})-[:FRIENDS_WITH]-(friend:Person)
            RETURN friend.name AS name, friend.city AS city, friend.age AS age
            ORDER BY friend.name
            """,
            name=name,
        )
        friends = [dict(record) for record in result]

    driver.close()
    return friends


@s7.post("/graph/relationship")
def create_relationship(rel: RelationshipCreate) -> dict:
    driver = GraphDatabase.driver(
        settings.neo4j_url,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )

    with driver.session() as session:
        from_exists = session.run(
            """
            MATCH (p:Person {name: $name})
            RETURN p
            """,
            name=rel.from_person,
        ).single()

        if not from_exists:
            driver.close()
            raise HTTPException(status_code=404, detail=f"Person '{rel.from_person}' not found")

        to_exists = session.run(
            """
            MATCH (p:Person {name: $name})
            RETURN p
            """,
            name=rel.to_person,
        ).single()

        if not to_exists:
            driver.close()
            raise HTTPException(status_code=404, detail=f"Person '{rel.to_person}' not found")

        session.run(
            """
            MATCH (a:Person {name: $from_person}), (b:Person {name: $to_person})
            MERGE (a)-[:FRIENDS_WITH]->(b)
            """,
            from_person=rel.from_person,
            to_person=rel.to_person,
        )

    driver.close()
    return {"status": "ok", "from": rel.from_person, "to": rel.to_person}


@s7.get("/graph/person/{name}/recommendations")
def get_recommendations(name: str) -> list[dict]:
    driver = GraphDatabase.driver(
        settings.neo4j_url,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )

    with driver.session() as session:
        exists = session.run(
            """
            MATCH (p:Person {name: $name})
            RETURN p
            """,
            name=name,
        ).single()

        if not exists:
            driver.close()
            raise HTTPException(status_code=404, detail=f"Person '{name}' not found")

        result = session.run(
            """
            MATCH (p:Person {name: $name})-[:FRIENDS_WITH]-(friend:Person)-[:FRIENDS_WITH]-(rec:Person)
            WHERE rec.name <> $name
              AND NOT (p)-[:FRIENDS_WITH]-(rec)
            RETURN rec.name AS name,
                   rec.city AS city,
                   COUNT(DISTINCT friend) AS mutual_friends
            ORDER BY mutual_friends DESC, rec.name ASC
            """
            ,
            name=name,
        )
        recommendations = [dict(record) for record in result]

    driver.close()
    return recommendations