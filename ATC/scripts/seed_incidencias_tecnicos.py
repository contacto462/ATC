from __future__ import annotations

from sqlalchemy import text

from app.core.db import SessionLocal


TECHNICIANS = [
    "Luis Alberto Bustamante Aguilera",
    "Jesus Sebastian Gonzalez Aguilera",
    "Marco Antonio Lopez Aguirre",
    "Diego Antonio Moncada Sepulveda",
    "Jason Kevin P\u00e9rez Ortiz",
    "Haxel Samir Del Carmen Saavedra Villanueva",
    "Enrique Alejandro Sandoval Nunez",
    "Omar Alejandro Trivi\u00f1o Silva",
    "Mauro Estefano Reyes Villegas",
    "Barbara Constanza Nu\u00f1ez Carrasco",
    "Nicolas Alfonso Bravo Rain",
    "Ricardo Andres Vergara Guerra",
    "Emmanuel Issak Correa Ubilla",
    "Rodrigo Octavio Carmona Agurto",
    "Bryan Benjamin Ibaceta Fabrega",
    "Bryan Alexander Rebolledo Hidalgo",
]


def seed_technicians() -> tuple[int, int]:
    db = SessionLocal()
    try:
        db.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS incidencias_tecnicos (
                    id BIGSERIAL PRIMARY KEY,
                    nombre VARCHAR(180) NOT NULL UNIQUE,
                    activo BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )

        db.execute(text("TRUNCATE TABLE incidencias_tecnicos RESTART IDENTITY"))

        inserted = 0
        for technician in TECHNICIANS:
            db.execute(
                text(
                    """
                    INSERT INTO incidencias_tecnicos (nombre, activo)
                    VALUES (:nombre, TRUE)
                    """
                ),
                {"nombre": technician},
            )
            inserted += 1

        db.commit()

        total = db.execute(text("SELECT COUNT(*) FROM incidencias_tecnicos")).scalar_one()
        return inserted, int(total)
    finally:
        db.close()


if __name__ == "__main__":
    affected, total = seed_technicians()
    print(f"Tecnicos cargados: {affected}")
    print(f"Total en incidencias_tecnicos: {total}")
