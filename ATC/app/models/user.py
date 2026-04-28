from __future__ import annotations

from typing import List
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import String, Boolean, Index

from app.core.db import Base

class User(Base):
    __tablename__ = "users"

    # =========================
    # 🆔 IDENTIFICACIÓN
    # =========================
    id: Mapped[int] = mapped_column(primary_key=True)

    # =========================
    # 👤 INFORMACIÓN BÁSICA
    # =========================
    name: Mapped[str] = mapped_column(String(100), nullable=False)

    username: Mapped[str] = mapped_column(
        String(50),
        unique=True,
        index=True,
        nullable=False,
    )

    hashed_password: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )

    # =========================
    # 🛡 ROLES Y ESTADO
    # =========================
    role: Mapped[str] = mapped_column(
        String(20),
        default="agent",
        nullable=False,
    )
    # Valores permitidos: "admin" | "agent"

    is_active: Mapped[bool] = mapped_column(
        Boolean,
        default=True,
        nullable=False,
    )

    # =========================
    # 🔗 RELACIONES
    # =========================
    assigned_tickets: Mapped[List["Ticket"]] = relationship(
        "Ticket",
        back_populates="assigned_to",
    )

    # =========================
    # 🛡 PROPIEDADES DE PERMISOS
    # =========================
    @property
    def is_admin(self) -> bool:
        return self.role == "admin"

    @property
    def is_agent(self) -> bool:
        return self.role == "agent"

    # =========================
    # 🧠 MÉTODOS ÚTILES
    # =========================
    def deactivate(self) -> None:
        self.is_active = False

    def activate(self) -> None:
        self.is_active = True

    def promote_to_admin(self) -> None:
        self.role = "admin"

    def demote_to_agent(self) -> None:
        self.role = "agent"

    # =========================
    # 🧾 DEBUG PROFESIONAL
    # =========================
    def __repr__(self) -> str:
        return (
            f"<User id={self.id} "
            f"username={self.username} "
            f"role={self.role}>"
        )


# =========================
# 📌 ÍNDICES OPCIONALES EXTRA
# =========================
Index("ix_users_role", User.role)