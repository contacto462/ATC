from __future__ import annotations

from datetime import datetime, timezone

from app.models.ticket import Ticket


def apply_ticket_status_change(ticket: Ticket, new_status: str) -> dict[str, object]:
    # Centralizamos el cambio de estado para reutilizar exactamente
    # la misma regla desde rutas web, correo y automatizaciones.
    old_status = (ticket.status or "").strip()
    now = datetime.now(timezone.utc)

    ticket.status = new_status

    became_resolved = new_status == "resolved" and old_status != "resolved"
    reopened_from_resolved = new_status in ("open", "pending") and old_status == "resolved"

    if new_status == "resolved" and ticket.resolved_at is None:
        # Guardamos la fecha solo cuando el ticket pasa a resuelto.
        ticket.resolved_at = now

    if reopened_from_resolved:
        # Si el ticket vuelve a abrirse, limpiamos la resolución previa.
        ticket.reopen_count = (ticket.reopen_count or 0) + 1
        ticket.resolved_at = None

    return {
        "old_status": old_status,
        "new_status": new_status,
        "changed_at": now,
        "became_resolved": became_resolved,
        "reopened_from_resolved": reopened_from_resolved,
    }


def mark_first_agent_reply(ticket: Ticket) -> None:
    # La primera respuesta del agente se fija una sola vez
    # para alimentar métricas de tiempo de primera respuesta.
    if ticket.first_agent_reply_at is None:
        ticket.first_agent_reply_at = datetime.now(timezone.utc)
