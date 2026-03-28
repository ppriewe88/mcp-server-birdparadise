"""
run with: mcp dev server.py
"""
import logging
import os
from typing import Annotated

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from pydantic import Field

from mcp_server.database.utils.capabilities import (
    DatabaseCapabilities,
    Structured,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv()
MCP_SERVER_ENDPOINT: str = os.getenv(
    "MCP_SERVER_ENDPOINT_STREAMABLE_HTTP", "http://127.0.0.1:8000/mcp"
)

mcp = FastMCP(
    name="bird_paradise_server",
    host="0.0.0.0",
    port=8000,
    stateless_http=True,
)

db = DatabaseCapabilities()


@mcp.tool(
    name="find_customer",
    description="Rufe dieses Tool auf, um Informationen über einen Kunden anhand seiner ID zu erhalten.",
)
def find_customer_capa(
    customer_id: Annotated[int, Field(..., description="Kunden-ID, z.B. 123")],
) -> Structured:
    """Tool, um Informationen über einen Kunden anhand seiner ID zu erhalten."""
    result: Structured = db.find_customer(customer_id)
    return result


@mcp.tool(
    name="show_customers",
    description="Rufe dieses Tool auf, um eine Liste der Kunden und ihrer IDs zu erhalten.",
)
def show_customers_capa() -> Structured:
    """Tool, um eine Liste der Kunden und ihrer IDs zu erhalten."""
    result: Structured = db.show_customers()
    return result


@mcp.tool(
    name="show_unpaid_invoices",
    description="Zeige unbezahlte Rechnungen an. Nutze dieses Tool, um eine Übersicht über alle unbezahlten Rechnungen zu erhalten.",
)
def show_unpaid_invoices_capa() -> Structured:
    """Tool, um unbezahlte Rechnungen anzuzeigen."""
    result: Structured = db.show_unpaid_invoices()
    return result


@mcp.tool(
    name="show_products",
    description="Zeige Produkte an. Nutze dieses Tool, um eine Übersicht über alle Produkte, deren Preise und Lagerbestände zu erhalten.",
)
def show_products_capa() -> Structured:
    """Tool, um Produkte anzuzeigen."""
    result: Structured = db.show_products()
    return result


@mcp.tool(
    name="show_unpaid_invoices_for_customer",
    description="Dieses Tool liefert für die ID eines Kunden dessen unbezahlte Rechnungen.",
)
def show_unpaid_invoices_for_customer_capa(
    customer_id: Annotated[
        int,
        Field(
            ...,
            description="""Kunden-ID. 
            Muss vom Nutzer bei der Anfrage angegeben werden (ganzzahlig). Wenn nicht gegeben, erfragen.""",
        ),
    ],
) -> Structured:
    """Tool, um unbezahlte Rechnungen eines bestimmten Kunden anzuzeigen."""
    result: Structured = db.show_unpaid_invoices_for_customer(customer_id)
    return result


@mcp.tool(
    name="pay_invoice",
    description="Bezahlen einer Rechnung. Nutze dieses Tool, um den Status einer Rechnung auf 'bezahlt' zu setzen und eventuelle Rabatte zu prüfen. Input: Rechnungs-ID",
)
def pay_invoice_capa(
    invoice_id: Annotated[
        int,
        Field(
            ...,
            description="""Die ID der zu bezahlenden Rechnung.
            Muss vom Nutzer bei der Anfrage angegeben werden (ganzzahlig). Wenn nicht gegeben, erfragen.""",
        ),
    ],
) -> str:
    """Tool, um eine Rechnung zu bezahlen."""
    result: str = db.pay_invoice(invoice_id)
    return result

if __name__ == "__main__":
    mcp.run(transport="streamable-http")
