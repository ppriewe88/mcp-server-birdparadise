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
    "MCP_SERVER_ENDPOINT_READ", "http://127.0.0.1:8000/mcp"
)

mcp = FastMCP(
    name="bird_paradise_reading_capas",
    host="0.0.0.0",
    port=8000,
    stateless_http=True,
)

db = DatabaseCapabilities()

########################## CUSTOMERS ##########################

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

########################## PRODUCTS ##########################


@mcp.tool(
    name="show_products",
    description="Zeige Produkte an. Nutze dieses Tool, um eine Übersicht über alle Produkte, deren Preise und Lagerbestände zu erhalten.",
)
def show_products_capa() -> Structured:
    """Tool, um Produkte anzuzeigen."""
    result: Structured = db.show_products()
    return result

@mcp.tool(
    name="show_low_stock_products",
    description="Zeige Produkte an, deren Lagerbestand unter dem Mindestbestand liegt.",
)
def show_low_stock_products_capa() -> Structured:
    """Tool, um Produkte mit unterschrittenem Mindestbestand anzuzeigen."""
    result: Structured = db.show_low_stock_products()
    return result


@mcp.tool(
    name="show_open_orders_for_product",
    description="Zeige offene (beauftragte) Bestellungen für ein bestimmtes Produkt.",
)
def show_open_orders_for_product_capa(
    product_id: Annotated[int, Field(..., description="Produkt-ID")],
) -> Structured:
    """Tool, um offene Bestellungen für ein Produkt anzuzeigen."""
    result: Structured = db.show_open_orders_for_product(product_id)
    return result


########################## ORDERS ##########################

@mcp.tool(
    name="show_orders_for_customer",
    description="Zeige alle Aufträge (Bestellungen) eines Kunden an, inklusive Produktname und Status.",
)
def show_orders_for_customer_capa(
    customer_id: Annotated[
        int,
        Field(
            ...,
            description="Kunden-ID. Muss vom Nutzer angegeben werden. Wenn nicht gegeben, erfragen.",
        ),
    ],
) -> Structured:
    """Tool, um alle Aufträge eines Kunden anzuzeigen."""
    result: Structured = db.show_orders_for_customer(customer_id)
    return result


########################## INVOICES ##########################


@mcp.tool(
    name="show_unpaid_invoices",
    description="Zeige unbezahlte Rechnungen an. Nutze dieses Tool, um eine Übersicht über alle unbezahlten Rechnungen zu erhalten.",
)
def show_unpaid_invoices_capa() -> Structured:
    """Tool, um unbezahlte Rechnungen anzuzeigen."""
    result: Structured = db.show_unpaid_invoices()
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
    name="show_invoices_for_customer",
    description="Zeige alle Rechnungen eines Kunden an, inklusive Produktname und Auftragsnummer.",
)
def show_invoices_for_customer_capa(
    customer_id: Annotated[
        int,
        Field(
            ...,
            description="Kunden-ID. Muss vom Nutzer angegeben werden. Wenn nicht gegeben, erfragen.",
        ),
    ],
) -> Structured:
    """Tool, um alle Rechnungen eines Kunden anzuzeigen."""
    result: Structured = db.show_invoices_for_customer(customer_id)
    return result


########################## REVENUE ANALYSIS ##########################


@mcp.tool(
    name="get_revenue_view_schema",
    description="Gibt das Schema und die Query-Anweisungen für die Umsatzanalyse-View zurück. Rufe dieses Tool ZUERST auf, bevor du eine Umsatz-Query formulierst.",
)
def get_revenue_view_schema_capa() -> Structured:
    """Tool, um das Schema der Umsatzanalyse-View zu erhalten."""
    result: Structured = db.get_revenue_view_schema()
    return result


@mcp.tool(
    name="execute_revenue_query",
    description="Führt eine SQL-Query gegen die Umsatzanalyse-View aus. Vorher MUSS get_revenue_view_schema aufgerufen worden sein, um das Schema zu kennen.",
)
def execute_revenue_query_capa(
    query: Annotated[
        str,
        Field(
            ...,
            description="Eine vollständige SQL-Query auf Basis der View vw_4BasisFürUmsatzanalysen.",
        ),
    ],
) -> Structured:
    """Tool, um eine Umsatzanalyse-Query auszuführen."""
    result: Structured = db.execute_revenue_query(query)
    return result


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
