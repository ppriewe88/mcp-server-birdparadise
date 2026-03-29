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
    name="create_customer",
    description="Neuen Kunden anlegen. Pflichtfelder: Name und E-Mail. Optional: Telefon, Stadt, Adresse, Land, Rabatt.",
)
def create_customer_capa(
    name: Annotated[str, Field(..., description="Name des Kunden")],
    email: Annotated[str, Field(..., description="E-Mail-Adresse des Kunden")],
    phone: Annotated[
        int | None,
        Field(None, description="Telefonnummer des Kunden (optional)"),
    ] = None,
    city: Annotated[
        str | None, Field(None, description="Stadt des Kunden (optional)")
    ] = None,
    address: Annotated[
        str | None, Field(None, description="Adresse des Kunden (optional)")
    ] = None,
    country: Annotated[
        str | None, Field(None, description="Land des Kunden (optional)")
    ] = None,
    discount: Annotated[
        int, Field(0, description="Rabatt in Prozent (0-100, Standard: 0)")
    ] = 0,
) -> Structured:
    """Tool, um einen neuen Kunden anzulegen."""
    result: Structured = db.create_customer(
        name, email, phone, city, address, country, discount
    )
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
    name="create_product",
    description="Neues Produkt anlegen. Hinweis: Verkaufspreis muss mindestens 19% über dem Einkaufspreis liegen.",
)
def create_product_capa(
    name: Annotated[str, Field(..., description="Produktname")],
    description: Annotated[str, Field(..., description="Produktbeschreibung")],
    category_id: Annotated[int, Field(..., description="Kategorie-ID")],
    purchase_price: Annotated[
        float, Field(..., description="Einkaufspreis (Dezimalzahl)")
    ],
    sale_price: Annotated[
        float,
        Field(
            ...,
            description="Verkaufspreis (Dezimalzahl, muss >= Einkaufspreis * 1.19 sein)",
        ),
    ],
    supplier_id: Annotated[int, Field(..., description="Lieferanten-ID")],
) -> Structured:
    """Tool, um ein neues Produkt anzulegen."""
    result: Structured = db.create_product(
        name, description, category_id, purchase_price, sale_price, supplier_id
    )
    return result


@mcp.tool(
    name="restock_inventory",
    description="Lagerbestand für ein Produkt auffüllen. Erhöht den Bestand um die angegebene Menge.",
)
def restock_inventory_capa(
    product_id: Annotated[int, Field(..., description="Produkt-ID")],
    quantity: Annotated[
        int, Field(..., description="Anzahl, um die der Bestand erhöht werden soll")
    ],
    storage_location_id: Annotated[
        int, Field(..., description="Lagerort-ID (z.B. 1, 2, ...)")
    ],
) -> Structured:
    """Tool, um den Lagerbestand eines Produkts aufzufüllen."""
    result: Structured = db.restock_inventory(product_id, quantity, storage_location_id)
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
    name="create_order",
    description="""Neuen Auftrag (Bestellung) anlegen. Die Datenbank prüft automatisch den Lagerbestand:
- Falls genug Bestand: Auftrag wird auf 'beauftragt' gesetzt, Lager wird reduziert, Rechnung wird automatisch erstellt.
- Falls nicht genug Bestand: Auftrag wird auf 'abgelehnt' gesetzt, keine Rechnung.""",
)
def create_order_capa(
    customer_id: Annotated[
        int,
        Field(
            ...,
            description="Kunden-ID. Muss vom Nutzer angegeben werden. Wenn nicht gegeben, erfragen.",
        ),
    ],
    product_id: Annotated[
        int,
        Field(
            ...,
            description="Produkt-ID. Muss vom Nutzer angegeben werden. Wenn nicht gegeben, erfragen.",
        ),
    ],
    quantity: Annotated[
        int,
        Field(
            ...,
            description="Bestellmenge. Muss vom Nutzer angegeben werden. Wenn nicht gegeben, erfragen.",
        ),
    ],
) -> Structured:
    """Tool, um einen neuen Auftrag anzulegen."""
    result: Structured = db.create_order(customer_id, product_id, quantity)
    return result


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


@mcp.tool(
    name="retry_rejected_order",
    description="Abgelehnte Bestellung erneut prüfen (z.B. nachdem Lager aufgefüllt wurde). Prüft Bestand und setzt bei Erfolg auf 'beauftragt' mit Rechnungsanlage.",
)
def retry_rejected_order_capa(
    order_id: Annotated[
        int,
        Field(
            ...,
            description="Bestell-ID der abgelehnten Bestellung. Muss vom Nutzer angegeben werden. Wenn nicht gegeben, erfragen.",
        ),
    ],
) -> Structured | str:
    """Tool, um eine abgelehnte Bestellung erneut zu prüfen."""
    result: Structured = db.retry_rejected_order(order_id)
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
) -> Structured:
    """Tool, um eine Rechnung zu bezahlen."""
    result: Structured = db.pay_invoice(invoice_id)
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


@mcp.tool(
    name="update_invoice_due_limit",
    description="Zahlungsfrist (due_limit in Tagen) einer Rechnung ändern. Das Fälligkeitsdatum wird automatisch neu berechnet.",
)
def update_invoice_due_limit_capa(
    invoice_id: Annotated[
        int,
        Field(
            ...,
            description="Rechnungs-ID. Muss vom Nutzer angegeben werden. Wenn nicht gegeben, erfragen.",
        ),
    ],
    new_due_limit: Annotated[
        int,
        Field(
            ...,
            description="Neue Zahlungsfrist in Tagen (z.B. 14, 30, 60).",
        ),
    ],
) -> Structured:
    """Tool, um die Zahlungsfrist einer Rechnung zu ändern."""
    result: Structured = db.update_invoice_due_limit(invoice_id, new_due_limit)
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
