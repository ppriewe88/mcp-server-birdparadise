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
    "MCP_SERVER_ENDPOINT_WRITE", "http://127.0.0.1:8010/mcp"
)

mcp = FastMCP(
    name="bird_paradise_writing_capas",
    host="0.0.0.0",
    port=8010,
    stateless_http=True,
)

db = DatabaseCapabilities()

########################## CUSTOMERS ##########################

@mcp.tool(
    name="create_customer",
    title="Kunde anlegen",
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

########################## PRODUCTS ##########################

@mcp.tool(
    name="create_product",
    title="Produkt anlegen",
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
    title="Lagerbestand auffüllen",
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
    name="restock_all_low_stock",
    title="Alle kritischen Lagerbestände auffüllen",
    description="Füllt automatisch alle Lagerbestände auf, bei denen der aktuelle Bestand unter dem Mindestbestand liegt. Der Bestand wird jeweils auf Mindestbestand + 10 gesetzt. Benötigt keine Eingabe.",
)
def restock_all_low_stock_capa() -> Structured:
    """Tool, um alle Lagerbestände mit unterschrittenem Mindestbestand automatisch aufzufüllen."""
    result: Structured = db.restock_all_low_stock()
    return result


########################## ORDERS ##########################


@mcp.tool(
    name="pay_all_unpaid_invoices",
    title="Alle unbezahlten Rechnungen bezahlen",
    description="Bezahlt automatisch alle offenen (unpaid/overdue) Rechnungen. Benötigt keine Eingabe.",
)
def pay_all_unpaid_invoices_capa() -> Structured:
    """Tool, um alle unbezahlten Rechnungen auf einmal zu bezahlen."""
    result: Structured = db.pay_all_unpaid_invoices()
    return result


@mcp.tool(
    name="retry_all_rejected_orders",
    title="Alle abgelehnten Bestellungen nachbearbeiten",
    description="Prüft automatisch alle abgelehnten Bestellungen erneut (z.B. nachdem Lager aufgefüllt wurde). Benötigt keine Eingabe.",
)
def retry_all_rejected_orders_capa() -> Structured:
    """Tool, um alle abgelehnten Bestellungen erneut zu prüfen."""
    result: Structured = db.retry_all_rejected_orders()
    return result


@mcp.tool(
    name="create_order",
    title="Auftrag anlegen",
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
    name="retry_rejected_order",
    title="Abgelehnte Bestellung prüfen",
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
) -> Structured:
    """Tool, um eine abgelehnte Bestellung erneut zu prüfen."""
    result: Structured = db.retry_rejected_order(order_id)
    return result


########################## INVOICES ##########################

@mcp.tool(
    name="pay_invoice",
    title="Rechnung bezahlen",
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
    name="update_invoice_due_limit",
    title="Zahlungsfrist Rechnung ändern",
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


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
