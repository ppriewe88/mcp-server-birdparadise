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
    name="search_customer",
    title="Kunde suchen",
    description="""Rufe dieses Tool auf, um nach einem Kunden zu suchen, insbesondere, wenn du seine ID und/oder seinen Namen brauchst.
    
    Du kannst über die ID suchen, wenn du sie schon kennst.
    Falls du sie nicht kennst, kannst du auch ein Suchwort verwenden (eins oder mehrere).
    Es wird dann nach Übereinstimmungen in Kundenname und Städten gesucht. 
    Je mehr Suchwörter du angibst, desto genauer wird die Suche (aber es müssen nicht alle Wörter übereinstimmen).""",
)
def search_customer_capa(
    customer_id: Annotated[
        int | None, Field(None, description="Optional: Kunden-ID, z.B. 5")
    ] = None,
    search_text: Annotated[
        str | None,
        Field(
            None,
            description="""Optional: Suchtext für Kundennamen oder Stadt.
            Suchtext besteht idealerweise aus EINEM Wort (kann aber auch mehrere beinhalten).
            Es wird nach Übereinstimmungen in Kundennamen und Städten gesucht.""",
        ),
    ] = None,
) -> Structured:
    """Tool, um Informationen über einen Kunden anhand seiner ID oder einem Suchtext zu erhalten."""
    if not customer_id and not search_text:
        raise ValueError(
            "Invalid input. Please provide either customer_id or search_text."
        )
    result: Structured = db.search_customer(customer_id, search_text)
    return result


########################## PRODUCTS ##########################


@mcp.tool(
    name="search_product",
    title="Produkt suchen",
    description="""Rufe dieses Tool auf, um nach einem Produkt (oder ähnlichen Produkten) zu suchen.
    
    Du kannst über die ID suchen, wenn du sie schon kennst.
    Du kannst auch ein Suchwort verwenden (eins oder mehrere; idealerweise ein Wort, je genauer desto besser). Es wird dann nach Übereinstimmungen in Produktnamen und Beschreibungen gesucht.
    Du kannst auch über die Kategorie (Warengruppe) suchen, indem du die Kategorie-ID angibst (1 = Nahrungsmittel, 2 = Käfig und Zubehör, 3 = Spielzeug, 4 = Medizinische Produkte).""",
)
def search_product_capa(
    product_id: Annotated[
        int | None, Field(None, description="Optional: Produkt-ID, z.B. 5")
    ] = None,
    search_text: Annotated[
        str | None,
        Field(
            None,
            description="""Optional: Suchtext für Produktname oder Beschreibung.
            Suchtext besteht idealerweise aus EINEM Wort (kann aber auch mehrere beinhalten).
            Es wird nach Übereinstimmungen in Produktnamen und -beschreibung gesucht.""",
        ),
    ] = None,
    category_id: Annotated[
        int | None,
        Field(
            None,
            description="""Optional: Kategorie-ID, zu wählen aus 1, 2, 3 oder 4.
            1 = Nahrungsmittel, 2 = Käfig und Zubehör, 3 = Spielzeug, 4 = Medizinische Produkte.""",
        ),
    ] = None,
) -> Structured:
    """Tool, um Informationen über ein Produkt zu erhalten."""
    if not product_id and not search_text and not category_id:
        raise ValueError(
            "Invalid input. Please provide either product_id, search_text, or category_id."
        )
    result: Structured = db.search_product(product_id, search_text, category_id)
    return result


@mcp.tool(
    name="show_low_stock_products",
    title="Mangelbestände anzeigen",
    description="Zeige Produkte an, deren Lagerbestand unter dem Mindestbestand liegt.",
)
def show_low_stock_products_capa() -> Structured:
    """Tool, um Produkte mit unterschrittenem Mindestbestand anzuzeigen."""
    result: Structured = db.show_low_stock_products()
    return result


@mcp.tool(
    name="show_open_orders_for_product",
    title="Offene Bestellungen pro Produkt",
    description="Zeige offene (beauftragte) Bestellungen für ein bestimmtes Produkt.",
)
def show_open_orders_for_product_capa(
    product_id: Annotated[int, Field(..., description="Produkt-ID")],
) -> Structured:
    """Tool, um offene Bestellungen für ein Produkt anzuzeigen."""
    result: Structured = db.show_open_orders_for_product(product_id)
    return result


########################## AUFTRÄGE ##########################

@mcp.tool(
    name="search_auftrag",
    title="Auftrag suchen",
    description="""Suche nach Aufträgen (Bestellung + Rechnung) anhand von Kunden-ID, Bestell-ID oder Rechnungs-ID.
    Mindestens ein Suchparameter muss angegeben werden. Mehrere können kombiniert werden.""",
)
def search_auftrag_capa(
    customer_id: Annotated[
        int | None, Field(None, description="Optional: Kunden-ID")
    ] = None,
    order_id: Annotated[
        int | None, Field(None, description="Optional: Bestell-ID")
    ] = None,
    invoice_id: Annotated[
        int | None, Field(None, description="Optional: Rechnungs-ID")
    ] = None,
) -> Structured:
    """Tool, um Aufträge nach Kunden-ID, Bestell-ID oder Rechnungs-ID zu suchen."""
    result: Structured = db.search_auftrag(customer_id, order_id, invoice_id)
    return result


@mcp.tool(
    name="show_auftraege_for_customer",
    title="Aufträge Kunde anzeigen",
    description="Zeige alle Aufträge (Bestellung + Rechnung) eines Kunden an, inklusive Produktname und Status.",
)
def show_auftraege_for_customer_capa(
    customer_id: Annotated[
        int,
        Field(
            ...,
            description="Kunden-ID. Muss vom Nutzer angegeben werden. Wenn nicht gegeben, erfragen.",
        ),
    ],
) -> Structured:
    """Tool, um alle Aufträge eines Kunden anzuzeigen."""
    result: Structured = db.show_auftraege_for_customer(customer_id)
    return result


@mcp.tool(
    name="show_unpaid_auftraege",
    title="Unbezahlte Aufträge anzeigen",
    description="Zeige unbezahlte Rechnungen an. Nutze dieses Tool, um eine Übersicht über alle Aufträge (Bestellung + Rechnung) mit unbezahlten Rechnungen zu erhalten.",
)
def show_unpaid_auftraege_capa() -> Structured:
    """Tool, um unbezahlte Aufträge anzuzeigen."""
    result: Structured = db.show_unpaid_auftraege()
    return result


@mcp.tool(
    name="show_unpaid_auftraege_for_customer",
    title="Unbezahlte Aufträge Kunde anzeigen",
    description="Dieses Tool liefert für die ID eines Kunden dessen Aufträge (Bestellung + Rechnung) mit unbezahlten Rechnungen.",
)
def show_unpaid_auftraege_for_customer_capa(
    customer_id: Annotated[
        int,
        Field(
            ...,
            description="""Kunden-ID.
            Muss vom Nutzer bei der Anfrage angegeben werden (ganzzahlig). Wenn nicht gegeben, erfragen.""",
        ),
    ],
) -> Structured:
    """Tool, um unbezahlte Aufträge eines bestimmten Kunden anzuzeigen."""
    result: Structured = db.show_unpaid_auftraege_for_customer(customer_id)
    return result


@mcp.tool(
    name="show_rejected_orders",
    title="Abgelehnte Bestellungen anzeigen",
    description="Zeige alle abgelehnten Bestellungen an. Bestellungen werden abgelehnt, wenn der Lagerbestand nicht ausreicht. Diese können nach einer Bestandsauffüllung erneut geprüft werden.",
)
def show_rejected_orders_capa() -> Structured:
    """Tool, um abgelehnte Bestellungen anzuzeigen."""
    result: Structured = db.show_rejected_orders()
    return result


########################## REVENUE ANALYSIS ##########################


@mcp.tool(
    name="get_revenue_view_schema",
    title="DB-Schema Umsatzanalyse-View abrufen",
    description="Gibt das Schema und die Query-Anweisungen für die Umsatzanalyse-View zurück. Rufe dieses Tool ZUERST auf, bevor du eine Umsatz-Query formulierst.",
)
def get_revenue_view_schema_capa() -> Structured:
    """Tool, um das Schema der Umsatzanalyse-View zu erhalten."""
    result: Structured = db.get_revenue_view_schema()
    return result


@mcp.tool(
    name="execute_revenue_query",
    title="Umsatzanalyse-Query ausführen",
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
