import re
from typing import Any

from pydantic import BaseModel

from mcp_server.database.utils.connect import establish_database_connection
from mcp_server.database.utils.prompts import revenue_view_schema

Row = dict[str, Any]
Status = dict[str, str]
QueryResult = list[Row] | Status | None

EMPTY_RETURN = {
    "Ungültige Anfrage oder keine Daten gefunden": "Es wurden keine Daten gefunden, die den Kriterien entsprechen."
}
ERROR_RETURN = {
    "Fehler": "Es gab ein Problem bei der Verbindung mit dem Server. Die Anfrage konnte nicht durchgefuehrt werden. Versuchen Sie es nochmal, oder wenden Sie sich an den Administrator/Betreiber des Servers."
}
class Structured(BaseModel):
    data: dict[str, Any]


class DatabaseCapabilities:
    def __init__(self) -> None:
        pass

    def normalize_empty_inputs(self, values: list[Any]) -> list[Any]:
        """Map frontend sentinel string 'EMPTY' to None for optional inputs."""
        normalized: list[Any] = []
        for value in values:
            if isinstance(value, str) and value.strip().upper() == "EMPTY":
                normalized.append(None)
            else:
                normalized.append(value)
        return normalized

    def _make_query(
        self,
        input_query: str,
        procedure: bool = False,
        params: Any = None,
    ) -> QueryResult:
        """Send query to the database and return results."""
        try:
            connection = establish_database_connection()
            cursor = connection.cursor(as_dict=True)

            if procedure and params:
                cursor.execute(input_query, params)
            else:
                cursor.execute(input_query)

            if cursor.description:
                rows: list[Row] = cursor.fetchall()
                connection.commit()
                return rows

            try:
                rows = cursor.fetchall()
                if rows:
                    connection.commit()
                    return rows
            except Exception:
                connection.commit()
                result: Status = {
                    "status": "success (fallback)",
                    "message": "Query executed, no data returned.",
                }
                return result

            connection.commit()
            result = {
                "status": "success",
                "message": "Query executed, no data returned.",
            }
            return result

        except Exception as e:
            print(f"Error during execution of sql-query:\n{e}")
            return None

    def _to_structured(self, query_result: QueryResult) -> Structured:
        """Convert a QueryResult to a Structured response, keyed by row id."""
        data: dict[str, Any]
        if isinstance(query_result, list):
            data = {str(i + 1): row for i, row in enumerate(query_result)}
            if len(list(data.keys())) == 0:
                data = {"1": EMPTY_RETURN}
        elif isinstance(query_result, dict):
            data = query_result
        else:
            data = {"1": ERROR_RETURN}
        return Structured(data=data)

    def _normalize_search_text(self, text: str) -> list[str]:
        """Normalize search text for SQL LIKE queries."""
        text = text.casefold().strip()
        text = re.sub(r"[^\w\s]", " ", text)
        text = text.replace("_", " ")
        text = re.sub(r"\s+", " ", text).strip()
        return [token for token in text.split(" ") if token]

    ########################## CUSTOMERS ##########################

    def create_customer(
        self,
        name: str,
        email: str,
        phone: int | None = None,
        city: str | None = None,
        address: str | None = None,
        country: str | None = None,
        discount: int = 0,
    ) -> Structured:
        """Create a new customer and return the created record."""
        query = f"""
            INSERT INTO customers (name, email, phone, city, address, country, discount)
            VALUES (N'{name}', N'{email}', {phone if phone else "NULL"},
                    {f"N'{city}'" if city else "NULL"},
                    {f"N'{address}'" if address else "NULL"},
                    {f"N'{country}'" if country else "NULL"},
                    {discount});
            SELECT * FROM customers WHERE id = SCOPE_IDENTITY();"""
        query_result = self._make_query(query)
        return self._to_structured(query_result)

    def search_customer(
        self,
        customer_id: int | None = None,
        search_text: str | None = None,
        city: str | None = None,
    ) -> Structured:
        """Search customers by id, free-text name tokens, or city."""
        query = """
            SELECT
                c.id,
                c.name,
                c.email,
                c.city,
                c.address,
                c.country,
                c.discount
            FROM customers c
            WHERE 1 = 1
        """

        params: dict[str, int | str] = {}

        if customer_id is not None:
            query += " AND c.id = %(customer_id)s"
            params["customer_id"] = customer_id

        if city:
            query += " AND c.city LIKE %(city)s"
            params["city"] = f"%{city.strip()}%"

        if search_text:
            search_words = self._normalize_search_text(search_text)

            if search_words:
                or_conditions: list[str] = []

                for idx, word in enumerate(search_words):
                    param_name = f"search_word_{idx}"
                    or_conditions.append(f"c.name LIKE %({param_name})s")
                    params[param_name] = f"%{word}%"

                query += " AND (" + " OR ".join(or_conditions) + ")"

        if not params:
            raise ValueError(
                "At least one search parameter (customer_id, search_text, or city) must be provided."
            )

        query_result = self._make_query(query, procedure=True, params=params)
        return self._to_structured(query_result)

    ########################## PRODUCTS ##########################

    def search_product(
        self,
        product_id: int | None = None,
        search_text: str | None = None,
        category_id: int | None = None,
    ) -> Structured:
        """Search products by id, name/description (contains) or category_id."""
        query = """
            SELECT
                p.id,
                p.name,
                p.description,
                p.sale_price AS Verkaufspreis,
                p.category_id AS Warengruppennummer,
                c.name AS Warengruppe
            FROM products p
            JOIN category c
                ON p.category_id = c.id
            WHERE 1=1
        """
        search_words = search_text.split() if search_text else []

        if category_id and category_id not in [1, 2, 3, 4]:
            raise ValueError(
                "Invalid category_id. Valid values are 1, 2, 3, or 4. 1 = Nahrungsmittel, 2 = Käfig und Zubehör, 3 = Spielzeug, 4 = Medizinische Produkte."
            )

        params: dict[str, int | str] = {}

        if product_id is not None:
            query += " AND p.id = %(product_id)s"
            params["product_id"] = product_id
        if category_id is not None:
            query += " AND p.category_id = %(category_id)s"
            params["category_id"] = category_id
        if search_text:
            search_words = self._normalize_search_text(search_text)

            if search_words:
                or_conditions: list[str] = []

                for idx, word in enumerate(search_words):
                    param_name = f"search_word_{idx}"
                    or_conditions.append(
                        f"""
                        p.name LIKE %({param_name})s
                        OR ISNULL(p.description, '') LIKE %({param_name})s
                        OR c.name LIKE %({param_name})s
                        """
                    )
                    params[param_name] = f"%{word}%"

                query += " AND (" + " OR ".join(or_conditions) + ")"

        if not params:
            raise ValueError(
                "At least one search parameter (product_id, search_text, or category_id) must be provided."
            )

        query_result = self._make_query(query, procedure=True, params=params)
        return self._to_structured(query_result)

    def create_product(
        self,
        name: str,
        description: str,
        category_id: int,
        purchase_price: float,
        sale_price: float,
        supplier_id: int,
    ) -> Structured:
        """Create a new product and return the created record."""
        min_sale_price = round(purchase_price * 1.19, 2)
        if sale_price < min_sale_price:
            raise ValueError(
                f"Verkaufspreis ({sale_price:.2f}) muss mindestens "
                f"{min_sale_price:.2f} betragen (Einkaufspreis {purchase_price:.2f} × 1.19). "
                f"Der Verkaufspreis muss die Mehrwertsteuer von 19% abdecken."
            )
        query = f"""
            INSERT INTO products (name, description, category_id, purchase_price, sale_price, supplier_id)
            VALUES (N'{name}', N'{description}', {category_id},
                    {purchase_price}, {sale_price}, {supplier_id});
            DECLARE @new_product_id INT = SCOPE_IDENTITY();
            INSERT INTO inventory (product_id, storage_location_id, stock, min_stock)
            SELECT @new_product_id, sl.id, 0, ABS(CHECKSUM(NEWID())) % 13 + 3
            FROM inventory_storagelocations sl;
            SELECT * FROM products WHERE id = @new_product_id;"""
        query_result = self._make_query(query)
        return self._to_structured(query_result)

    def restock_inventory(
        self, product_id: int, quantity: int, storage_location_id: int
    ) -> Structured:
        """Add stock to a product's inventory at a given storage location."""
        query = f"""
            UPDATE inventory
            SET stock = stock + {quantity}
            WHERE product_id = {product_id}
              AND storage_location_id = {storage_location_id};
            SELECT i.product_id, p.name AS product_name,
                   i.stock, i.min_stock,
                   invsl.name AS storage
            FROM inventory i
            JOIN products p ON p.id = i.product_id
            JOIN inventory_storagelocations invsl ON invsl.id = i.storage_location_id
            WHERE i.product_id = {product_id}
              AND i.storage_location_id = {storage_location_id};"""
        query_result = self._make_query(query)
        return self._to_structured(query_result)

    def restock_all_low_stock(self) -> Structured:
        """Restock all inventory entries where stock is below min_stock to min_stock + 10."""
        query = """
            UPDATE inventory
            SET stock = min_stock + 10
            WHERE stock < min_stock;"""
        self._make_query(query)
        result = self._to_structured(
            [
                {
                    "status": "success",
                    "message": "Alle Produkte wurden wieder auf 10 Stück über Mindestbestand aufgefüllt.",
                }
            ]
        )
        return result

    def show_low_stock_products(self) -> Structured:
        """Show products where current stock is below min_stock."""
        query = """
            SELECT v.*, p.name AS produkt_name
            FROM vw_2KritischeLagerbestände v
            JOIN products p ON v.Produkt_ID = p.id"""
        query_result = self._make_query(query)
        return self._to_structured(query_result)

    def show_open_orders_for_product(self, product_id: int) -> Structured:
        """Show open (commissioned) orders for a specific product."""
        query = f"""
            SELECT o.id AS order_id, o.customer_id,
                   c.name AS customer_name,
                   o.quantity,
                   FORMAT(o.created_at, 'yyyy-MM-dd') AS created_at,
                   os.name AS status
            FROM orders o
            JOIN customers c ON c.id = o.customer_id
            JOIN orders_status os ON os.id = o.status_id
            WHERE o.product_id = {product_id}
              AND os.name LIKE '%beauftragt%'
            ORDER BY o.created_at DESC"""
        query_result = self._make_query(query)
        return self._to_structured(query_result)

    ########################## AUFTRÄGE ##########################

    def create_order(
        self, customer_id: int, product_id: int, quantity: int
    ) -> Structured:
        """Create an order. The DB trigger handles stock check, inventory reduction, and invoice creation."""
        query = f"""
            INSERT INTO orders (customer_id, product_id, quantity, status_id)
            VALUES ({customer_id}, {product_id}, {quantity}, 1);
            DECLARE @new_order_id INT = SCOPE_IDENTITY();
            SELECT a.*, p.name AS produkt_name
            FROM vw_0Aufträge a
            JOIN products p ON a.Produkt_ID = p.id
            WHERE a.Bestell_ID = @new_order_id;"""
        query_result = self._make_query(query)
        return self._to_structured(query_result)

    def show_auftraege_for_customer(self, customer_id: int) -> Structured:
        """Show all Aufträge (order + invoice records) for a customer."""
        query = f"""
            SELECT a.*, p.name AS produkt_name
            FROM vw_0Aufträge a
            JOIN products p ON a.Produkt_ID = p.id
            WHERE a.Kunden_ID = {customer_id}
            ORDER BY a.Auftragseingang DESC"""
        query_result = self._make_query(query)
        return self._to_structured(query_result)

    def search_auftrag(
        self,
        customer_id: int | None = None,
        order_id: int | None = None,
        invoice_id: int | None = None,
    ) -> Structured:
        """Search Aufträge by customer_id, order_id, or invoice_id."""
        query = """
            SELECT a.*, p.name AS produkt_name
            FROM vw_0Aufträge a
            JOIN products p ON a.Produkt_ID = p.id
            WHERE 1=1
        """
        params: dict[str, int] = {}

        if customer_id is not None:
            query += " AND a.Kunden_ID = %(customer_id)s"
            params["customer_id"] = customer_id
        if order_id is not None:
            query += " AND a.Bestell_ID = %(order_id)s"
            params["order_id"] = order_id
        if invoice_id is not None:
            query += " AND a.Rechnungs_ID = %(invoice_id)s"
            params["invoice_id"] = invoice_id

        if not params:
            raise ValueError(
                "Mindestens ein Suchparameter (customer_id, order_id oder invoice_id) muss angegeben werden."
            )

        query += " ORDER BY a.Auftragseingang DESC"
        query_result = self._make_query(query, procedure=True, params=params)
        return self._to_structured(query_result)

    def retry_rejected_order(self, order_id: int) -> Structured:
        """Re-check a rejected order for commission (after restock)."""
        proc_query = "EXEC spCheckExistingOrderForCommission @orderID_checkforcomm = %s"
        self._make_query(proc_query, procedure=True, params=order_id)
        select_query = f"""
            SELECT a.*, p.name AS produkt_name
            FROM vw_0Aufträge a
            JOIN products p ON a.Produkt_ID = p.id
            WHERE a.Bestell_ID = {order_id}"""
        query_result = self._make_query(select_query)
        return self._to_structured(query_result)

    def retry_all_rejected_orders(self) -> Structured:
        """Re-check all rejected orders for commission via server-side cursor."""
        query = """
            DECLARE @oid INT;
            DECLARE @before INT, @after INT;

            SELECT @before = COUNT(*)
            FROM orders o
            JOIN orders_status os ON os.id = o.status_id
            WHERE os.name LIKE '%abgelehnt%';

            DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
                SELECT o.id FROM orders o
                JOIN orders_status os ON os.id = o.status_id
                WHERE os.name LIKE '%abgelehnt%';
            OPEN cur;
            FETCH NEXT FROM cur INTO @oid;
            WHILE @@FETCH_STATUS = 0
            BEGIN
                EXEC spCheckExistingOrderForCommission @orderID_checkforcomm = @oid;
                FETCH NEXT FROM cur INTO @oid;
            END
            CLOSE cur;
            DEALLOCATE cur;

            SELECT @after = COUNT(*)
            FROM orders o
            JOIN orders_status os ON os.id = o.status_id
            WHERE os.name LIKE '%abgelehnt%';

            SELECT @before AS vorher_abgelehnt,
                   @after AS nachher_abgelehnt,
                   @before - @after AS beauftragt;"""
        query_result = self._make_query(query)
        if isinstance(query_result, list) and query_result:
            row = query_result[0]
            vorher = row.get("vorher_abgelehnt", 0)
            nachher = row.get("nachher_abgelehnt", 0)
            beauftragt = row.get("beauftragt", 0)
        else:
            vorher = nachher = beauftragt = 0
        if vorher == 0:
            msg = "Es gibt keine abgelehnten Bestellungen zum Nachbearbeiten."
        elif beauftragt == 0:
            msg = (f"{vorher} abgelehnte Bestellung(en) wurden geprüft. "
                   f"Keine konnte beauftragt werden (alle noch abgelehnt).")
        else:
            msg = (f"{vorher} abgelehnte Bestellung(en) wurden geprüft. "
                   f"{beauftragt} davon wurden auf 'beauftragt' gesetzt, "
                   f"{nachher} sind weiterhin abgelehnt.")
        return self._to_structured([{"status": "success", "message": msg}])

    def show_rejected_orders(self) -> Structured:
        """Show all rejected orders (status 'abgelehnt')."""
        query = """
            SELECT o.id AS order_id, o.customer_id,
                   c.name AS customer_name,
                   o.product_id, p.name AS product_name,
                   o.quantity,
                   FORMAT(o.created_at, 'yyyy-MM-dd') AS created_at,
                   os.name AS status
            FROM orders o
            JOIN customers c ON c.id = o.customer_id
            JOIN products p ON p.id = o.product_id
            JOIN orders_status os ON os.id = o.status_id
            WHERE os.name LIKE '%abgelehnt%'
            ORDER BY o.created_at DESC"""
        query_result = self._make_query(query)
        return self._to_structured(query_result)

    ########################## AUFTRÄGE (UNBEZAHLT / DETAILS) ##########################

    def show_unpaid_auftraege(self) -> Structured:
        """Get a list of unpaid and overdue Aufträge."""
        query = """
            SELECT a.*, p.name AS produkt_name
            FROM vw_0Aufträge a
            JOIN products p ON a.Produkt_ID = p.id
            WHERE a.Rechnungsstatus LIKE '%unpaid%'
               OR a.Rechnungsstatus LIKE '%overdue%'"""
        query_result = self._make_query(query)
        result = self._to_structured(query_result)
        return result

    def pay_all_unpaid_invoices(self) -> Structured:
        """Pay all unpaid/overdue invoices via server-side cursor."""
        query = """
            DECLARE @iid INT, @cnt INT = 0;
            DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
                SELECT i.id FROM invoices i
                JOIN invoices_status invs ON invs.id = i.status_id
                WHERE invs.name LIKE '%unpaid%' OR invs.name LIKE '%overdue%';
            OPEN cur;
            FETCH NEXT FROM cur INTO @iid;
            WHILE @@FETCH_STATUS = 0
            BEGIN
                EXEC spChangeInvoiceStatusAndCheckDiscount @invoiceID = @iid, @newStatusID = 2;
                SET @cnt = @cnt + 1;
                FETCH NEXT FROM cur INTO @iid;
            END
            CLOSE cur;
            DEALLOCATE cur;
            SELECT @cnt AS anzahl;"""
        query_result = self._make_query(query)
        if isinstance(query_result, list) and query_result:
            cnt = query_result[0].get("anzahl", 0)
        else:
            cnt = 0
        if cnt == 0:
            msg = "Es gibt keine unbezahlten Rechnungen."
        else:
            msg = f"{cnt} Rechnung(en) wurden bezahlt."
        return self._to_structured([{"status": "success", "message": msg}])

    def show_unpaid_auftraege_for_customer(self, customer_id: int) -> Structured:
        """Get unpaid/overdue Aufträge for a specific customer."""
        query = f"""
            SELECT a.*, p.name AS produkt_name
            FROM vw_0Aufträge a
            JOIN products p ON a.Produkt_ID = p.id
            WHERE a.Kunden_ID = {customer_id}
              AND (a.Rechnungsstatus LIKE '%unpaid%' OR a.Rechnungsstatus LIKE '%overdue%')"""
        query_result = self._make_query(query)
        result = self._to_structured(query_result)
        return result

    def pay_invoice(self, invoice_id: int) -> Structured:
        """Pay an invoice and return the updated order/invoice pair."""
        proc_query = "EXEC spChangeInvoiceStatusAndCheckDiscount @invoiceID = %s, @newStatusID = 2"
        proc_result = self._make_query(proc_query, procedure=True, params=invoice_id)
        if proc_result is None:
            return self._to_structured(
                {"error": f"Rechnung {invoice_id} konnte nicht bezahlt werden."}
            )
        select_query = f"""
            SELECT a.*, p.name AS produkt_name
            FROM vw_0Aufträge a
            JOIN products p ON a.Produkt_ID = p.id
            WHERE a.Rechnungs_ID = {invoice_id}"""
        query_result = self._make_query(select_query)
        return self._to_structured(query_result)

    ########################## REVENUE ANALYSIS ##########################

    def get_revenue_view_schema(self) -> Structured:
        """Return the schema and query instructions for vw_4BasisFürUmsatzanalysen."""
        return self._to_structured([{"info": revenue_view_schema}])

    def execute_revenue_query(self, query: str) -> Structured:
        """Execute a read-only SQL query against the revenue view."""
        if not query.strip().upper().startswith("SELECT"):
            return self._to_structured(
                {"error": "Nur SELECT-Queries erlaubt. INSERT, UPDATE, DROP etc. sind nicht gestattet."}
            )
        query_result = self._make_query(query)
        return self._to_structured(query_result)

    ########################## INVOICE MANAGEMENT ##########################

    def update_invoice_due_limit(
        self, invoice_id: int, new_due_limit: int
    ) -> Structured:
        """Update the due_limit (in days) for an invoice."""
        query = f"""
            UPDATE invoices SET due_limit = {new_due_limit} WHERE id = {invoice_id};
            SELECT i.id AS invoice_id, i.due_limit,
                   FORMAT(i.due_date, 'yyyy-MM-dd') AS due_date,
                   invs.name AS status
            FROM invoices i
            JOIN invoices_status invs ON invs.id = i.status_id
            WHERE i.id = {invoice_id};"""
        query_result = self._make_query(query)
        return self._to_structured(query_result)

