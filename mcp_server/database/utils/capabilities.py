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
    "Fehler": "Es ist ein Fehler aufgetreten. Bitte überprüfe die Eingaben und versuche es erneut."
}
class Structured(BaseModel):
    data: dict[str, Any]


class DatabaseCapabilities:
    def __init__(self) -> None:
        pass

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

    def find_customer(self, customer_id: int) -> Structured:
        """Get customer entry by id."""
        query = f"SELECT * FROM customers WHERE id = {customer_id}"
        query_result = self._make_query(query)
        return self._to_structured(query_result)

    def show_customers(self) -> Structured:
        """Get a list of customers with their IDs."""
        query = "SELECT id, name FROM customers"
        query_result = self._make_query(query)
        result = self._to_structured(query_result)
        return result

    ########################## PRODUCTS ##########################

    def show_products(self) -> Structured:
        """Get a list of products with prices and inventory."""
        query = """
            SELECT p.id, p.name, p.sale_price, p.description,
                i.stock, i.min_stock, invsl.name AS storage
            FROM products p
            JOIN inventory i ON i.product_id = p.id
            JOIN inventory_storagelocations invsl ON invsl.id = i.storage_location_id"""
        query_result = self._make_query(query)
        result = self._to_structured(query_result)
        return result

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
        query = f"""
            INSERT INTO products (name, description, category_id, purchase_price, sale_price, supplier_id)
            VALUES (N'{name}', N'{description}', {category_id},
                    {purchase_price}, {sale_price}, {supplier_id});
            SELECT * FROM products WHERE id = SCOPE_IDENTITY();"""
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

    def show_auftrag_by_invoice_id(self, invoice_id: int) -> Structured:
        """Get an Auftrag (order + invoice record) by invoice id."""
        query = f"""
        SELECT a.Bestellstatus, a.Auftragseingang, a.Kunden_ID, a.Produkt_ID, a.Bestellmenge, a.Rechnungs_ID, a.Umsatz, a.rabattierter_Umsatz, a.Mahngebühr, a.Zahlungsfrist, a.Zahltag, a.Rechnungsstatus, a.Status_Auftrag, p.name as produkt_name
        FROM vw_0Aufträge a
        JOIN products p ON a.Produkt_id = p.id
        WHERE Rechnungs_ID = {invoice_id} 
        """
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
